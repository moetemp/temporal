package stream

import (
	"fmt"
	"slices"
	"strings"

	"go.temporal.io/api/streamservice/v1"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/protobuf/proto"
)

// Withdrawal is what a batch or a close of a Producer withdraws, as the crate's Withdrawal.
type Withdrawal struct {
	PreviousCompleted *streamservice.WorkflowTaskEpoch
	FailedTasks       []int64
	WithdrawRun       string
	// A Workflow batch before a Local Activity, whose task facts come later. It withdraws no
	// task and keeps no facts.
	Defer bool
}

// admit runs the Producer checks of a write in the order of the crate's append script, and
// answers whether the write withdraws earlier Records. It changes no state.
func (s *Stream) admit(producer *streamservice.Producer, w Withdrawal) (bool, error) {
	if s.runFenced(producer.GetRunId()) {
		return false, NewError(ReasonFenced, "A Workflow retry withdrew the run of the Producer")
	}
	known := s.Producers[producerKey(producer)]
	if known != nil && CompareEpochs(producer.GetEpoch(), known.GetEpoch()) < 0 {
		return false, NewError(ReasonFenced, "The epoch is lower than the newest one of the Producer")
	}
	run := s.Runs[producer.GetRunId()]
	task := taskOf(producer.GetEpoch())
	switch producer.GetKind() {
	case streamservice.PRODUCER_KIND_LOCAL_ACTIVITY:
		if run != nil && (slices.ContainsFunc(run.GetWithdrawn(), func(t *streamservice.WorkflowTaskEpoch) bool {
			return compareTasks(t, task) == 0
		}) || (run.GetLastTask() != nil && len(withdrawTasks(
			[]*streamservice.WorkflowTaskEpoch{task}, run.GetLastTask(), run.GetLastCompleted(), run.GetLastFailed(),
		).withdrawn) > 0)) {
			return false, NewError(ReasonFenced, "A Superseded Record withdrew the Workflow Task of the call")
		}
		return s.withdrawsRun(w.WithdrawRun), nil
	case streamservice.PRODUCER_KIND_WORKFLOW:
		flagged := !w.Defer && len(withdrawTasks(run.GetIndex(), task, w.PreviousCompleted, w.FailedTasks).withdrawn) > 0
		return flagged || s.withdrawsRun(w.WithdrawRun), nil
	default:
		flagged := known != nil && known.GetHolds() != nil &&
			CompareEpochs(known.GetHolds(), producer.GetEpoch()) < 0
		return flagged || s.withdrawsRun(w.WithdrawRun), nil
	}
}

// settle keeps the facts of a write that went in: the epoch, the task index and the run.
func (s *Stream) settle(producer *streamservice.Producer, w Withdrawal, wroteRecords bool) {
	if w.WithdrawRun != "" && !s.runFenced(w.WithdrawRun) {
		s.FencedRuns = append(s.FencedRuns, w.WithdrawRun)
	}
	kind := producer.GetKind()
	if kind != streamservice.PRODUCER_KIND_WORKFLOW && kind != streamservice.PRODUCER_KIND_LOCAL_ACTIVITY {
		return
	}
	if s.Runs == nil {
		s.Runs = make(map[string]*streampb.RunTasks)
	}
	run := s.Runs[producer.GetRunId()]
	if run == nil {
		run = &streampb.RunTasks{}
		s.Runs[producer.GetRunId()] = run
	}
	task := taskOf(producer.GetEpoch())
	if kind == streamservice.PRODUCER_KIND_WORKFLOW && !w.Defer {
		split := withdrawTasks(run.GetIndex(), task, w.PreviousCompleted, w.FailedTasks)
		run.Index = split.kept
		run.Withdrawn = append(run.Withdrawn, split.withdrawn...)
		run.LastTask, run.LastCompleted, run.LastFailed = task, w.PreviousCompleted, w.FailedTasks
	}
	if wroteRecords {
		run.Wrote = true
		if !slices.ContainsFunc(run.Index, func(t *streamservice.WorkflowTaskEpoch) bool { return compareTasks(t, task) == 0 }) {
			run.Index = append(run.Index, task)
		}
	}
}

func (s *Stream) runFenced(run string) bool {
	return run != "" && slices.Contains(s.FencedRuns, run)
}

// withdrawsRun says whether a retry run's withdrawal of run gets a Superseded Record. Only a run
// that wrote has Records to withdraw.
func (s *Stream) withdrawsRun(run string) bool {
	return run != "" && !s.runFenced(run) && s.Runs[run].GetWrote()
}

func supersededRecord(producer *streamservice.Producer, w Withdrawal) *streamservice.StreamRecord {
	epoch := producer.GetEpoch()
	if w.Defer {
		// A deferred batch withdraws only the retry run's previous run, so no epoch goes with it.
		epoch = nil
	}
	return &streamservice.StreamRecord{
		Kind: streamservice.STREAM_RECORD_KIND_SUPERSEDED,
		Supersession: &streamservice.Supersession{
			ProducerKind:          producer.GetKind(),
			ProducerId:            producer.GetId(),
			Epoch:                 epoch,
			PreviousCompletedTask: w.PreviousCompleted,
			FailedTasks:           w.FailedTasks,
			WithdrawRun:           w.WithdrawRun,
			RunId:                 producer.GetRunId(),
		},
	}
}

// orphans applies to a Local Activity write with its run's last completed task (S110). It splits
// the run's open Local Activity tasks into those that never completed and those still open.
func (s *Stream) orphans(req *streamservice.AppendRecordsRequest) (bool, taskSplit) {
	producer := req.GetProducer()
	if producer.GetKind() != streamservice.PRODUCER_KIND_LOCAL_ACTIVITY ||
		req.GetPreviousCompletedTask() == nil || producer.GetRunId() == "" {
		return false, taskSplit{}
	}
	return true, withdrawTasks(s.Runs[producer.GetRunId()].GetOpenLocal(), taskOf(producer.GetEpoch()),
		completedTask(req), req.GetFailedTasks())
}

// settleOrphans keeps the open Local Activity tasks and fences the withdrawn ones.
func (s *Stream) settleOrphans(producer *streamservice.Producer, split taskSplit, wroteRecords bool) {
	run := s.Runs[producer.GetRunId()]
	run.OpenLocal = split.kept
	task := taskOf(producer.GetEpoch())
	if wroteRecords && !slices.ContainsFunc(run.OpenLocal, func(t *streamservice.WorkflowTaskEpoch) bool { return compareTasks(t, task) == 0 }) {
		run.OpenLocal = append(run.OpenLocal, task)
	}
	run.Withdrawn = append(run.Withdrawn, split.withdrawn...)
}

// completedTask is the run's last completed task of a write, or nil before the first.
func completedTask(req *streamservice.AppendRecordsRequest) *streamservice.WorkflowTaskEpoch {
	if c := req.GetPreviousCompletedTask(); c.GetStartedEventId() > 0 {
		return c
	}
	return nil
}

// orphanRecord withdraws a run's orphaned Local Activity Records. It names the run's Workflow
// Producer in the write's task, so a reader's Workflow rule withdraws the run's Local Activity
// Records of the tasks after the completed one and before that task, and of the failed ones.
func orphanRecord(req *streamservice.AppendRecordsRequest) *streamservice.StreamRecord {
	producer := req.GetProducer()
	epoch := producer.GetEpoch()
	return &streamservice.StreamRecord{
		Kind: streamservice.STREAM_RECORD_KIND_SUPERSEDED,
		Supersession: &streamservice.Supersession{
			ProducerKind: streamservice.PRODUCER_KIND_WORKFLOW,
			ProducerId:   "wf:" + encodePart(producer.GetRunId()),
			Epoch: &streamservice.ProducerEpoch{
				TaskStartedEventId: epoch.GetTaskStartedEventId(),
				TaskAttempt:        epoch.GetTaskAttempt(),
				TaskStartedTime:    epoch.GetTaskStartedTime(),
			},
			PreviousCompletedTask: completedTask(req),
			FailedTasks:           req.GetFailedTasks(),
			RunId:                 producer.GetRunId(),
		},
	}
}

// encodePart percent-encodes an id part as the crate does, so a Producer id matches its.
func encodePart(text string) string {
	var out strings.Builder
	for _, b := range []byte(text) {
		if ('a' <= b && b <= 'z') || ('A' <= b && b <= 'Z') || ('0' <= b && b <= '9') ||
			b == '_' || b == '.' || b == '-' || b == '~' {
			out.WriteByte(b)
		} else {
			fmt.Fprintf(&out, "%%%02X", b)
		}
	}
	return out.String()
}

type taskSplit struct {
	withdrawn []*streamservice.WorkflowTaskEpoch
	kept      []*streamservice.WorkflowTaskEpoch
}

// withdrawTasks is the crate's Workflow withdrawal rule over a task index. For task t with the
// previous completed task c, it withdraws the tasks in failed or strictly between c and t, and
// those at the started id of c or of t with a lower attempt. It keeps the tasks after c, since
// no task up to c can be withdrawn later.
func withdrawTasks(
	index []*streamservice.WorkflowTaskEpoch,
	t, c *streamservice.WorkflowTaskEpoch,
	failed []int64,
) taskSplit {
	var split taskSplit
	ts, cs := t.GetStartedEventId(), c.GetStartedEventId()
	for _, key := range index {
		started := key.GetStartedEventId()
		if slices.Contains(failed, started) ||
			(cs < started && started < ts) ||
			(started == ts && compareTasks(key, t) < 0) ||
			(c != nil && started == cs && belowCompleted(key, c)) {
			split.withdrawn = append(split.withdrawn, key)
		} else if c == nil || compareTasks(key, c) > 0 {
			split.kept = append(split.kept, key)
		}
	}
	return split
}

// belowCompleted says whether a task at c's started id is a lower attempt than c. A c with no
// attempt compares by start time only.
func belowCompleted(key, c *streamservice.WorkflowTaskEpoch) bool {
	if c.GetAttempt() == 0 {
		return compareTimestamps(key.GetStartedTime(), c.GetStartedTime()) < 0
	}
	return compareTasks(key, c) < 0
}

func taskOf(epoch *streamservice.ProducerEpoch) *streamservice.WorkflowTaskEpoch {
	return &streamservice.WorkflowTaskEpoch{
		StartedEventId: epoch.GetTaskStartedEventId(),
		Attempt:        epoch.GetTaskAttempt(),
		StartedTime:    epoch.GetTaskStartedTime(),
	}
}

func compareTasks(a, b *streamservice.WorkflowTaskEpoch) int {
	if c := compareInts(a.GetStartedEventId(), b.GetStartedEventId()); c != 0 {
		return c
	}
	if c := compareInts(int64(a.GetAttempt()), int64(b.GetAttempt())); c != 0 {
		return c
	}
	return compareTimestamps(a.GetStartedTime(), b.GetStartedTime())
}

// batchSize is the stored size of the entries of one write.
func batchSize(entries []*streamservice.StreamRecord) int64 {
	size := 0
	for _, entry := range entries {
		size += proto.Size(entry)
	}
	return int64(size)
}
