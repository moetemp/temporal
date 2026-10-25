package stream

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/streamservice/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The files in testdata are copies of the crate's shared vectors, at moetemp/sdk-streams
// crates/streams/testdata, so the server's Producer rules and the Stores agree.

type vectorTime [2]int64

func (t *vectorTime) proto() *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return &timestamppb.Timestamp{Seconds: t[0], Nanos: int32(t[1])}
}

type vectorEpoch struct {
	TaskStartedEventID   int64       `json:"task_started_event_id"`
	TaskAttempt          int32       `json:"task_attempt"`
	TaskStartedTime      *vectorTime `json:"task_started_time"`
	AttemptScheduledTime *vectorTime `json:"attempt_scheduled_time"`
	Attempt              int32       `json:"attempt"`
}

func (e *vectorEpoch) proto() *streamservice.ProducerEpoch {
	if e == nil {
		return nil
	}
	return &streamservice.ProducerEpoch{
		TaskStartedEventId:   e.TaskStartedEventID,
		TaskAttempt:          e.TaskAttempt,
		TaskStartedTime:      e.TaskStartedTime.proto(),
		AttemptScheduledTime: e.AttemptScheduledTime.proto(),
		Attempt:              e.Attempt,
	}
}

var vectorKinds = map[string]streamservice.ProducerKind{
	"workflow":            streamservice.PRODUCER_KIND_WORKFLOW,
	"local_activity":      streamservice.PRODUCER_KIND_LOCAL_ACTIVITY,
	"activity":            streamservice.PRODUCER_KIND_ACTIVITY,
	"standalone_activity": streamservice.PRODUCER_KIND_STANDALONE_ACTIVITY,
	"client":              streamservice.PRODUCER_KIND_CLIENT,
}

func readVectors(t *testing.T, name string, into any) {
	data, err := os.ReadFile("testdata/" + name)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, into))
}

// vectorWithdraws applies a Superseded Record to a Record with the server's rules: the task rule
// of withdrawTasks for a Workflow item, and CompareEpochs for the other kinds that withdraw.
func vectorWithdraws(s *streamservice.Supersession, p *streamservice.Producer) bool {
	if s.GetWithdrawRun() != "" && p.GetRunId() == s.GetWithdrawRun() {
		return true
	}
	if s.GetEpoch() == nil || p.GetEpoch() == nil {
		return false
	}
	if s.GetProducerKind() != streamservice.PRODUCER_KIND_WORKFLOW {
		return withdrawsEpochs(s.GetProducerKind()) && p.GetKind() == s.GetProducerKind() &&
			p.GetId() == s.GetProducerId() && CompareEpochs(p.GetEpoch(), s.GetEpoch()) < 0
	}
	switch p.GetKind() {
	case streamservice.PRODUCER_KIND_WORKFLOW:
		if p.GetId() != s.GetProducerId() {
			return false
		}
	case streamservice.PRODUCER_KIND_LOCAL_ACTIVITY:
		if p.GetRunId() != s.GetRunId() {
			return false
		}
	default:
		return false
	}
	index := []*streamservice.WorkflowTaskEpoch{taskOf(p.GetEpoch())}
	split := withdrawTasks(index, taskOf(s.GetEpoch()), s.GetPreviousCompletedTask(),
		s.GetFailedTasks())
	return len(split.withdrawn) > 0
}

func TestWithdrawalsMatchTheCrateVectors(t *testing.T) {
	var vectors []struct {
		Name         string `json:"name"`
		Supersession struct {
			ProducerKind string       `json:"producer_kind"`
			ProducerID   string       `json:"producer_id"`
			RunID        string       `json:"run_id"`
			Epoch        *vectorEpoch `json:"epoch"`
			FailedTasks  []int64      `json:"failed_tasks"`
			WithdrawRun  string       `json:"withdraw_run"`
			Completed    *struct {
				StartedEventID int64       `json:"started_event_id"`
				Attempt        int32       `json:"attempt"`
				StartedTime    *vectorTime `json:"started_time"`
			} `json:"previous_completed_task"`
		} `json:"supersession"`
		Record struct {
			Producer struct {
				Kind  string       `json:"kind"`
				ID    string       `json:"id"`
				RunID string       `json:"run_id"`
				Epoch *vectorEpoch `json:"epoch"`
			} `json:"producer"`
		} `json:"record"`
		Withdraws bool `json:"withdraws"`
	}
	readVectors(t, "withdrawals.json", &vectors)
	require.NotEmpty(t, vectors)
	for _, v := range vectors {
		s := &streamservice.Supersession{
			ProducerKind: vectorKinds[v.Supersession.ProducerKind],
			ProducerId:   v.Supersession.ProducerID,
			RunId:        v.Supersession.RunID,
			Epoch:        v.Supersession.Epoch.proto(),
			FailedTasks:  v.Supersession.FailedTasks,
			WithdrawRun:  v.Supersession.WithdrawRun,
		}
		if c := v.Supersession.Completed; c != nil {
			s.PreviousCompletedTask = &streamservice.WorkflowTaskEpoch{
				StartedEventId: c.StartedEventID,
				Attempt:        c.Attempt,
				StartedTime:    c.StartedTime.proto(),
			}
		}
		p := v.Record.Producer
		producer := &streamservice.Producer{
			Kind: vectorKinds[p.Kind], Id: p.ID, RunId: p.RunID, Epoch: p.Epoch.proto(),
		}
		require.Equal(t, v.Withdraws, vectorWithdraws(s, producer), v.Name)
	}
}

// The crate's epoch keys sort as its epochs compare, so CompareEpochs must give the same order.
func TestCompareEpochsMatchesTheCrateKeys(t *testing.T) {
	var vectors []struct {
		Epoch vectorEpoch `json:"epoch"`
		Key   string      `json:"key"`
	}
	readVectors(t, "epochs.json", &vectors)
	require.NotEmpty(t, vectors)
	for _, a := range vectors {
		for _, b := range vectors {
			require.Equal(t, strings.Compare(a.Key, b.Key), CompareEpochs(a.Epoch.proto(), b.Epoch.proto()),
				"%s against %s", a.Key, b.Key)
		}
	}
}
