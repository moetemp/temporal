package stream

import (
	"context"
	"errors"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/streamservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/resource"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ReaderRun is the open Workflow run that reads a page directly.
type ReaderRun struct {
	WorkflowID string
	RunID      string
}

// readerRunOf finds the reading run in the links of a direct call. A call with no Workflow link,
// such as a replay's read again, keeps no floor.
func readerRunOf(links []nexus.Link) *ReaderRun {
	for _, link := range links {
		if event, err := commonnexus.ConvertNexusLinkToLinkWorkflowEvent(link); err == nil &&
			event.GetWorkflowId() != "" && event.GetRunId() != "" {
			return &ReaderRun{WorkflowID: event.GetWorkflowId(), RunID: event.GetRunId()}
		}
	}
	return nil
}

// readerFloor is the floor that a read from from sets for the run. It is false when the Stream
// keeps no floors, when the run's floor is already that low, or when the read fails anyway.
func (s *Stream) readerFloor(ctx chasm.Context, runID string, from int64) (int64, bool) {
	if !s.Nested {
		return 0, false
	}
	if from == 0 {
		from = s.TrimmedOffset + 1
	}
	if from <= s.TrimmedOffset {
		return 0, false
	}
	if reader := s.reader(ctx, runID); reader != nil && reader.GetOffset() <= from {
		return 0, false
	}
	return from, true
}

// floorStart is where a run's floor starts for a read from start. A reset run copied the reads of
// the old run into its History, and its first live read names the lowest of them in floor_from,
// so the floor covers what its replay reads again.
func (s *Stream) floorStart(req *streamservice.ReadRecordsRequest, start int64) int64 {
	floorFrom := req.GetFloorFrom()
	if floorFrom <= 0 || start == 0 || floorFrom >= start {
		return start
	}
	return max(floorFrom, s.TrimmedOffset+1)
}

// suggestsContinueAsNew says whether a run's floor keeps more than half of the byte cap. The
// writer fails when the cap fills, and only the run's continue-as-new frees what its floor keeps.
func (s *Stream) suggestsContinueAsNew(ctx chasm.Context, runID string, limits Limits) bool {
	if runID == "" || limits.MaxStoredBytes <= 0 {
		return false
	}
	reader := s.reader(ctx, runID)
	if reader == nil {
		return false
	}
	var kept int64
	for _, info := range s.Index[s.firstBatchAt(reader.GetOffset()):] {
		kept += info.GetBytes()
	}
	return 2*kept > limits.MaxStoredBytes
}

// AddReader keeps the floor of a Workflow run that reads from from, in the transaction that
// answers the read. No trim passes the floor while the run is open.
func (s *Stream) AddReader(ctx chasm.MutableContext, run ReaderRun, from int64, limits Limits) error {
	floor, ok := s.readerFloor(ctx, run.RunID, from)
	if !ok {
		return nil
	}
	if _, known := s.Readers[run.RunID]; !known && len(s.Readers) >= limits.MaxReaders {
		return serviceerror.NewResourceExhausted(
			enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			"The Stream has the most Workflow readers")
	}
	s.setReader(ctx, run, floor)
	s.armReadersCheck(ctx, limits)
	return nil
}

// armReadersCheck arms the check of the reader runs while the Stream has floors.
func (s *Stream) armReadersCheck(ctx chasm.MutableContext, limits Limits) {
	if len(s.Readers) == 0 {
		s.ReadersCheckTime = nil
		return
	}
	if s.ReadersCheckTime != nil {
		return
	}
	s.ReadersCheckTime = timestamppb.New(ctx.Now(s).Add(limits.ReaderCheck))
	ctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: s.ReadersCheckTime.AsTime()},
		&streampb.StreamReadersCheckTask{Deadline: s.ReadersCheckTime})
}

// readersCheckTaskHandler drops the floors of the reader runs that closed. A run that continued
// as new is closed too, since the new run's History has none of its reads.
type readersCheckTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*streampb.StreamReadersCheckTask]

	config            *Config
	namespaceRegistry namespace.Registry
	historyClient     resource.HistoryClient
}

func newReadersCheckTaskHandler(
	config *Config,
	namespaceRegistry namespace.Registry,
	historyClient resource.HistoryClient,
) *readersCheckTaskHandler {
	return &readersCheckTaskHandler{config: config, namespaceRegistry: namespaceRegistry, historyClient: historyClient}
}

func (h *readersCheckTaskHandler) Validate(
	_ chasm.Context,
	s *Stream,
	_ chasm.TaskInvocation,
	task *streampb.StreamReadersCheckTask,
) (bool, error) {
	return s.GetReadersCheckTime() != nil && s.GetReadersCheckTime().AsTime().Equal(task.GetDeadline().AsTime()), nil
}

func (h *readersCheckTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *streampb.StreamReadersCheckTask,
) error {
	readers, err := chasm.ReadComponent(ctx, ref,
		func(s *Stream, cctx chasm.Context, _ struct{}) (map[string]string, error) {
			runs := make(map[string]string, len(s.Readers))
			for runID := range s.Readers {
				runs[runID] = s.reader(cctx, runID).GetWorkflowId()
			}
			return runs, nil
		}, struct{}{})
	if err != nil {
		return err
	}
	var closed []string
	for runID, workflowID := range readers {
		open, err := runOpen(ctx, h.historyClient, ref.NamespaceID, ReaderRun{WorkflowID: workflowID, RunID: runID})
		if err != nil {
			return err
		}
		if !open {
			closed = append(closed, runID)
		}
	}
	ns, err := h.namespaceRegistry.GetNamespaceByID(namespace.ID(ref.NamespaceID))
	if err != nil {
		return err
	}
	limits := h.config.LimitsFor(ns.Name().String())
	_, _, err = chasm.UpdateComponent(ctx, ref,
		func(s *Stream, mctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			s.dropReaders(mctx, closed)
			// Cleared first, so the transition arms the next check while floors are left.
			s.ReadersCheckTime = nil
			s.ApplyDue(mctx, limits)
			return struct{}{}, nil
		}, struct{}{})
	return err
}

// runOpen says whether a Workflow run is still open. A deleted or unknown run is closed. The run may be on
// another shard, so it goes through the history client.
func runOpen(ctx context.Context, historyClient resource.HistoryClient, namespaceID string, run ReaderRun) (bool, error) {
	resp, err := historyClient.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId: namespaceID,
		Execution:   &commonpb.WorkflowExecution{WorkflowId: run.WorkflowID, RunId: run.RunID},
	})
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return false, nil
	}
	// A link can name a run id that is no run id at all.
	if _, ok := errors.AsType[*serviceerror.InvalidArgument](err); ok {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return resp.GetWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, nil
}
