package stream

import (
	"context"
	"errors"

	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/streamservice/v1"
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/namespace"
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

// readerFloor is the floor that a read of req by the run sets. It is false when the run's floor
// is already that low, or when the read fails anyway.
func (s *Stream) readerFloor(runID string, req *streamservice.ReadRecordsRequest) (int64, bool) {
	from, err := s.StartOffset(req)
	if err != nil {
		return 0, false
	}
	if from == 0 {
		from = s.TrimmedOffset + 1
	}
	if from <= s.TrimmedOffset {
		return 0, false
	}
	if reader, ok := s.Readers[runID]; ok && reader.GetOffset() <= from {
		return 0, false
	}
	return from, true
}

// AddReader keeps the floor of a Workflow run that reads from req's start. No trim passes it
// while the run is open.
func (s *Stream) AddReader(
	ctx chasm.MutableContext,
	run ReaderRun,
	req *streamservice.ReadRecordsRequest,
	limits Limits,
) error {
	floor, ok := s.readerFloor(run.RunID, req)
	if !ok {
		return nil
	}
	if _, known := s.Readers[run.RunID]; !known && len(s.Readers) >= limits.MaxReaders {
		return serviceerror.NewResourceExhausted(
			enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			"The Stream has the most Workflow readers")
	}
	if s.Readers == nil {
		s.Readers = make(map[string]*streampb.ReaderFloor)
	}
	s.Readers[run.RunID] = &streampb.ReaderFloor{WorkflowId: run.WorkflowID, Offset: floor}
	s.finish(ctx, limits)
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
}

func newReadersCheckTaskHandler(config *Config, namespaceRegistry namespace.Registry) *readersCheckTaskHandler {
	return &readersCheckTaskHandler{config: config, namespaceRegistry: namespaceRegistry}
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
		func(s *Stream, _ chasm.Context, _ struct{}) (map[string]string, error) {
			runs := make(map[string]string, len(s.Readers))
			for runID, reader := range s.Readers {
				runs[runID] = reader.GetWorkflowId()
			}
			return runs, nil
		}, struct{}{})
	if err != nil {
		return err
	}
	var closed []string
	for runID, workflowID := range readers {
		open, err := runOpen(ctx, chasm.ExecutionKey{NamespaceID: ref.NamespaceID, BusinessID: workflowID, RunID: runID})
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
			for _, runID := range closed {
				delete(s.Readers, runID)
			}
			// Cleared first, so the transition arms the next check while floors are left.
			s.ReadersCheckTime = nil
			s.ApplyDue(mctx, limits)
			return struct{}{}, nil
		}, struct{}{})
	return err
}

// runOpen says whether a Workflow run is still open. A deleted run is closed.
func runOpen(ctx context.Context, key chasm.ExecutionKey) (bool, error) {
	open, err := chasm.ReadComponent(ctx, chasm.NewComponentRefByArchetypeID(key, chasm.WorkflowArchetypeID),
		func(h Host, cctx chasm.Context, _ struct{}) (bool, error) {
			return !h.LifecycleState(cctx).IsClosed(), nil
		}, struct{}{})
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return false, nil
	}
	return open, err
}
