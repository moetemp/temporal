package stream

import (
	"context"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/streamservice/v1"
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

// Host is a component that nests Streams: the Workflow, for the Streams of its run chain, or a
// Standalone Activity, for its own. A nested Stream commits in its host's transaction, so an
// append and the wake of its readers go together.
type Host interface {
	chasm.Component
	NestedStreams() chasm.Map[string, *Stream]
	// CheckOwner refuses a create whose owner the host doesn't prove, with the task token of its
	// current attempt. A Workflow's chain is checked before the transaction.
	CheckOwner(ctx chasm.Context, owner *streamservice.Producer, token []byte) error
	// CheckWriter refuses a write that claims the host's own Producer with no proof.
	CheckWriter(ctx chasm.Context, producer *streamservice.Producer, token []byte) error
}

// ChainHost is the Workflow component of a run that ends a chain.
type ChainHost interface {
	Host
	// RunEnd is how this closed run ended.
	RunEnd(ctx chasm.Context) (RunEnd, error)
}

// ActivityArchetypeID is the archetype of the Activity library's component, which hosts a
// Standalone Activity's Streams. The activity library imports this one, so the id is made here.
var ActivityArchetypeID = chasm.GenerateTypeID(chasm.FullyQualifiedName("activity", "activity"))

// RunEnd is how the run that ends a chain ended. The chain's close-with-run Streams close with it.
type RunEnd struct {
	Result  *commonpb.Payload
	Failure *failurepb.Failure
	Status  streamservice.CloseStatus
	RunID   string
	// The started event id of the run's last completed Workflow Task, zero if none.
	LastCompleted int64
}

// EndChain closes the Stream with the end of its run chain when it closes with the run. First it
// withdraws the closing run's Records of tasks after its last completed Workflow Task, since a
// terminate or a timeout ends the run while a task is open, and that task never completes.
func (s *Stream) EndChain(ctx chasm.MutableContext, end RunEnd, limits Limits) {
	if s.Phase == streampb.STREAM_PHASE_OPEN || s.Phase == streampb.STREAM_PHASE_CLOSED {
		s.withdrawRunTail(ctx, end)
	}
	if s.GetCloseWithRun() && s.Phase == streampb.STREAM_PHASE_OPEN {
		s.close(ctx, s.GetOwner(), end.Result, end.Failure, end.Status)
	}
	s.finish(ctx, limits)
}

// EndOwner closes a Standalone Activity's Stream with the Activity's end, in the transition that
// ends it. A retry doesn't end the Activity, so the Stream goes on across attempts.
func (s *Stream) EndOwner(ctx chasm.MutableContext, end RunEnd, limits Limits) {
	if s.Phase == streampb.STREAM_PHASE_OPEN {
		s.close(ctx, s.GetOwner(), end.Result, end.Failure, end.Status)
	}
	s.finish(ctx, limits)
}

// target is where a Stream lives: its own execution keyed by the Stream id, or a child of its
// host, the Workflow component of its chain's first run or a Standalone Activity.
type target struct {
	key    chasm.ExecutionKey
	id     string
	nested bool
	host   chasm.ArchetypeID
}

// workflow says whether the Stream nests in a Workflow's chain.
func (t target) workflow() bool {
	return t.nested && t.host == chasm.WorkflowArchetypeID
}

// activity says whether the Stream nests in a Standalone Activity.
func (t target) activity() bool {
	return t.nested && t.host == ActivityArchetypeID
}

// targetOf finds the Stream that a ref names. A Workflow Stream nests in the first run of its
// chain, since a run's CHASM tree doesn't move to the next run.
func targetOf(namespaceID, streamID string, ref *streamservice.StreamRef) (target, error) {
	if ref.GetWorkflowId() != "" && ref.GetActivityId() != "" {
		return target{}, invalid("A Stream ref names a Workflow or an Activity, not both")
	}
	if ref.GetActivityId() != "" {
		// An empty activity_run_id is the Activity's current run.
		return target{
			key: chasm.ExecutionKey{
				NamespaceID: namespaceID,
				BusinessID:  ref.GetActivityId(),
				RunID:       ref.GetActivityRunId(),
			},
			id:     streamID,
			nested: true,
			host:   ActivityArchetypeID,
		}, nil
	}
	if ref.GetWorkflowId() == "" {
		return target{key: chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: streamID}, id: streamID}, nil
	}
	if ref.GetFirstExecutionRunId() == "" {
		return target{}, invalid("A Workflow Stream ref needs first_execution_run_id")
	}
	return target{
		key: chasm.ExecutionKey{
			NamespaceID: namespaceID,
			BusinessID:  ref.GetWorkflowId(),
			RunID:       ref.GetFirstExecutionRunId(),
		},
		id:     streamID,
		nested: true,
		host:   chasm.WorkflowArchetypeID,
	}, nil
}

// newNestedStream makes a nested Stream, whose Workflow readers keep their floors.
func newNestedStream(ctx chasm.MutableContext) (*Stream, error) {
	s := NewStream(ctx, streampb.STREAM_PHASE_PENDING)
	s.Nested = true
	return s, nil
}

func (t target) hostRef() chasm.ComponentRef {
	return chasm.NewComponentRefByArchetypeID(t.key, t.host)
}

// update runs fn on the Stream. With start set, a missing nested Stream is made first. With check
// set, the host of a nested Stream checks the call first.
func update[O any](
	ctx context.Context,
	t target,
	start func(chasm.MutableContext) (*Stream, error),
	check func(Host, chasm.Context) error,
	update func(*Stream, chasm.MutableContext) (O, error),
) (O, error) {
	fn := func(s *Stream, mctx chasm.MutableContext) (O, error) {
		out, err := update(s, mctx)
		return out, carry(err)
	}
	if !t.nested {
		out, _, err := chasm.UpdateComponent(ctx, chasm.NewComponentRef[*Stream](t.key),
			func(s *Stream, mctx chasm.MutableContext, _ struct{}) (O, error) { return fn(s, mctx) }, struct{}{})
		return out, uncarry(err)
	}
	out, _, err := chasm.UpdateComponent(ctx, t.hostRef(),
		func(h Host, mctx chasm.MutableContext, _ struct{}) (O, error) {
			var zero O
			if check != nil {
				if err := check(h, mctx); err != nil {
					return zero, carry(err)
				}
			}
			streams := h.NestedStreams()
			if field, ok := streams[t.id]; ok {
				return fn(field.Get(mctx), mctx)
			}
			if start == nil {
				return zero, carry(notFound())
			}
			s, err := start(mctx)
			if err != nil {
				return zero, err
			}
			out, err := fn(s, mctx)
			if err != nil {
				return zero, err
			}
			streams[t.id] = chasm.NewComponentField(mctx, s)
			return out, nil
		}, struct{}{})
	return out, uncarry(err)
}

// read runs fn on the Stream with no write.
func read[O any](ctx context.Context, t target, view func(*Stream, chasm.Context) (O, error)) (O, error) {
	fn := func(s *Stream, cctx chasm.Context) (O, error) {
		out, err := view(s, cctx)
		return out, carry(err)
	}
	if !t.nested {
		out, err := chasm.ReadComponent(ctx, chasm.NewComponentRef[*Stream](t.key),
			func(s *Stream, cctx chasm.Context, _ struct{}) (O, error) { return fn(s, cctx) }, struct{}{})
		return out, uncarry(err)
	}
	out, err := chasm.ReadComponent(ctx, t.hostRef(),
		func(h Host, cctx chasm.Context, _ struct{}) (O, error) {
			field, ok := h.NestedStreams()[t.id]
			if !ok {
				var zero O
				return zero, carry(notFound())
			}
			return fn(field.Get(cctx), cctx)
		}, struct{}{})
	return out, uncarry(err)
}

// carriedError holds a Stream error through the framework, which rewrites a component's NotFound
// error into a missing execution. stream_expired has the NotFound code too.
type carriedError struct{ err error }

func (e *carriedError) Error() string { return e.err.Error() }

func carry(err error) error {
	if ReasonOf(err) == "" {
		return err
	}
	return &carriedError{err: err}
}

func uncarry(err error) error {
	if carried, ok := errors.AsType[*carriedError](err); ok {
		return carried.err
	}
	return err
}

// poll waits until done holds for the Stream. It answers no error when the context ends first.
func poll(ctx context.Context, t target, done func(*Stream) bool) error {
	var err error
	if !t.nested {
		_, _, err = chasm.PollComponent(ctx, chasm.NewComponentRef[*Stream](t.key),
			func(s *Stream, _ chasm.Context, _ struct{}) (struct{}, bool, error) { return struct{}{}, done(s), nil },
			struct{}{})
	} else {
		_, _, err = chasm.PollComponent(ctx, t.hostRef(),
			func(h Host, cctx chasm.Context, _ struct{}) (struct{}, bool, error) {
				field, ok := h.NestedStreams()[t.id]
				return struct{}{}, ok && done(field.Get(cctx)), nil
			}, struct{}{})
	}
	return err
}

// closeChainTaskHandler closes the close-with-run Streams nested in the chain's first run when
// the chain's last run closes. Both runs are on the Workflow's shard.
type closeChainTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*streampb.StreamCloseChainTask]

	config            *Config
	namespaceRegistry namespace.Registry
}

func newCloseChainTaskHandler(config *Config, namespaceRegistry namespace.Registry) *closeChainTaskHandler {
	return &closeChainTaskHandler{config: config, namespaceRegistry: namespaceRegistry}
}

func (h *closeChainTaskHandler) Validate(
	chasm.Context,
	ChainHost,
	chasm.TaskInvocation,
	*streampb.StreamCloseChainTask,
) (bool, error) {
	return true, nil
}

func (h *closeChainTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *streampb.StreamCloseChainTask,
) error {
	end, err := chasm.ReadComponent(ctx, ref,
		func(h ChainHost, cctx chasm.Context, _ struct{}) (RunEnd, error) {
			return h.RunEnd(cctx)
		}, struct{}{})
	if err != nil {
		return err
	}
	ns, err := h.namespaceRegistry.GetNamespaceByID(namespace.ID(ref.NamespaceID))
	if err != nil {
		return err
	}
	limits := h.config.LimitsFor(ns.Name().String())
	first := target{
		key:    chasm.ExecutionKey{NamespaceID: ref.NamespaceID, BusinessID: ref.BusinessID, RunID: task.GetFirstRunId()},
		nested: true,
		host:   chasm.WorkflowArchetypeID,
	}
	_, _, err = chasm.UpdateComponent(ctx, first.hostRef(),
		func(h Host, mctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			for _, field := range h.NestedStreams() {
				field.Get(mctx).EndChain(mctx, end, limits)
			}
			return struct{}{}, nil
		}, struct{}{})
	// The chain's first run is gone after its retention, and then so are its Streams.
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return nil
	}
	return err
}

// RunEndOfCompletion gives the end of a closed run from its Nexus completion and its status.
func RunEndOfCompletion(
	completion nexusrpc.CompleteOperationOptions,
	status enumspb.WorkflowExecutionStatus,
) (RunEnd, error) {
	end := RunEnd{Status: closeStatusOf(status)}
	if completion.Error == nil {
		if result, ok := completion.Result.(*commonpb.Payload); ok {
			end.Result = result
		}
		return end, nil
	}
	nf, err := nexusrpc.DefaultFailureConverter().ErrorToFailure(completion.Error)
	if err != nil {
		return RunEnd{}, err
	}
	failure, err := commonnexus.NexusFailureToTemporalFailure(nf)
	if err != nil {
		return RunEnd{}, err
	}
	// The operation error wraps the run's own failure.
	end.Failure = failure
	if cause := failure.GetCause(); cause != nil {
		end.Failure = cause
	}
	return end, nil
}

// RunEndOfEvent gives the end of a run from its close event, in the same shape as
// RunEndOfCompletion, so a close in the run's own transaction and a close by the task agree.
func RunEndOfEvent(event *historypb.HistoryEvent) RunEnd {
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		end := RunEnd{Status: streamservice.CLOSE_STATUS_COMPLETED}
		attrs := event.GetWorkflowExecutionCompletedEventAttributes()
		if results := attrs.GetResult().GetPayloads(); len(results) > 0 {
			end.Result = results[0]
		}
		return end
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		return RunEnd{
			Status:  streamservice.CLOSE_STATUS_FAILED,
			Failure: event.GetWorkflowExecutionFailedEventAttributes().GetFailure(),
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		details := event.GetWorkflowExecutionCanceledEventAttributes().GetDetails()
		return RunEnd{Status: streamservice.CLOSE_STATUS_CANCELED, Failure: &failurepb.Failure{
			Message: "operation canceled",
			FailureInfo: &failurepb.Failure_CanceledFailureInfo{
				CanceledFailureInfo: &failurepb.CanceledFailureInfo{Details: details},
			},
		}}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		return RunEnd{Status: streamservice.CLOSE_STATUS_TERMINATED, Failure: &failurepb.Failure{
			Message: "operation terminated",
			FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
				TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{},
			},
		}}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		return RunEnd{Status: streamservice.CLOSE_STATUS_TIMED_OUT, Failure: &failurepb.Failure{
			Message: "operation exceeded internal timeout",
			FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
				TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{},
			},
		}}
	default:
		return RunEnd{Status: streamservice.CLOSE_STATUS_COMPLETED}
	}
}

func closeStatusOf(status enumspb.WorkflowExecutionStatus) streamservice.CloseStatus {
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		return streamservice.CLOSE_STATUS_FAILED
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return streamservice.CLOSE_STATUS_CANCELED
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return streamservice.CLOSE_STATUS_TERMINATED
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return streamservice.CLOSE_STATUS_TIMED_OUT
	default:
		return streamservice.CLOSE_STATUS_COMPLETED
	}
}
