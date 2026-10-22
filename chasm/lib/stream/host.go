package stream

import (
	"context"
	"errors"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/streamservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

// Host is the Workflow component, which nests the Streams of its run chain. A Workflow Stream
// commits in the Workflow's transaction, so an append and the wake of its readers go together.
type Host interface {
	chasm.Component
	callback.CompletionSource
	NestedStreams() chasm.Map[string, *Stream]
}

// target is where a Stream lives: its own execution keyed by the Stream id, or a child of the
// Workflow component of its chain's first run.
type target struct {
	key    chasm.ExecutionKey
	id     string
	nested bool
}

// targetOf finds the Stream that a ref names. A Workflow Stream nests in the first run of its
// chain, since a run's CHASM tree doesn't move to the next run.
func targetOf(namespaceID, streamID string, ref *streamservice.StreamRef) (target, error) {
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
	}, nil
}

// newHeldStream makes a Workflow Stream, which holds its Records while its chain is open.
func newHeldStream(ctx chasm.MutableContext) (*Stream, error) {
	s := NewStream(ctx, streampb.STREAM_PHASE_PENDING)
	s.HoldRecords = true
	return s, nil
}

func (t target) hostRef() chasm.ComponentRef {
	return chasm.NewComponentRefByArchetypeID(t.key, chasm.WorkflowArchetypeID)
}

// update runs fn on the Stream. With start set, a missing nested Stream is made first.
func update[O any](
	ctx context.Context,
	t target,
	start func(chasm.MutableContext) (*Stream, error),
	fn func(*Stream, chasm.MutableContext) (O, error),
) (O, error) {
	if !t.nested {
		out, _, err := chasm.UpdateComponent(ctx, chasm.NewComponentRef[*Stream](t.key),
			func(s *Stream, mctx chasm.MutableContext, _ struct{}) (O, error) { return fn(s, mctx) }, struct{}{})
		return out, err
	}
	out, _, err := chasm.UpdateComponent(ctx, t.hostRef(),
		func(h Host, mctx chasm.MutableContext, _ struct{}) (O, error) {
			streams := h.NestedStreams()
			if field, ok := streams[t.id]; ok {
				return fn(field.Get(mctx), mctx)
			}
			var zero O
			if start == nil {
				return zero, notFound()
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
	return out, err
}

// read runs fn on the Stream with no write.
func read[O any](ctx context.Context, t target, fn func(*Stream, chasm.Context) (O, error)) (O, error) {
	if !t.nested {
		return chasm.ReadComponent(ctx, chasm.NewComponentRef[*Stream](t.key),
			func(s *Stream, cctx chasm.Context, _ struct{}) (O, error) { return fn(s, cctx) }, struct{}{})
	}
	return chasm.ReadComponent(ctx, t.hostRef(),
		func(h Host, cctx chasm.Context, _ struct{}) (O, error) {
			field, ok := h.NestedStreams()[t.id]
			if !ok {
				var zero O
				return zero, notFound()
			}
			return fn(field.Get(cctx), cctx)
		}, struct{}{})
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

func (h *closeChainTaskHandler) Validate(chasm.Context, Host, chasm.TaskInvocation, *streampb.StreamCloseChainTask) (bool, error) {
	return true, nil
}

func (h *closeChainTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *streampb.StreamCloseChainTask,
) error {
	completion, err := chasm.ReadComponent(ctx, ref,
		func(h Host, cctx chasm.Context, _ struct{}) (nexusrpc.CompleteOperationOptions, error) {
			return h.GetNexusCompletion(cctx, "")
		}, struct{}{})
	if err != nil {
		return err
	}
	result, failure, err := outcome(completion)
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
	}
	_, _, err = chasm.UpdateComponent(ctx, first.hostRef(),
		func(h Host, mctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			for _, field := range h.NestedStreams() {
				s := field.Get(mctx)
				// The chain ended, so the Retention and the size cap apply from now on.
				s.HoldRecords = false
				if s.GetCloseWithRun() && s.Phase == streampb.STREAM_PHASE_OPEN {
					s.close(mctx, s.GetOwner(), result, failure)
				}
				s.finish(mctx, limits)
			}
			return struct{}{}, nil
		}, struct{}{})
	// The chain's first run is gone after its retention, and then so are its Streams.
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return nil
	}
	return err
}

// outcome gives the result or the failure of a closed run from its Nexus completion.
func outcome(completion nexusrpc.CompleteOperationOptions) (*commonpb.Payload, *failurepb.Failure, error) {
	if completion.Error == nil {
		if result, ok := completion.Result.(*commonpb.Payload); ok {
			return result, nil, nil
		}
		return nil, nil, nil
	}
	nf, err := nexusrpc.DefaultFailureConverter().ErrorToFailure(completion.Error)
	if err != nil {
		return nil, nil, err
	}
	failure, err := commonnexus.NexusFailureToTemporalFailure(nf)
	if err != nil {
		return nil, nil, err
	}
	// The operation error wraps the run's own failure.
	if cause := failure.GetCause(); cause != nil && completion.Error.State != nexus.OperationStateCanceled {
		return nil, cause, nil
	}
	return nil, failure, nil
}
