package streamnotifier

import (
	"cmp"
	"maps"
	"net/url"
	"slices"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	streamnotifierpb "go.temporal.io/server/chasm/lib/streamnotifier/gen/streamnotifierpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// BusinessID is the notifier's execution ID for a stream: its owner and topic, escaped so that no
// two streams map to one ID. The run ID is left out, so one notifier serves the stream across runs.
func BusinessID(ref *streampb.StreamReference) string {
	return url.PathEscape(ref.GetWorkflowId()) + "/" + url.PathEscape(ref.GetTopic())
}

var _ chasm.RootComponent = (*StreamNotifier)(nil)
var _ callback.CompletionSource = (*StreamNotifier)(nil)

// StreamNotifier holds the Nexus callbacks attached to one stream. It hands each notification to
// them as progress, folded, and completes them when the stream closes.
type StreamNotifier struct {
	chasm.UnimplementedComponent

	*streamnotifierpb.NotifierState

	// Callbacks by the request ID they were attached with.
	Callbacks chasm.Map[string, *callback.Callback]
}

// newStreamNotifier creates the notifier for a stream. The request that creates it records the
// activity that arms its expiry.
func newStreamNotifier(ref *streampb.StreamReference) *StreamNotifier {
	return &StreamNotifier{
		NotifierState: &streamnotifierpb.NotifierState{
			StreamRef: &streampb.StreamReference{
				OwnerKind:  ref.GetOwnerKind(),
				WorkflowId: ref.GetWorkflowId(),
				Topic:      ref.GetTopic(),
			},
		},
	}
}

func (n *StreamNotifier) LifecycleState(chasm.Context) chasm.LifecycleState {
	if n.Expired {
		return chasm.LifecycleStateCompleted
	}
	return chasm.LifecycleStateRunning
}

func (n *StreamNotifier) ContextMetadata(chasm.Context) map[string]string {
	return nil
}

// Terminate closes the stream with a failure, so the attached callbacks complete, and ends the
// execution.
func (n *StreamNotifier) Terminate(ctx chasm.MutableContext, _ chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	if !n.Closed {
		if err := n.close(ctx, nil, true); err != nil {
			return chasm.TerminateComponentResponse{}, err
		}
	}
	n.Expired = true
	return chasm.TerminateComponentResponse{}, nil
}

// touch records activity and arms the expiry for it.
func (n *StreamNotifier) touch(ctx chasm.MutableContext, after time.Duration) {
	now := timestamppb.New(ctx.Now(n))
	n.LastActivityTime = now
	ctx.AddTask(n, chasm.TaskAttributes{ScheduledTime: now.AsTime().Add(after)}, &streamnotifierpb.ExpiryTask{
		LastActivityTime: now,
	})
}

type attachInput struct {
	requestID    string
	callback     *commonpb.Callback_Nexus
	maxCallbacks int
	idleTimeout  time.Duration
}

// attach adds a callback, or changes nothing when one with the request ID is attached. On a
// closed stream the callback completes right away.
func (n *StreamNotifier) attach(ctx chasm.MutableContext, in attachInput) error {
	if n.Expired {
		return serviceerror.NewFailedPrecondition("the stream closed and its notifier no longer takes callbacks")
	}
	if _, ok := n.Callbacks[in.requestID]; ok {
		return nil
	}
	if len(n.Callbacks) >= in.maxCallbacks {
		return serviceerror.NewFailedPreconditionf("the stream notifier holds the maximum of %d callbacks", in.maxCallbacks)
	}
	cb := callback.NewCallback(in.requestID, timestamppb.New(ctx.Now(n)), &callbackspb.Callback{
		Variant: &callbackspb.Callback_Nexus_{
			Nexus: &callbackspb.Callback_Nexus{
				Url:    in.callback.GetUrl(),
				Header: maps.Clone(in.callback.GetHeader()),
			},
		},
	})
	if n.Callbacks == nil {
		n.Callbacks = make(chasm.Map[string, *callback.Callback])
	}
	n.Callbacks[in.requestID] = chasm.NewComponentField(ctx, cb)
	if n.Closed {
		return callback.TransitionScheduled.Apply(cb, ctx, callback.EventScheduled{})
	}
	if n.Counter > 0 {
		// A callback attached after notifications starts from the latest one.
		if err := cb.DeliverProgress(ctx, n.progress()); err != nil {
			return err
		}
	}
	n.touch(ctx, in.idleTimeout)
	return nil
}

// detach removes a callback. Its completion, if already scheduled, is not delivered.
func (n *StreamNotifier) detach(requestID string) {
	delete(n.Callbacks, requestID)
}

type notifyInput struct {
	position    string
	counter     int64
	metadata    map[string]string
	close       bool
	closeResult *commonpb.Payload
	idleTimeout time.Duration
	retention   time.Duration
}

// notify keeps the highest counter and hands it to every callback as progress. A close completes
// every callback and keeps the notifier taking late attaches for the closed retention.
func (n *StreamNotifier) notify(ctx chasm.MutableContext, in notifyInput) error {
	if n.Closed {
		return nil
	}
	if in.counter > n.Counter {
		n.Counter = in.counter
		n.Position = in.position
		n.Metadata = maps.Clone(in.metadata)
		progress := n.progress()
		for _, field := range n.Callbacks {
			if err := field.Get(ctx).DeliverProgress(ctx, progress); err != nil {
				return err
			}
		}
	}
	if in.close {
		if err := n.close(ctx, in.closeResult, false); err != nil {
			return err
		}
		n.touch(ctx, in.retention)
		return nil
	}
	n.touch(ctx, in.idleTimeout)
	return nil
}

func (n *StreamNotifier) close(ctx chasm.MutableContext, result *commonpb.Payload, idle bool) error {
	n.Closed = true
	n.IdleClosed = idle
	n.CloseResult = result
	n.CloseTime = timestamppb.New(ctx.Now(n))
	return callback.ScheduleStandbyCallbacks(ctx, n.Callbacks)
}

func (n *StreamNotifier) progress() *nexuspb.NexusOperationProgress {
	return &nexuspb.NexusOperationProgress{
		Position: n.Position,
		Counter:  n.Counter,
		Metadata: maps.Clone(n.Metadata),
	}
}

// GetNexusCompletion is the completion the callbacks deliver when the stream closes.
func (n *StreamNotifier) GetNexusCompletion(chasm.Context, string) (nexusrpc.CompleteOperationOptions, error) {
	completion := nexusrpc.CompleteOperationOptions{CloseTime: n.GetCloseTime().AsTime()}
	if n.IdleClosed {
		completion.Error = &nexus.OperationError{
			State: nexus.OperationStateFailed,
			Cause: &nexus.FailureError{Failure: nexus.Failure{Message: "the stream was idle for too long and its notifier closed it"}},
		}
		return completion, nil
	}
	if n.CloseResult != nil {
		completion.Result = common.CloneProto(n.CloseResult)
	}
	return completion, nil
}

func (n *StreamNotifier) describeCallbacks(ctx chasm.Context) ([]*streampb.StreamCallbackInfo, error) {
	callbacks := make([]*callback.Callback, 0, len(n.Callbacks))
	for _, field := range n.Callbacks {
		callbacks = append(callbacks, field.Get(ctx))
	}
	slices.SortFunc(callbacks, func(a, b *callback.Callback) int {
		return cmp.Or(
			a.GetRegistrationTime().AsTime().Compare(b.GetRegistrationTime().AsTime()),
			cmp.Compare(a.GetRequestId(), b.GetRequestId()),
		)
	})
	infos := make([]*streampb.StreamCallbackInfo, 0, len(callbacks))
	for _, cb := range callbacks {
		apiCallback, err := cb.ToAPICallback()
		if err != nil {
			return nil, err
		}
		state, _, err := cb.APIState(ctx)
		if err != nil {
			return nil, err
		}
		infos = append(infos, &streampb.StreamCallbackInfo{
			RequestId:               cb.GetRequestId(),
			Callback:                apiCallback,
			State:                   state,
			DeliveredCounter:        cb.GetDeliveredProgressCounter(),
			ProgressDisabled:        cb.GetProgressDisabled(),
			Attempt:                 max(cb.GetAttempt(), cb.GetProgressAttempt()),
			LastAttemptCompleteTime: common.CloneProto(cb.GetLastAttemptCompleteTime()),
			LastAttemptFailure:      common.CloneProto(cb.GetLastAttemptFailure()),
		})
	}
	return infos, nil
}
