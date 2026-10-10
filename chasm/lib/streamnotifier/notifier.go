package streamnotifier

import (
	"cmp"
	"maps"
	"net/url"
	"slices"
	"strings"
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

// BusinessID is the notifier's execution ID for a stream: its owner kind, owner, the first run of
// the owner's run chain, and topic, escaped so that no two streams map to one ID. One notifier
// serves the stream across the chain's runs, and a later chain that reuses the Workflow ID and
// topic gets a notifier of its own.
func BusinessID(ref *streampb.StreamReference) string {
	kind := strings.ToLower(strings.TrimPrefix(ref.GetOwnerKind().String(), "STREAM_OWNER_KIND_"))
	return kind + "/" + url.PathEscape(ref.GetWorkflowId()) + "/" + url.PathEscape(ref.GetRunId()) + "/" + url.PathEscape(ref.GetTopic())
}

var _ chasm.RootComponent = (*StreamNotifier)(nil)
var _ callback.CompletionSource = (*StreamNotifier)(nil)
var _ callback.ProgressSource = (*StreamNotifier)(nil)

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
				RunId:      ref.GetRunId(),
				Topic:      ref.GetTopic(),
			},
		},
	}
}

// LifecycleState is Completed for a closed stream whose retention passed, so the stream stays
// closed, and Failed for an open one that went idle with no callbacks, so a later attach or
// notification starts a new notifier.
func (n *StreamNotifier) LifecycleState(chasm.Context) chasm.LifecycleState {
	switch {
	case n.Expired && n.Closed:
		return chasm.LifecycleStateCompleted
	case n.Expired:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

func (n *StreamNotifier) ContextMetadata(chasm.Context) map[string]string {
	return nil
}

// Terminate closes the stream with a failure, so the attached callbacks complete, and ends the
// execution.
func (n *StreamNotifier) Terminate(ctx chasm.MutableContext, _ chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	if !n.Closed {
		if err := n.close(ctx, nil, "the stream's notifier was terminated"); err != nil {
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
	requestID          string
	callback           *commonpb.Callback_Nexus
	operationToken     string
	startTime          *timestamppb.Timestamp
	maxCallbacks       int
	idleTimeout        time.Duration
	ownerCheckInterval time.Duration
}

// attach adds a callback, or changes nothing when one with the request ID is attached. On a
// closed stream the callback completes right away. A full notifier makes room only by dropping
// callbacks that are done.
func (n *StreamNotifier) attach(ctx chasm.MutableContext, in attachInput) error {
	if n.Expired {
		return serviceerror.NewFailedPrecondition("the stream closed and its notifier no longer takes callbacks")
	}
	if _, ok := n.Callbacks[in.requestID]; ok {
		return nil
	}
	n.dropClosedCallers(ctx)
	if len(n.Callbacks) >= in.maxCallbacks && !n.evictOne(ctx) {
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
	if in.operationToken != "" || in.startTime != nil {
		if n.CallerStarts == nil {
			n.CallerStarts = map[string]*streamnotifierpb.CallerStart{}
		}
		n.CallerStarts[in.requestID] = &streamnotifierpb.CallerStart{OperationToken: in.operationToken, StartTime: in.startTime}
	}
	if n.Closed {
		return callback.TransitionScheduled.Apply(cb, ctx, callback.EventScheduled{})
	}
	if n.Counter > 0 {
		// A callback attached after notifications starts from the latest one.
		if err := cb.DeliverProgress(ctx, n.progress()); err != nil {
			return err
		}
	}
	n.armOwnerCheck(ctx, in.ownerCheckInterval)
	n.touch(ctx, in.idleTimeout)
	return nil
}

// dropClosedCallers forgets the callbacks whose caller's operation closed: nothing will read their
// progress or their completion.
func (n *StreamNotifier) dropClosedCallers(ctx chasm.MutableContext) {
	for requestID, field := range n.Callbacks {
		if field.Get(ctx).GetCallerOperationClosed() {
			n.detach(requestID)
		}
	}
}

// evictOne drops the oldest callback that is done and reports whether it dropped one. A callback
// whose caller still waits for its completion is never dropped, even one that takes no progress,
// such as an HSM caller's; callbacks whose caller closed are already gone (dropClosedCallers).
func (n *StreamNotifier) evictOne(ctx chasm.MutableContext) bool {
	var done *callback.Callback
	for _, field := range n.Callbacks {
		cb := field.Get(ctx)
		if cb.Status != callbackspb.CALLBACK_STATUS_SUCCEEDED && cb.Status != callbackspb.CALLBACK_STATUS_FAILED {
			continue
		}
		if done == nil || cb.GetRegistrationTime().AsTime().Before(done.GetRegistrationTime().AsTime()) {
			done = cb
		}
	}
	if done == nil {
		return false
	}
	n.detach(done.GetRequestId())
	return true
}

// hasWaitingCallbacks reports whether a callback still waits for the stream's close.
func (n *StreamNotifier) hasWaitingCallbacks(ctx chasm.Context) bool {
	for _, field := range n.Callbacks {
		cb := field.Get(ctx)
		if cb.Status == callbackspb.CALLBACK_STATUS_STANDBY && !cb.GetCallerOperationClosed() {
			return true
		}
	}
	return false
}

// armOwnerCheck arms an owner check if none is armed, while callbacks wait for the close.
func (n *StreamNotifier) armOwnerCheck(ctx chasm.MutableContext, interval time.Duration) {
	if n.Closed || n.OwnerCheckTime != nil || !n.hasWaitingCallbacks(ctx) {
		return
	}
	at := timestamppb.New(ctx.Now(n).Add(interval))
	n.OwnerCheckTime = at
	ctx.AddTask(n, chasm.TaskAttributes{ScheduledTime: at.AsTime()}, &streamnotifierpb.OwnerCheckTask{ScheduledTime: at})
}

// failIdleCallbacks fails the callbacks that wait for the close, keeping the stream open, and
// reports whether there were any.
func (n *StreamNotifier) failIdleCallbacks(ctx chasm.MutableContext) (bool, error) {
	failed := false
	for requestID, field := range n.Callbacks {
		cb := field.Get(ctx)
		if cb.Status != callbackspb.CALLBACK_STATUS_STANDBY || cb.GetCallerOperationClosed() {
			continue
		}
		n.IdleFailedRequestIds = append(n.IdleFailedRequestIds, requestID)
		if err := callback.TransitionScheduled.Apply(cb, ctx, callback.EventScheduled{}); err != nil {
			return false, err
		}
		failed = true
	}
	slices.Sort(n.IdleFailedRequestIds)
	return failed, nil
}

// detach removes a callback. Its completion, if already scheduled, is not delivered.
func (n *StreamNotifier) detach(requestID string) {
	delete(n.Callbacks, requestID)
	delete(n.CallerStarts, requestID)
	n.IdleFailedRequestIds = slices.DeleteFunc(n.IdleFailedRequestIds, func(id string) bool { return id == requestID })
	n.CanceledRequestIds = slices.DeleteFunc(n.CanceledRequestIds, func(id string) bool { return id == requestID })
}

// cancel completes a waiting callback with a cancellation, because its caller canceled the
// operation and is waiting for that completion. A callback already completing keeps its completion.
func (n *StreamNotifier) cancel(ctx chasm.MutableContext, requestID string) error {
	field, ok := n.Callbacks[requestID]
	if !ok {
		return nil
	}
	cb := field.Get(ctx)
	if cb.Status != callbackspb.CALLBACK_STATUS_STANDBY {
		return nil
	}
	n.CanceledRequestIds = append(n.CanceledRequestIds, requestID)
	slices.Sort(n.CanceledRequestIds)
	return callback.TransitionScheduled.Apply(cb, ctx, callback.EventScheduled{})
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
	n.dropClosedCallers(ctx)
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
		if err := n.close(ctx, in.closeResult, ""); err != nil {
			return err
		}
		n.touch(ctx, in.retention)
		return nil
	}
	n.touch(ctx, in.idleTimeout)
	return nil
}

// close completes every callback still waiting: with the result, or with failure when it is set.
func (n *StreamNotifier) close(ctx chasm.MutableContext, result *commonpb.Payload, failure string) error {
	n.Closed = true
	n.CloseFailure = failure
	n.CloseResult = result
	n.CloseTime = timestamppb.New(ctx.Now(n))
	n.OwnerCheckTime = nil
	return callback.ScheduleStandbyCallbacks(ctx, n.Callbacks)
}

func (n *StreamNotifier) progress() *nexuspb.NexusOperationProgress {
	return &nexuspb.NexusOperationProgress{
		Position: n.Position,
		Counter:  n.Counter,
		Metadata: maps.Clone(n.Metadata),
	}
}

// GetNexusProgressOptions carries the caller's operation token and start time on each progress
// delivery, so a caller that has not seen the start response yet can still tell the operation.
func (n *StreamNotifier) GetNexusProgressOptions(_ chasm.Context, requestID string) (nexusrpc.CompleteOperationOptions, error) {
	return n.callerStart(requestID), nil
}

func (n *StreamNotifier) callerStart(requestID string) nexusrpc.CompleteOperationOptions {
	var options nexusrpc.CompleteOperationOptions
	if start, ok := n.CallerStarts[requestID]; ok {
		options.OperationToken = start.GetOperationToken()
		if start.GetStartTime() != nil {
			options.StartTime = start.GetStartTime().AsTime()
		}
	}
	return options
}

// GetNexusCompletion is the completion a callback delivers: a cancellation if its caller canceled,
// a failure if an idle timeout failed it or the stream closed with a failure, otherwise the
// stream's close result. Each carries the caller's operation token and start time, since a
// completion can reach the caller before the handler's start response does.
func (n *StreamNotifier) GetNexusCompletion(ctx chasm.Context, requestID string) (nexusrpc.CompleteOperationOptions, error) {
	completion := n.callerStart(requestID)
	completion.CloseTime = n.GetCloseTime().AsTime()
	if _, canceled := slices.BinarySearch(n.CanceledRequestIds, requestID); canceled {
		completion.CloseTime = ctx.Now(n)
		completion.Error = &nexus.OperationError{
			State: nexus.OperationStateCanceled,
			Cause: &nexus.FailureError{Failure: nexus.Failure{Message: "the caller canceled the operation"}},
		}
		return completion, nil
	}
	failure := n.CloseFailure
	if _, idle := slices.BinarySearch(n.IdleFailedRequestIds, requestID); idle {
		failure = "the stream was idle for too long, so its notifier failed the callers waiting on it"
		completion.CloseTime = ctx.Now(n)
	}
	if failure != "" {
		completion.Error = &nexus.OperationError{
			State: nexus.OperationStateFailed,
			Cause: &nexus.FailureError{Failure: nexus.Failure{Message: failure}},
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
		// The header holds the caller's callback token. A reader of this namespace could forge a
		// completion with it, also for a caller in another namespace.
		if nexusCallback := apiCallback.GetNexus(); nexusCallback != nil {
			nexusCallback.Header = nil
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
