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
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	streamnotifierpb "go.temporal.io/server/chasm/lib/streamnotifier/gen/streamnotifierpb/v1"
	"go.temporal.io/server/common"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// BusinessID is the notifier's execution ID for a stream: its owner kind, owner, the first run of
// the owner's run chain, and topic, escaped so that no two streams map to one ID. One notifier
// serves the stream across the chain's runs, and a later chain that reuses the Workflow ID and
// topic gets a notifier of its own.
func BusinessID(ref *streampb.StreamReference) string {
	kind := strings.ToLower(strings.TrimPrefix(ref.GetOwnerKind().String(), "STREAM_OWNER_KIND_"))
	return kind + "/" + url.PathEscape(
		ref.GetWorkflowId(),
	) + "/" + url.PathEscape(
		ref.GetRunId(),
	) + "/" + url.PathEscape(
		ref.GetTopic(),
	)
}

var _ chasm.RootComponent = (*StreamNotifier)(nil)
var _ callback.CompletionSource = (*StreamNotifier)(nil)
var _ callback.StreamWakeSource = (*StreamNotifier)(nil)

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
func (n *StreamNotifier) Terminate(
	ctx chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	if !n.Closed {
		if err := n.close(ctx, nil, streamFailure("the stream's notifier was terminated")); err != nil {
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
	ctx.AddTask(
		n,
		chasm.TaskAttributes{ScheduledTime: now.AsTime().Add(after)},
		&streamnotifierpb.ExpiryTask{
			LastActivityTime: now,
		},
	)
}

type attachInput struct {
	requestID string
	// Exactly one of callback and workflowTarget is set.
	callback           *commonpb.Callback_Nexus
	workflowTarget     *streampb.WorkflowWakeTarget
	recheckInitial     time.Duration
	operationToken     string
	startTime          *timestamppb.Timestamp
	maxCallbacks       int
	maxCallbackBytes   int
	idleTimeout        time.Duration
	ownerCheckInterval time.Duration
}

// attach adds a callback, or changes nothing when one with the request ID is attached. On a
// closed stream the callback completes right away. A full notifier makes room only by dropping
// callbacks that are done.
func (n *StreamNotifier) attach(ctx chasm.MutableContext, in attachInput) error {
	if n.Expired {
		return serviceerror.NewFailedPrecondition(
			"the stream closed and its notifier no longer takes callbacks",
		)
	}
	if _, ok := n.Callbacks[in.requestID]; ok {
		return nil
	}
	n.dropClosedCallers(ctx)
	cb := callback.NewCallback(in.requestID, timestamppb.New(ctx.Now(n)), callbackOf(in))
	var start *streamnotifierpb.CallerStart
	if in.operationToken != "" || in.startTime != nil {
		start = &streamnotifierpb.CallerStart{
			OperationToken: in.operationToken,
			StartTime:      in.startTime,
		}
	}
	size := callbackBytes(cb, start)
	if !n.Closed && n.Counter > 0 {
		// The progress delivered below is kept on the callback.
		size += proto.Size(n.progress())
	}
	used := n.heldBytes(ctx)
	for len(n.Callbacks) >= in.maxCallbacks || used+size > in.maxCallbackBytes {
		freed, ok := n.evictOne(ctx)
		if !ok && len(n.Callbacks) >= in.maxCallbacks {
			return serviceerror.NewFailedPreconditionf(
				"the stream notifier holds the maximum of %d callbacks",
				in.maxCallbacks,
			)
		}
		if !ok {
			return serviceerror.NewFailedPreconditionf(
				"the stream notifier's callbacks would pass the maximum of %d bytes",
				in.maxCallbackBytes,
			)
		}
		used -= freed
	}
	if n.Callbacks == nil {
		n.Callbacks = make(chasm.Map[string, *callback.Callback])
	}
	n.Callbacks[in.requestID] = chasm.NewComponentField(ctx, cb)
	if start != nil {
		if n.CallerStarts == nil {
			n.CallerStarts = map[string]*streamnotifierpb.CallerStart{}
		}
		n.CallerStarts[in.requestID] = start
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
	if n.RecheckTime == nil {
		n.armRecheck(ctx, in.recheckInitial)
	}
	n.touch(ctx, in.idleTimeout)
	return nil
}

// callbackOf builds the callback an attach asked for.
func callbackOf(in attachInput) *callbackspb.Callback {
	if target := in.workflowTarget; target != nil {
		return &callbackspb.Callback{
			Variant: &callbackspb.Callback_StreamWake_{
				StreamWake: &callbackspb.Callback_StreamWake{
					WorkflowId:     target.GetWorkflowId(),
					RunId:          target.GetRunId(),
					SubscriptionId: target.GetSubscriptionId(),
				},
			},
		}
	}
	return &callbackspb.Callback{
		Variant: &callbackspb.Callback_Nexus_{
			Nexus: &callbackspb.Callback_Nexus{
				Url:    in.callback.GetUrl(),
				Header: maps.Clone(in.callback.GetHeader()),
			},
		},
	}
}

// waitingWorkflowTargets answers the Workflow readers that still take wakes.
func (n *StreamNotifier) waitingWorkflowTargets(ctx chasm.Context) []*callback.Callback {
	var waiting []*callback.Callback
	for _, field := range n.Callbacks {
		cb := field.Get(ctx)
		if cb.GetCallback().GetStreamWake() != nil &&
			cb.Status == callbackspb.CALLBACK_STATUS_STANDBY &&
			!cb.GetProgressDisabled() && !cb.GetReceiverClosed() {
			waiting = append(waiting, cb)
		}
	}
	return waiting
}

// armRecheck arms a recheck gap from now while Workflow readers wait, and disarms it otherwise.
func (n *StreamNotifier) armRecheck(ctx chasm.MutableContext, gap time.Duration) {
	if n.Closed || gap <= 0 || len(n.waitingWorkflowTargets(ctx)) == 0 {
		n.RecheckTime = nil
		n.RecheckGap = nil
		return
	}
	at := timestamppb.New(ctx.Now(n).Add(gap))
	n.RecheckTime = at
	n.RecheckGap = durationpb.New(gap)
	ctx.AddTask(
		n,
		chasm.TaskAttributes{ScheduledTime: at.AsTime()},
		&streamnotifierpb.RecheckTask{ScheduledTime: at},
	)
}

// recheck asks every waiting Workflow reader to read again, since no notification came for the
// gap, and arms the next recheck after twice the gap, up to maxGap.
func (n *StreamNotifier) recheck(ctx chasm.MutableContext, maxGap time.Duration) error {
	progress := n.progress()
	progress.Recheck = true
	for _, cb := range n.waitingWorkflowTargets(ctx) {
		if err := cb.DeliverProgress(ctx, progress); err != nil {
			return err
		}
	}
	n.armRecheck(ctx, min(2*n.GetRecheckGap().AsDuration(), maxGap))
	return nil
}

// dropClosedCallers forgets the callbacks whose caller's operation closed: nothing will read their
// progress or their completion.
func (n *StreamNotifier) dropClosedCallers(ctx chasm.MutableContext) {
	for requestID, field := range n.Callbacks {
		if field.Get(ctx).GetReceiverClosed() {
			n.detach(requestID)
		}
	}
}

// evictOne drops the oldest callback that is done and returns its size, or false if no callback
// is done. A callback whose caller still waits for its completion is never dropped, even one that
// takes no progress, such as an HSM caller's; callbacks whose caller closed are already gone
// (dropClosedCallers).
func (n *StreamNotifier) evictOne(ctx chasm.MutableContext) (int, bool) {
	var done *callback.Callback
	for _, field := range n.Callbacks {
		cb := field.Get(ctx)
		if cb.Status != callbackspb.CALLBACK_STATUS_SUCCEEDED &&
			cb.Status != callbackspb.CALLBACK_STATUS_FAILED {
			continue
		}
		if done == nil ||
			cb.GetRegistrationTime().AsTime().Before(done.GetRegistrationTime().AsTime()) {
			done = cb
		}
	}
	if done == nil {
		return 0, false
	}
	freed := callbackBytes(done, n.CallerStarts[done.GetRequestId()])
	n.detach(done.GetRequestId())
	return freed, true
}

// heldBytes is the size of the callbacks the notifier holds, as callbackBytes counts each.
func (n *StreamNotifier) heldBytes(ctx chasm.Context) int {
	size := 0
	for requestID, field := range n.Callbacks {
		size += callbackBytes(field.Get(ctx), n.CallerStarts[requestID])
	}
	return size
}

// callbackBytes is the persisted size of a callback and of the start its caller saw, which is what
// the notifier's state holds for it.
func callbackBytes(cb *callback.Callback, start *streamnotifierpb.CallerStart) int {
	return proto.Size(cb.CallbackState) + proto.Size(start)
}

// hasWaitingCallbacks reports whether a callback still waits for the stream's close.
func (n *StreamNotifier) hasWaitingCallbacks(ctx chasm.Context) bool {
	for _, field := range n.Callbacks {
		cb := field.Get(ctx)
		if cb.Status == callbackspb.CALLBACK_STATUS_STANDBY && !cb.GetReceiverClosed() {
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
	ctx.AddTask(
		n,
		chasm.TaskAttributes{ScheduledTime: at.AsTime()},
		&streamnotifierpb.OwnerCheckTask{ScheduledTime: at},
	)
}

// failIdleCallbacks fails the callbacks that wait for the close, keeping the stream open, and
// reports whether there were any.
func (n *StreamNotifier) failIdleCallbacks(ctx chasm.MutableContext) (bool, error) {
	failed := false
	for requestID, field := range n.Callbacks {
		cb := field.Get(ctx)
		if cb.Status != callbackspb.CALLBACK_STATUS_STANDBY || cb.GetReceiverClosed() {
			continue
		}
		n.IdleFailedRequestIds = append(n.IdleFailedRequestIds, requestID)
		err := callback.TransitionScheduled.Apply(cb, ctx, callback.EventScheduled{})
		if err != nil {
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
	n.IdleFailedRequestIds = slices.DeleteFunc(
		n.IdleFailedRequestIds,
		func(id string) bool { return id == requestID },
	)
	n.CanceledRequestIds = slices.DeleteFunc(
		n.CanceledRequestIds,
		func(id string) bool { return id == requestID },
	)
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
	// Set instead of closeResult when the stream closes with a failure.
	closeFailure *failurepb.Failure
	idleTimeout  time.Duration
	// The idle timeout when no callback waits, such as for a notifier only clients poll.
	idleTimeoutNoCallbacks time.Duration
	retention              time.Duration
	recheckInitial         time.Duration
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
		// A notification shows the producer reaches the notifier, so the quiet period starts over.
		if n.RecheckTime != nil {
			n.armRecheck(ctx, in.recheckInitial)
		}
	}
	if in.close {
		if err := n.close(ctx, in.closeResult, in.closeFailure); err != nil {
			return err
		}
		n.touch(ctx, in.retention)
		return nil
	}
	if n.hasWaitingCallbacks(ctx) {
		n.touch(ctx, in.idleTimeout)
	} else {
		n.touch(ctx, in.idleTimeoutNoCallbacks)
	}
	return nil
}

// startIdle arms the expiry of a notifier that a poll started, and changes nothing on one that
// already saw activity.
func (n *StreamNotifier) startIdle(ctx chasm.MutableContext, idleTimeout time.Duration) {
	if n.LastActivityTime == nil {
		n.touch(ctx, idleTimeout)
	}
}

// pollResponse is what a poll answers with: the latest notification and the close outcome.
func (n *StreamNotifier) pollResponse(
	_ chasm.Context,
	_ struct{},
) (*workflowservice.PollStreamNotifierResponse, error) {
	return &workflowservice.PollStreamNotifierResponse{
		Counter:      n.Counter,
		Position:     n.Position,
		Metadata:     maps.Clone(n.Metadata),
		Closed:       n.Closed,
		CloseResult:  common.CloneProto(n.CloseResult),
		CloseFailure: common.CloneProto(n.CloseFailure),
	}, nil
}

// pollDone reports whether a poll that waits for a counter above after returns: the counter
// passed it, the stream closed, or the notifier ended, so the client starts a new one.
func (n *StreamNotifier) pollDone(after int64) bool {
	return n.Counter > after || n.Closed || n.Expired
}

// close completes every callback still waiting: with the result, or with failure when it is set.
func (n *StreamNotifier) close(
	ctx chasm.MutableContext,
	result *commonpb.Payload,
	failure *failurepb.Failure,
) error {
	n.Closed = true
	n.CloseFailure = failure
	n.CloseResult = result
	n.CloseTime = timestamppb.New(ctx.Now(n))
	n.OwnerCheckTime = nil
	n.RecheckTime = nil
	n.RecheckGap = nil
	return callback.ScheduleStandbyCallbacks(ctx, n.Callbacks)
}

func (n *StreamNotifier) progress() *callbackspb.Progress {
	return &callbackspb.Progress{
		Position: n.Position,
		Counter:  n.Counter,
		Metadata: maps.Clone(n.Metadata),
	}
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
func (n *StreamNotifier) GetNexusCompletion(
	ctx chasm.Context,
	requestID string,
) (nexusrpc.CompleteOperationOptions, error) {
	completion := n.callerStart(requestID)
	completion.CloseTime = n.GetCloseTime().AsTime()
	if _, canceled := slices.BinarySearch(n.CanceledRequestIds, requestID); canceled {
		completion.CloseTime = ctx.Now(n)
		completion.Error = &nexus.OperationError{
			State: nexus.OperationStateCanceled,
			Cause: &nexus.FailureError{
				Failure: nexus.Failure{Message: "the caller canceled the operation"},
			},
		}
		return completion, nil
	}
	failure := n.GetCloseFailure()
	if _, idle := slices.BinarySearch(n.IdleFailedRequestIds, requestID); idle {
		failure = streamFailure("the stream was idle for too long, " +
			"so its notifier failed the callers waiting on it")
		completion.CloseTime = ctx.Now(n)
	}
	if failure != nil {
		opErr, err := operationError(failure)
		if err != nil {
			return nexusrpc.CompleteOperationOptions{}, err
		}
		completion.Error = opErr
		return completion, nil
	}
	completion.Result = nilPayload()
	if n.CloseResult != nil {
		completion.Result = common.CloneProto(n.CloseResult)
	}
	return completion, nil
}

// GetStreamWakeCompletion is the wake that completes a Workflow reader's callback: none if the
// reader detached, a resubscribe if an idle timeout failed it, otherwise the stream's close.
func (n *StreamNotifier) GetStreamWakeCompletion(
	_ chasm.Context,
	requestID string,
) (*streampb.StreamWake, error) {
	if _, canceled := slices.BinarySearch(n.CanceledRequestIds, requestID); canceled {
		return nil, nil
	}
	wake := &streampb.StreamWake{
		Position: n.Position,
		Counter:  n.Counter,
		Metadata: maps.Clone(n.Metadata),
	}
	if _, idle := slices.BinarySearch(n.IdleFailedRequestIds, requestID); idle {
		wake.Resubscribe = true
		return wake, nil
	}
	wake.Closed = true
	wake.CloseResult = common.CloneProto(n.CloseResult)
	wake.CloseFailure = common.CloneProto(n.CloseFailure)
	return wake, nil
}

func (n *StreamNotifier) describeCallbacks(
	ctx chasm.Context,
) ([]*streampb.StreamCallbackInfo, error) {
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
		var apiCallback *commonpb.Callback
		var workflowTarget *streampb.WorkflowWakeTarget
		if target := cb.GetCallback().GetStreamWake(); target != nil {
			workflowTarget = &streampb.WorkflowWakeTarget{
				WorkflowId:     target.GetWorkflowId(),
				RunId:          target.GetRunId(),
				SubscriptionId: target.GetSubscriptionId(),
			}
		} else {
			var err error
			if apiCallback, err = cb.ToAPICallback(); err != nil {
				return nil, err
			}
		}
		// The header values hold the caller's callback token. A reader of this namespace could
		// forge a completion with one, also for a caller in another namespace. The keys stay, so a
		// reader still sees which headers the callback carries.
		if nexusCallback := apiCallback.GetNexus(); nexusCallback != nil {
			for key := range nexusCallback.Header {
				nexusCallback.Header[key] = ""
			}
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
			WorkflowTarget:          workflowTarget,
		})
	}
	return infos, nil
}

// streamFailure is a failure of the notifier's own, such as an idle timeout. A retry gets the same
// answer, so it is non-retryable.
func streamFailure(message string) *failurepb.Failure {
	return &failurepb.Failure{
		Message: message,
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: true},
		},
	}
}

// operationError completes a callback with failure the way a Workflow-run operation completes with
// its run's failure: canceled for a canceled failure, failed otherwise, with failure as the cause.
func operationError(failure *failurepb.Failure) (*nexus.OperationError, error) {
	cause, err := commonnexus.TemporalFailureToNexusFailure(failure)
	if err != nil {
		return nil, err
	}
	state, message := nexus.OperationStateFailed, "operation failed"
	if failure.GetCanceledFailureInfo() != nil {
		state, message = nexus.OperationStateCanceled, "operation canceled"
	}
	opErr := &nexus.OperationError{
		Message: message,
		State:   state,
		Cause:   &nexus.FailureError{Failure: cause},
	}
	if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
		return nil, err
	}
	return opErr, nil
}

// nilPayload is the result of a close that carries none. A typed caller decodes a result by its
// encoding, so an empty payload without one would fail the caller's Workflow Task on every try.
func nilPayload() *commonpb.Payload {
	return &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("binary/null")}}
}
