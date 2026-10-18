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
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	streamnotifierpb "go.temporal.io/server/chasm/lib/streamnotifier/gen/streamnotifierpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// BusinessID is the notifier's execution ID for a stream: its owner kind, owner, the run that keys
// the owner, and topic, escaped so that no two streams map to one ID. For a Workflow the run is the
// first run of its chain, so one notifier serves the stream across the chain's runs, and a later
// chain that reuses the Workflow ID and topic gets a notifier of its own. For an Activity it is the
// Activity run. For a Workflow Activity the owner is the Workflow ID and the Activity ID, and the
// run is the run that scheduled the Activity.
func BusinessID(ref *streampb.StreamReference) string {
	kind := strings.ToLower(strings.TrimPrefix(ref.GetOwnerKind().String(), "STREAM_OWNER_KIND_"))
	ownerID := url.PathEscape(ref.GetWorkflowId())
	switch ref.GetOwnerKind() {
	case enumspb.STREAM_OWNER_KIND_ACTIVITY:
		ownerID = url.PathEscape(ref.GetActivityId())
	case enumspb.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY:
		kind = "workflow_activity"
		ownerID += "/" + url.PathEscape(ref.GetActivityId())
	default:
	}
	return kind + "/" + ownerID + "/" + url.PathEscape(
		ref.GetRunId(),
	) + "/" + url.PathEscape(
		ref.GetTopic(),
	)
}

var _ chasm.RootComponent = (*StreamNotifier)(nil)
var _ callback.CompletionSource = (*StreamNotifier)(nil)
var _ callback.StreamWakeSource = (*StreamNotifier)(nil)

// StreamNotifier holds the callbacks attached to one stream. It hands each notification, folded,
// as progress to the callbacks whose variant takes progress, and completes all of them when the
// stream closes.
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
				ActivityId: ref.GetActivityId(),
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

// touch records activity and moves the expiry to after from now. It arms a task only when none is
// armed or the expiry moved earlier, so a busy stream does not write a task per notification.
func (n *StreamNotifier) touch(ctx chasm.MutableContext, after time.Duration) {
	now := ctx.Now(n)
	n.LastActivityTime = timestamppb.New(now)
	n.ExpiryTime = timestamppb.New(now.Add(after))
	if n.ExpiryTaskTime == nil || n.ExpiryTime.AsTime().Before(n.ExpiryTaskTime.AsTime()) {
		n.armExpiry(ctx)
	}
}

// armExpiry arms the expiry task for the current expiry time.
func (n *StreamNotifier) armExpiry(ctx chasm.MutableContext) {
	at := timestamppb.New(n.GetExpiryTime().AsTime())
	n.ExpiryTaskTime = at
	ctx.AddTask(
		n,
		chasm.TaskAttributes{ScheduledTime: at.AsTime()},
		&streamnotifierpb.ExpiryTask{ScheduledTime: at},
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
	maxCloseBytes      int
	idleTimeout        time.Duration
	ownerCheckInterval time.Duration
	// Set for a callback that gets progress only when it waits.
	idle bool
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
	// The close outcome comes after the attach, so an open stream counts the largest one.
	used := n.heldBytes(ctx) + in.maxCloseBytes
	if n.Closed {
		used += proto.Size(n.CloseResult) + proto.Size(n.CloseFailure) - in.maxCloseBytes
	}
	for len(n.Callbacks) >= in.maxCallbacks || used+size > in.maxCallbackBytes {
		freed, ok := n.evictOne(ctx)
		// A full notifier frees room when callbacks complete, so the caller may try again later.
		if !ok && len(n.Callbacks) >= in.maxCallbacks {
			return serviceerror.NewResourceExhaustedf(
				enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
				"the stream notifier holds the maximum of %d callbacks",
				in.maxCallbacks,
			)
		}
		if !ok {
			return serviceerror.NewResourceExhaustedf(
				enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
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
	if in.idle {
		n.IdleRequestIds = append(n.IdleRequestIds, in.requestID)
		slices.Sort(n.IdleRequestIds)
	} else if n.Counter > 0 {
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

// armRecheck moves the recheck to gap from now while Workflow readers wait, and disarms it
// otherwise. It adds a task only when none is armed or the recheck moved earlier, so a busy stream
// does not write a task per notification.
func (n *StreamNotifier) armRecheck(ctx chasm.MutableContext, gap time.Duration) {
	if n.Closed || gap <= 0 || len(n.waitingWorkflowTargets(ctx)) == 0 {
		n.disarmRecheck()
		return
	}
	n.RecheckTime = timestamppb.New(ctx.Now(n).Add(gap))
	n.RecheckGap = durationpb.New(gap)
	if n.RecheckTaskTime == nil || n.RecheckTime.AsTime().Before(n.RecheckTaskTime.AsTime()) {
		n.armRecheckTask(ctx)
	}
}

// armRecheckTask arms the recheck task for the current recheck time.
func (n *StreamNotifier) armRecheckTask(ctx chasm.MutableContext) {
	at := timestamppb.New(n.GetRecheckTime().AsTime())
	n.RecheckTaskTime = at
	ctx.AddTask(
		n,
		chasm.TaskAttributes{ScheduledTime: at.AsTime()},
		&streamnotifierpb.RecheckTask{ScheduledTime: at},
	)
}

// disarmRecheck clears the recheck. An armed task finds no match and does nothing.
func (n *StreamNotifier) disarmRecheck() {
	n.RecheckTime = nil
	n.RecheckGap = nil
	n.RecheckTaskTime = nil
}

// recheck asks every waiting Workflow reader to read again, since no notification came for the
// gap, and arms the next recheck after twice the gap, up to maxGap. It wakes idle readers too and
// uses up their waits, so a wait that never reached the notifier delays a reader by at most the
// gap. The reader asks for its next wake after it reads.
func (n *StreamNotifier) recheck(ctx chasm.MutableContext, maxGap time.Duration) error {
	progress := n.progress()
	progress.Recheck = true
	for _, cb := range n.waitingWorkflowTargets(ctx) {
		delete(n.Waits, cb.GetRequestId())
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

// progressReserve is the most a callback's pending progress takes, with room for the counter and
// the field tags.
const progressReserve = maxPositionBytes + maxMetadataBytes + 64

// requestIDCopies is how many more times the state can hold a callback's request ID, besides the
// copy inside the callback: as the key of Callbacks and of CallerStarts, and in
// IdleFailedRequestIds and CanceledRequestIds. A request ID can be as long as an ID.
const requestIDCopies = 4

// callbackBytes is the persisted size of a callback and of the start its caller saw, which is what
// the notifier's state holds for it. Pending progress counts as the largest one, since a later
// notification can give every callback that much while their deliveries retry.
func callbackBytes(cb *callback.Callback, start *streamnotifierpb.CallerStart) int {
	return proto.Size(cb.CallbackState) - proto.Size(cb.GetPendingProgress()) + progressReserve +
		proto.Size(start) + requestIDCopies*len(cb.GetRequestId())
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
	delete(n.Waits, requestID)
	n.IdleRequestIds = slices.DeleteFunc(
		n.IdleRequestIds,
		func(id string) bool { return id == requestID },
	)
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
	delete(n.Waits, requestID)
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
	debounceWindow         time.Duration
}

// notify keeps the highest counter and hands it to every callback as progress, at most once per
// debounce window. A close completes every callback and keeps the notifier taking late attaches
// for the closed retention.
func (n *StreamNotifier) notify(ctx chasm.MutableContext, in notifyInput) error {
	if n.Closed {
		return nil
	}
	n.dropClosedCallers(ctx)
	if in.counter > n.Counter {
		n.Counter = in.counter
		n.Position = in.position
		n.Metadata = maps.Clone(in.metadata)
		switch {
		case in.close:
			// The close completes every callback right after, so the progress goes with it.
			if err := n.deliverProgress(ctx); err != nil {
				return err
			}
		case n.inDebounceWindow(ctx):
			n.DebounceHeld = true
			ctx.MetricsHandler().Counter(NotificationHeld.Name()).Record(1)
		default:
			if err := n.deliverProgress(ctx); err != nil {
				return err
			}
			n.openDebounceWindow(ctx, in.debounceWindow)
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

// deliverProgress hands the latest notification to every callback that takes it: a callback
// that is not idle, or an idle one whose wait the counter passed. That wait is then used up.
func (n *StreamNotifier) deliverProgress(ctx chasm.MutableContext) error {
	progress := n.progress()
	for requestID, field := range n.Callbacks {
		if n.isIdle(requestID) {
			after, waiting := n.Waits[requestID]
			if !waiting || n.Counter <= after {
				continue
			}
			delete(n.Waits, requestID)
		}
		if err := field.Get(ctx).DeliverProgress(ctx, progress); err != nil {
			return err
		}
	}
	return nil
}

// isIdle reports whether the callback was attached idle.
func (n *StreamNotifier) isIdle(requestID string) bool {
	_, idle := slices.BinarySearch(n.IdleRequestIds, requestID)
	return idle
}

// wait asks for one delivery to an idle callback once the counter passes after. A counter that
// passed already delivers at once, outside any debounce window. A newer wait replaces an older
// one. A callback that is not idle gets every delivery, so its wait changes nothing. It reports
// whether it dropped the callback because a delivery found its receiver closed.
func (n *StreamNotifier) wait(
	ctx chasm.MutableContext,
	requestID string,
	after int64,
	idleTimeout time.Duration,
) (bool, error) {
	field, ok := n.Callbacks[requestID]
	if !ok {
		return false, serviceerror.NewNotFoundf(
			"no callback is attached with request ID %q",
			requestID,
		)
	}
	// The reader's run closed or lost the reader's place, so no wake reaches it. The caller
	// answers NotFound, and a reader in a running run attaches again.
	if field.Get(ctx).GetReceiverClosed() {
		n.detach(requestID)
		return true, nil
	}
	// A closed stream completes the callback, which is the last thing it gets. A callback that
	// completes for another reason, such as a detach, takes no more progress.
	if n.Closed || !n.isIdle(requestID) ||
		field.Get(ctx).Status != callbackspb.CALLBACK_STATUS_STANDBY {
		return false, nil
	}
	n.touch(ctx, idleTimeout)
	if n.Counter > after {
		cb := field.Get(ctx)
		if cb.GetDeliveredProgressCounter() >= n.Counter {
			// The reader read below a counter it was woken for, so the store lost records or a
			// writer sent a counter past the store's head. Rechecks alone wake this reader then.
			ctx.MetricsHandler().Counter(WaitBelowDelivered.Name()).Record(1)
			ctx.Logger().Warn("A stream reader waits below a counter it already got",
				tag.NewStringTag("request-id", requestID),
				tag.NewInt64("after-counter", after),
				tag.NewInt64("counter", n.Counter))
		}
		delete(n.Waits, requestID)
		return false, cb.DeliverProgress(ctx, n.progress())
	}
	if n.Waits == nil {
		n.Waits = map[string]int64{}
	}
	n.Waits[requestID] = after
	return false, nil
}

// inDebounceWindow reports whether a delivery opened a window that has not ended.
func (n *StreamNotifier) inDebounceWindow(ctx chasm.Context) bool {
	return n.DebounceUntil != nil && ctx.Now(n).Before(n.DebounceUntil.AsTime())
}

// openDebounceWindow starts a window after a delivery, and arms the task that delivers what comes
// inside it. A window of zero turns the debounce off.
func (n *StreamNotifier) openDebounceWindow(ctx chasm.MutableContext, window time.Duration) {
	n.DebounceHeld = false
	if window <= 0 {
		n.DebounceUntil = nil
		return
	}
	until := timestamppb.New(ctx.Now(n).Add(window))
	n.DebounceUntil = until
	ctx.AddTask(
		n,
		chasm.TaskAttributes{ScheduledTime: until.AsTime()},
		&streamnotifierpb.DebounceTask{ScheduledTime: until},
	)
}

// endDebounceWindow delivers the notification held in the window that ended and opens the next
// window, or closes the window when nothing came inside it.
func (n *StreamNotifier) endDebounceWindow(ctx chasm.MutableContext, window time.Duration) error {
	if !n.DebounceHeld {
		n.DebounceUntil = nil
		return nil
	}
	if err := n.deliverProgress(ctx); err != nil {
		return err
	}
	ctx.MetricsHandler().Counter(TrailingDelivery.Name()).Record(1)
	n.openDebounceWindow(ctx, window)
	return nil
}

// takesNotification reports whether a notification that does not close the stream changes the
// notifier: an open notifier that has not ended and holds a lower counter.
func (n *StreamNotifier) takesNotification(counter int64) bool {
	return !n.Closed && !n.Expired && counter > n.Counter
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
	n.disarmRecheck()
	n.DebounceUntil = nil
	n.DebounceHeld = false
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
// reader detached, a resubscribe if an idle timeout failed it, otherwise the stream's close with
// no result.
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
	// One Workflow Task scheduled event carries the wakes of all of a run's readers, so a wake
	// leaves out a close outcome that can be large. The reader's final read of the store returns
	// it. Only a close the owner check makes has no outcome in the store, and that one is small.
	if proto.Size(n.CloseFailure) <= maxWakeCloseFailureBytes {
		wake.CloseFailure = common.CloneProto(n.CloseFailure)
	}
	return wake, nil
}

// maxWakeCloseFailureBytes bounds the close failure a wake carries.
const maxWakeCloseFailureBytes = 4 * 1024

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
