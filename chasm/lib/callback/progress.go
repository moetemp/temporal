package callback

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/fx"
)

// progressInvocation is what a progress task needs to make one delivery.
type progressInvocation struct {
	callback *callbackspb.Callback
	progress *callbackspb.Progress
}

// progressResult is how a progress delivery ended.
type progressResult int

const (
	progressDelivered progressResult = iota
	// The receiver refused progress, which turns progress off for this callback.
	progressRefused
	// The delivery may succeed on a later attempt.
	progressRetry
	// The receiver is closed, so nothing will read this callback any more.
	progressReceiverClosed
)

// progressDelivery makes one progress delivery to the receiver of a callback.
type progressDelivery func(
	ctx context.Context,
	h *progressTaskHandler,
	ref chasm.ComponentRef,
	invocation progressInvocation,
) (progressResult, error)

// variantProgressDelivery returns the progress delivery of a callback's variant, or nil when the
// variant takes only the completion. Each variant that takes progress adds its case here.
func variantProgressDelivery(cb *callbackspb.Callback) progressDelivery {
	switch cb.GetVariant().(type) {
	case *callbackspb.Callback_StreamWake_:
		return deliverStreamWakeProgress
	default:
		return nil
	}
}

// progressDeliveryFor is a variable so tests can give progress to a variant that takes none.
var progressDeliveryFor = variantProgressDelivery

// DeliverProgress hands the source's progress to the callback while the source runs. Progress is
// dropped once the completion is scheduled, since the completion supersedes it and never waits for
// it, after the receiver refused progress, and when the counter is not above one already seen,
// unless it asks for a recheck. A delivery in flight takes the newest pending progress when it
// finishes, so a burst folds into it.
func (c *Callback) DeliverProgress(
	ctx chasm.MutableContext,
	progress *callbackspb.Progress,
) error {
	if c.Status != callbackspb.CALLBACK_STATUS_STANDBY || c.ProgressDisabled {
		return nil
	}
	if progressDeliveryFor(c.GetCallback()) == nil {
		return nil
	}
	seen := max(
		c.GetPendingProgress().GetCounter(),
		c.DeliveredProgressCounter,
		c.ProgressInFlightCounter,
	)
	if progress.GetCounter() <= seen && !progress.GetRecheck() {
		return nil
	}
	c.PendingProgress = foldProgress(c.GetPendingProgress(), progress)
	if c.ProgressInFlight == 0 {
		return c.startProgressDelivery(ctx)
	}
	return nil
}

// foldProgress merges progress into the progress that waits. The higher counter wins with its
// position and metadata, and a recheck stays set.
func foldProgress(waiting, next *callbackspb.Progress) *callbackspb.Progress {
	if waiting == nil {
		return next
	}
	folded := common.CloneProto(waiting)
	if next.GetCounter() >= folded.GetCounter() {
		folded.Counter = next.GetCounter()
		folded.Position = next.GetPosition()
		folded.Metadata = next.GetMetadata()
	}
	folded.Recheck = folded.GetRecheck() || next.GetRecheck()
	return folded
}

func (c *Callback) startProgressDelivery(ctx chasm.MutableContext) error {
	destination, err := callbackDestination(c.GetCallback())
	if err != nil {
		return err
	}
	// Deliveries are numbered, since a recheck may carry the counter of one already delivered.
	c.ProgressDeliveries++
	c.ProgressInFlight = c.ProgressDeliveries
	c.ProgressInFlightCounter = c.GetPendingProgress().GetCounter()
	c.ProgressAttempt = 0
	ctx.AddTask(c, chasm.TaskAttributes{Destination: destination}, &callbackspb.ProgressTask{
		Delivery: c.ProgressInFlight,
	})
	return nil
}

// stopProgress forgets any progress waiting or in flight. Called when the completion is scheduled:
// the completion never waits for a progress delivery, and a result that arrives afterwards is
// ignored.
func (c *Callback) stopProgress() {
	c.PendingProgress = nil
	c.ProgressInFlight = 0
	c.ProgressInFlightCounter = 0
	c.ProgressAttempt = 0
}

// nolint:revive // context.Context is an input parameter for chasm.ReadComponent, not a function
// parameter
func (c *Callback) loadProgressArgs(
	_ chasm.Context,
	_ chasm.NoValue,
) (progressInvocation, error) {
	return progressInvocation{
		callback: common.CloneProto(c.GetCallback()),
		progress: common.CloneProto(c.GetPendingProgress()),
	}, nil
}

type saveProgressInput struct {
	// inFlight names the delivery, as the task that made it saw it.
	inFlight int64
	// sent is the progress the delivery carried, the newest pending progress when it was made.
	sent        *callbackspb.Progress
	result      progressResult
	err         error
	retryPolicy func(attempt int32, err error) time.Duration
}

func (c *Callback) saveProgressResult(
	ctx chasm.MutableContext,
	input saveProgressInput,
) (chasm.NoValue, error) {
	// The completion took over, or this result is for a delivery that is no longer in flight.
	if c.Status != callbackspb.CALLBACK_STATUS_STANDBY || c.ProgressInFlight != input.inFlight {
		return nil, nil
	}
	switch input.result {
	case progressDelivered:
		c.DeliveredProgressCounter = max(c.DeliveredProgressCounter, input.sent.GetCounter())
		pending := c.GetPendingProgress()
		if input.sent.GetRecheck() && pending.GetRecheck() {
			// The recheck on the wire covers one that came while it was there.
			pending = common.CloneProto(pending)
			pending.Recheck = false
			c.PendingProgress = pending
		}
		if pending.GetCounter() > c.DeliveredProgressCounter || pending.GetRecheck() {
			return nil, c.startProgressDelivery(ctx)
		}
		c.stopProgress()
	case progressRefused:
		c.ProgressDisabled = true
		c.stopProgress()
	case progressReceiverClosed:
		c.ProgressDisabled = true
		c.ReceiverClosed = true
		c.stopProgress()
	case progressRetry:
		// No attempt cap. The completion clears progress, and one delivery in flight at the
		// maximum backoff interval keeps a receiver that answers 503 for ever cheap.
		c.ProgressAttempt++
		backoff := input.retryPolicy(c.ProgressAttempt, input.err)
		ctx.AddTask(
			c,
			chasm.TaskAttributes{ScheduledTime: ctx.Now(c).Add(backoff)},
			&callbackspb.ProgressBackoffTask{
				Delivery: c.ProgressInFlight,
				Attempt:  c.ProgressAttempt,
			},
		)
	default:
		return nil, fmt.Errorf("unknown progress delivery result %d", input.result)
	}
	return nil, nil
}

type progressTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*callbackspb.ProgressTask]
	invocation *invocationTaskHandler
}

func newProgressTaskHandler(opts invocationTaskHandlerOptions) *progressTaskHandler {
	return &progressTaskHandler{invocation: newInvocationTaskHandler(opts)}
}

func (h *progressTaskHandler) Validate(
	_ chasm.Context,
	cb *Callback,
	_ chasm.TaskInvocation,
	task *callbackspb.ProgressTask,
) (bool, error) {
	return cb.Status == callbackspb.CALLBACK_STATUS_STANDBY &&
		!cb.ProgressDisabled &&
		cb.ProgressInFlight == task.Delivery &&
		cb.ProgressAttempt == task.Attempt, nil
}

func (h *progressTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	taskAttr chasm.TaskAttributes,
	task *callbackspb.ProgressTask,
) error {
	ns, err := h.invocation.namespaceRegistry.GetNamespaceByID(namespace.ID(ref.NamespaceID))
	if err != nil {
		return fmt.Errorf("failed to get namespace by ID: %w", err)
	}
	invocation, err := chasm.ReadComponent(ctx, ref, (*Callback).loadProgressArgs, nil)
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(
		ctx,
		h.invocation.config.RequestTimeout(ns.Name().String(), taskAttr.Destination),
	)
	defer cancel()

	result := progressRefused
	err = fmt.Errorf("callback variant %T takes no progress", invocation.callback.GetVariant())
	if deliver := progressDeliveryFor(invocation.callback); deliver != nil {
		result, err = deliver(callCtx, h, ref, invocation)
	}
	if err != nil {
		h.invocation.logger.Warn("Callback progress delivery failed",
			tag.WorkflowNamespace(ns.Name().String()),
			tag.Destination(taskAttr.Destination),
			tag.Bool("refused", result == progressRefused),
			tag.Error(err),
		)
	}
	retryPolicy := h.invocation.config.RetryPolicy()
	_, _, err = chasm.UpdateComponent(ctx, ref, (*Callback).saveProgressResult, saveProgressInput{
		inFlight: task.Delivery,
		sent:     invocation.progress,
		result:   result,
		err:      err,
		retryPolicy: func(attempt int32, err error) time.Duration {
			return retryPolicy.ComputeNextDelay(0, int(attempt), err)
		},
	})
	return err
}

type progressBackoffTaskHandler struct {
	chasm.PureTaskHandlerBase
}

type progressBackoffTaskHandlerOptions struct {
	fx.In
}

func newProgressBackoffTaskHandler(progressBackoffTaskHandlerOptions) *progressBackoffTaskHandler {
	return &progressBackoffTaskHandler{}
}

func (h *progressBackoffTaskHandler) Validate(
	_ chasm.Context,
	cb *Callback,
	_ chasm.TaskInvocation,
	task *callbackspb.ProgressBackoffTask,
) (bool, error) {
	return cb.Status == callbackspb.CALLBACK_STATUS_STANDBY &&
		!cb.ProgressDisabled &&
		cb.ProgressInFlight == task.Delivery &&
		cb.ProgressAttempt == task.Attempt, nil
}

// Execute retries the delivery in flight, with whatever progress is newest by then.
func (h *progressBackoffTaskHandler) Execute(
	ctx chasm.MutableContext,
	cb *Callback,
	_ chasm.TaskAttributes,
	task *callbackspb.ProgressBackoffTask,
) error {
	destination, err := callbackDestination(cb.GetCallback())
	if err != nil {
		return err
	}
	ctx.AddTask(cb, chasm.TaskAttributes{Destination: destination}, &callbackspb.ProgressTask{
		Delivery: task.Delivery,
		Attempt:  task.Attempt,
	})
	return nil
}
