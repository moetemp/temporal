package callback

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	queuescommon "go.temporal.io/server/service/history/queues/common"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"
)

// ProgressSource is implemented by a parent whose progress deliveries carry the operation's token,
// start time and links, the way its completion does. A parent without it sends progress without
// them; a receiver only needs them for a completion that arrives before the start.
type ProgressSource interface {
	GetNexusProgressOptions(ctx chasm.Context, requestID string) (nexusrpc.CompleteOperationOptions, error)
}

// DeliverProgress hands the source's progress to the callback while the source runs. Progress is
// dropped once the completion is scheduled, since the completion supersedes it and never waits for
// it, after the receiver refused progress, and when the counter is not above one already seen. A
// delivery in flight takes the newest pending progress when it finishes, so a burst folds into it.
func (c *Callback) DeliverProgress(ctx chasm.MutableContext, progress *nexuspb.NexusOperationProgress) error {
	if c.Status != callbackspb.CALLBACK_STATUS_STANDBY || c.ProgressDisabled {
		return nil
	}
	// A worker-hosted handler would need OnProgress, which is not delivered yet.
	if c.GetCallback().GetNexusHandler() != nil {
		return nil
	}
	seen := max(c.GetPendingProgress().GetCounter(), c.DeliveredProgressCounter, c.ProgressInFlight)
	if progress.GetCounter() <= seen {
		return nil
	}
	c.PendingProgress = progress
	if c.ProgressInFlight == 0 {
		return c.startProgressDelivery(ctx)
	}
	return nil
}

func (c *Callback) startProgressDelivery(ctx chasm.MutableContext) error {
	destination, err := callbackDestination(c.GetCallback())
	if err != nil {
		return err
	}
	c.ProgressInFlight = c.GetPendingProgress().GetCounter()
	c.ProgressAttempt = 0
	ctx.AddTask(c, chasm.TaskAttributes{Destination: destination}, &callbackspb.ProgressTask{
		Counter: c.ProgressInFlight,
	})
	return nil
}

// stopProgress forgets any progress waiting or in flight. Called when the completion is scheduled:
// the completion never waits for a progress delivery, and a result that arrives afterwards is
// ignored.
func (c *Callback) stopProgress() {
	c.PendingProgress = nil
	c.ProgressInFlight = 0
	c.ProgressAttempt = 0
}

// progressInvocation is what a progress task needs to make one delivery.
type progressInvocation struct {
	callback *callbackspb.Callback_Nexus
	progress *nexuspb.NexusOperationProgress
	options  nexusrpc.CompleteOperationOptions
}

//nolint:revive // context.Context is an input parameter for chasm.ReadComponent, not a function parameter
func (c *Callback) loadProgressArgs(ctx chasm.Context, _ chasm.NoValue) (progressInvocation, error) {
	invocation := progressInvocation{
		callback: c.GetCallback().GetNexus(),
		progress: common.CloneProto(c.GetPendingProgress()),
	}
	if source, ok := c.CompletionSource.Get(ctx).(ProgressSource); ok {
		options, err := source.GetNexusProgressOptions(ctx, c.RequestId)
		if err != nil {
			return progressInvocation{}, err
		}
		invocation.options = options
	}
	return invocation, nil
}

// progressResult is how a progress delivery ended.
type progressResult int

const (
	progressDelivered progressResult = iota
	// The receiver refused progress, which turns progress off for this callback.
	progressRefused
	// The delivery may succeed on a later attempt.
	progressRetry
)

type saveProgressInput struct {
	// inFlight names the delivery, as the task that made it saw it.
	inFlight int64
	// sent is the counter the delivery carried, the newest pending progress when it was made.
	sent        int64
	result      progressResult
	err         error
	retryPolicy func(attempt int32, err error) time.Duration
}

func (c *Callback) saveProgressResult(ctx chasm.MutableContext, input saveProgressInput) (chasm.NoValue, error) {
	// The completion took over, or this result is for a delivery that is no longer in flight.
	if c.Status != callbackspb.CALLBACK_STATUS_STANDBY || c.ProgressInFlight != input.inFlight {
		return nil, nil
	}
	switch input.result {
	case progressDelivered:
		c.DeliveredProgressCounter = max(c.DeliveredProgressCounter, input.sent)
		if c.GetPendingProgress().GetCounter() > c.DeliveredProgressCounter {
			return nil, c.startProgressDelivery(ctx)
		}
		c.stopProgress()
	case progressRefused:
		c.ProgressDisabled = true
		c.stopProgress()
	case progressRetry:
		c.ProgressAttempt++
		ctx.AddTask(c, chasm.TaskAttributes{
			ScheduledTime: ctx.Now(c).Add(input.retryPolicy(c.ProgressAttempt, input.err)),
		}, &callbackspb.ProgressBackoffTask{Counter: c.ProgressInFlight, Attempt: c.ProgressAttempt})
	default:
		return nil, fmt.Errorf("unknown progress delivery result %d", input.result)
	}
	return nil, nil
}

// progressBody is the OperationProgress object of the Nexus HTTP spec.
func progressBody(progress *nexuspb.NexusOperationProgress) ([]byte, error) {
	body := struct {
		Position string            `json:"position,omitempty"`
		Counter  int64             `json:"counter"`
		Metadata map[string]string `json:"metadata,omitempty"`
	}{
		Position: progress.GetPosition(),
		Counter:  progress.GetCounter(),
		Metadata: progress.GetMetadata(),
	}
	return json.Marshal(body)
}

// progressRefusedByHandler reports whether an outbound delivery got a 4xx other than one that asks
// to try again later, which tells the handler to stop sending progress.
func progressRefusedByHandler(err error) bool {
	handlerErr, ok := errors.AsType[*nexus.HandlerError](err)
	if !ok {
		return false
	}
	switch handlerErr.Type { // nolint:exhaustive
	case nexus.HandlerErrorTypeBadRequest,
		nexus.HandlerErrorTypeUnauthenticated,
		nexus.HandlerErrorTypeUnauthorized,
		nexus.HandlerErrorTypeNotFound,
		nexus.HandlerErrorTypeConflict:
		return true
	default:
		return false
	}
}

type progressTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*callbackspb.ProgressTask]
	invocation *invocationTaskHandler
}

func newProgressTaskHandler(opts invocationTaskHandlerOptions) *progressTaskHandler {
	return &progressTaskHandler{invocation: newInvocationTaskHandler(opts)}
}

func (h *progressTaskHandler) Validate(_ chasm.Context, cb *Callback, _ chasm.TaskInvocation, task *callbackspb.ProgressTask) (bool, error) {
	return cb.Status == callbackspb.CALLBACK_STATUS_STANDBY &&
		!cb.ProgressDisabled &&
		cb.ProgressInFlight == task.Counter &&
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
	callCtx, cancel := context.WithTimeout(ctx, h.invocation.config.RequestTimeout(ns.Name().String(), taskAttr.Destination))
	defer cancel()

	var result progressResult
	if invocation.callback.GetUrl() == chasm.NexusCompletionHandlerURL {
		result, err = h.deliverInternal(callCtx, invocation)
	} else {
		result, err = h.deliverOutbound(callCtx, ns, taskAttr, invocation)
	}
	if err != nil {
		h.invocation.logger.Warn("Nexus progress delivery failed",
			tag.WorkflowNamespace(ns.Name().String()),
			tag.Destination(taskAttr.Destination),
			tag.Bool("refused", result == progressRefused),
			tag.Error(err),
		)
	}
	retryPolicy := h.invocation.config.RetryPolicy()
	_, _, err = chasm.UpdateComponent(ctx, ref, (*Callback).saveProgressResult, saveProgressInput{
		inFlight: task.Counter,
		sent:     invocation.progress.GetCounter(),
		result:   result,
		err:      err,
		retryPolicy: func(attempt int32, err error) time.Duration {
			return retryPolicy.ComputeNextDelay(0, int(attempt), err)
		},
	})
	return err
}

// deliverOutbound posts the progress to the callback URL with the running state. A system URL is
// routed to the caller's frontend by the HTTP caller, the same way a completion is.
func (h *progressTaskHandler) deliverOutbound(
	ctx context.Context,
	ns *namespace.Namespace,
	taskAttr chasm.TaskAttributes,
	invocation progressInvocation,
) (progressResult, error) {
	body, err := progressBody(invocation.progress)
	if err != nil {
		return progressRefused, err
	}
	client := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{
		HTTPCaller: h.invocation.httpCallerProvider(queuescommon.NamespaceIDAndDestination{
			NamespaceID: ns.ID().String(),
			Destination: taskAttr.Destination,
		}),
		Serializer: commonnexus.PayloadSerializer,
	})
	options := invocation.options
	options.Header = invocation.callback.GetHeader()
	options.Result = &nexus.Content{Header: nexus.Header{"type": "application/json"}, Data: body}
	options.Progress = true
	options.Error = nil
	options.CloseTime = time.Time{}
	err = client.CompleteOperation(ctx, invocation.callback.GetUrl(), options)
	switch {
	case err == nil:
		return progressDelivered, nil
	case progressRefusedByHandler(err):
		return progressRefused, err
	default:
		return progressRetry, err
	}
}

// deliverInternal hands the progress to the caller's operation in History, for a caller on this
// cluster reached through the internal completion handler.
func (h *progressTaskHandler) deliverInternal(ctx context.Context, invocation progressInvocation) (progressResult, error) {
	encodedToken := nexus.Header(invocation.callback.GetHeader()).Get(commonnexus.CallbackTokenHeader)
	if encodedToken == "" {
		return progressRefused, errors.New("callback has no token")
	}
	ref, requestID, err := chasm.UnpackNexusCallbackToken(encodedToken)
	if err != nil {
		return progressRefused, err
	}
	_, err = h.invocation.historyClient.CompleteNexusOperationChasm(ctx, &historyservice.CompleteNexusOperationChasmRequest{
		Completion: &tokenspb.NexusOperationCompletion{ComponentRef: ref, RequestId: requestID},
		Outcome: &historyservice.CompleteNexusOperationChasmRequest_Progress{
			Progress: proto.CloneOf(invocation.progress),
		},
	})
	switch {
	case err == nil:
		return progressDelivered, nil
	case common.IsRetryableRPCError(err):
		return progressRetry, err
	default:
		// Includes NotFound: the caller's operation closed, so nothing will read progress for it.
		return progressRefused, err
	}
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

func (h *progressBackoffTaskHandler) Validate(_ chasm.Context, cb *Callback, _ chasm.TaskInvocation, task *callbackspb.ProgressBackoffTask) (bool, error) {
	return cb.Status == callbackspb.CALLBACK_STATUS_STANDBY &&
		!cb.ProgressDisabled &&
		cb.ProgressInFlight == task.Counter &&
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
		Counter: task.Counter,
		Attempt: task.Attempt,
	})
	return nil
}
