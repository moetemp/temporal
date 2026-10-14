package callback

import (
	"context"
	"errors"
	"maps"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
)

// streamWakeDestination groups the deliveries of every stream wake callback. They all go to
// History, so none of them trips a circuit breaker meant for an outside endpoint.
const streamWakeDestination = "stream-wake://history"

// StreamWakeSource is implemented by a parent whose callbacks may wake a stream reader in a
// Workflow.
type StreamWakeSource interface {
	// GetStreamWakeCompletion returns the wake that completes a callback, or nil when the reader
	// needs none, for example after the reader detached.
	GetStreamWakeCompletion(ctx chasm.Context, requestID string) (*streampb.StreamWake, error)
}

func streamWakeRequest(
	namespaceID string,
	target *callbackspb.Callback_StreamWake,
	wake *streampb.StreamWake,
) *historyservice.RecordStreamWakeRequest {
	wake = common.CloneProto(wake)
	wake.SubscriptionId = target.GetSubscriptionId()
	return &historyservice.RecordStreamWakeRequest{
		NamespaceId: namespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: target.GetWorkflowId(),
			RunId:      target.GetRunId(),
		},
		Wake: wake,
	}
}

// deliverStreamWakeProgress hands progress to the reader's Workflow as a stream wake. A closed
// run answers NotFound, which marks the callback so the notifier drops it.
func deliverStreamWakeProgress(
	ctx context.Context,
	h *progressTaskHandler,
	ref chasm.ComponentRef,
	invocation progressInvocation,
) (progressResult, error) {
	progress := invocation.progress
	_, err := h.invocation.historyClient.RecordStreamWake(ctx, streamWakeRequest(
		ref.NamespaceID,
		invocation.callback.GetStreamWake(),
		&streampb.StreamWake{
			Position: progress.GetPosition(),
			Counter:  progress.GetCounter(),
			Metadata: maps.Clone(progress.GetMetadata()),
			Recheck:  progress.GetRecheck(),
		},
	))
	if err == nil {
		return progressDelivered, nil
	}
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return progressReceiverClosed, err
	}
	// A refusal turns progress off for good, so only an answer a retry won't change is one, such
	// as the limit of readers per run.
	if _, ok := errors.AsType[*serviceerror.InvalidArgument](err); ok {
		return progressRefused, err
	}
	if _, ok := errors.AsType[*serviceerror.FailedPrecondition](err); ok {
		return progressRefused, err
	}
	return progressRetry, err
}

// invocableStreamWake delivers the wake that completes a stream wake callback, such as the
// stream's close, to the reader's Workflow. A reader that needs no wake has its place freed.
type invocableStreamWake struct {
	namespaceID string
	target      *callbackspb.Callback_StreamWake
	// wake is nil when the reader needs no completion.
	wake *streampb.StreamWake
}

// WrapError keeps a failed delivery from counting against a destination, since the callback's
// own backoff retries it.
func (c invocableStreamWake) WrapError(_ invocationResult, err error) error {
	return err
}

func (c invocableStreamWake) Invoke(
	ctx context.Context,
	_ *namespace.Namespace,
	h *invocationTaskHandler,
	_ *callbackspb.InvocationTask,
	_ chasm.TaskAttributes,
) invocationResult {
	wake := c.wake
	if wake == nil {
		wake = &streampb.StreamWake{}
	}
	req := streamWakeRequest(c.namespaceID, c.target, wake)
	// The reader detached, so it frees its place among the run's readers for a new one.
	req.Release = c.wake == nil
	_, err := h.historyClient.RecordStreamWake(ctx, req)
	if err == nil {
		return invocationResultOK{}
	}
	// A closed run reads nothing more, so its reader needs no completion.
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return invocationResultOK{}
	}
	if common.IsRetryableRPCError(err) {
		return invocationResultRetry{err: err}
	}
	return invocationResultFail{err: err}
}
