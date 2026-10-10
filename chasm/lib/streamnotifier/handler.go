package streamnotifier

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	streamnotifierpb "go.temporal.io/server/chasm/lib/streamnotifier/gen/streamnotifierpb/v1"
	"go.temporal.io/server/common/log"
)

type handler struct {
	streamnotifierpb.UnimplementedStreamNotifierServiceServer

	config *Config
	logger log.Logger
}

func newHandler(config *Config, logger log.Logger) *handler {
	return &handler{config: config, logger: logger}
}

// startOrUpdate applies update to the stream's notifier, creating it if the stream has none. A
// closed stream's notifier is not replaced once its execution completes, so the stream stays
// closed; one that ended idle (failed) is, so the stream goes on.
// A new notifier takes the update in its start function: what an update changes on a root created
// in the same transaction is not persisted. The update is idempotent, so applying it again after
// the start changes nothing.
func startOrUpdate[I any](
	ctx context.Context,
	namespaceID, businessID string,
	ref *streampb.StreamReference,
	update func(*StreamNotifier, chasm.MutableContext, I) (struct{}, error),
	input I,
) error {
	_, err := chasm.UpdateWithStartExecution(
		ctx,
		chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: businessID},
		func(mctx chasm.MutableContext, in I) (*StreamNotifier, error) {
			n := newStreamNotifier(ref)
			_, err := update(n, mctx, in)
			return n, err
		},
		update,
		input,
		chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly, chasm.BusinessIDConflictPolicyUseExisting),
	)
	if _, ok := errors.AsType[*chasm.ExecutionAlreadyStartedError](err); ok {
		return serviceerror.NewFailedPrecondition("the stream closed and its notifier no longer takes requests")
	}
	return err
}

func (h *handler) AttachStreamCallback(
	ctx context.Context,
	req *streamnotifierpb.AttachStreamCallbackRequest,
) (_ *streamnotifierpb.AttachStreamCallbackResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	fe := req.GetFrontendRequest()
	ns := fe.GetNamespace()
	err := startOrUpdate(ctx, req.GetNamespaceId(), req.GetBusinessId(), fe.GetStreamRef(),
		func(n *StreamNotifier, mctx chasm.MutableContext, r *workflowservice.AttachStreamCallbackRequest) (struct{}, error) {
			return struct{}{}, n.attach(mctx, attachInput{
				requestID:          r.GetRequestId(),
				callback:           r.GetCallback(),
				operationToken:     r.GetOperationToken(),
				startTime:          r.GetStartTime(),
				maxCallbacks:       h.config.MaxCallbacks(ns),
				idleTimeout:        h.config.IdleTimeout(ns),
				ownerCheckInterval: h.config.OwnerCheckInterval(ns),
			})
		},
		fe,
	)
	if err != nil {
		return nil, err
	}
	return &streamnotifierpb.AttachStreamCallbackResponse{FrontendResponse: &workflowservice.AttachStreamCallbackResponse{}}, nil
}

func (h *handler) NotifyStream(
	ctx context.Context,
	req *streamnotifierpb.NotifyStreamRequest,
) (_ *streamnotifierpb.NotifyStreamResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	fe := req.GetFrontendRequest()
	ns := fe.GetNamespace()
	err := startOrUpdate(ctx, req.GetNamespaceId(), req.GetBusinessId(), fe.GetStreamRef(),
		func(n *StreamNotifier, mctx chasm.MutableContext, r *workflowservice.NotifyStreamRequest) (struct{}, error) {
			return struct{}{}, n.notify(mctx, notifyInput{
				position:    r.GetPosition(),
				counter:     r.GetCounter(),
				metadata:    r.GetMetadata(),
				close:       r.GetClose(),
				closeResult: r.GetCloseResult(),
				idleTimeout: h.config.IdleTimeout(ns),
				retention:   h.config.ClosedRetention(ns),
			})
		},
		fe,
	)
	if err != nil {
		return nil, err
	}
	return &streamnotifierpb.NotifyStreamResponse{FrontendResponse: &workflowservice.NotifyStreamResponse{}}, nil
}

func (h *handler) DetachStreamCallback(
	ctx context.Context,
	req *streamnotifierpb.DetachStreamCallbackRequest,
) (_ *streamnotifierpb.DetachStreamCallbackResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	ref := chasm.NewComponentRef[*StreamNotifier](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetBusinessId(),
	})
	_, _, err := chasm.UpdateComponent(ctx, ref,
		// The caller canceled its operation and waits for its completion, so the callback is
		// completed as canceled rather than dropped.
		func(n *StreamNotifier, mctx chasm.MutableContext, requestID string) (struct{}, error) {
			return struct{}{}, n.cancel(mctx, requestID)
		},
		req.GetFrontendRequest().GetRequestId(),
	)
	// Detaching from a stream with no notifier changes nothing.
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	return &streamnotifierpb.DetachStreamCallbackResponse{FrontendResponse: &workflowservice.DetachStreamCallbackResponse{}}, nil
}

func (h *handler) DescribeStreamNotifier(
	ctx context.Context,
	req *streamnotifierpb.DescribeStreamNotifierRequest,
) (_ *streamnotifierpb.DescribeStreamNotifierResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	ref := chasm.NewComponentRef[*StreamNotifier](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetBusinessId(),
	})
	resp, err := chasm.ReadComponent(ctx, ref,
		func(n *StreamNotifier, cctx chasm.Context, _ struct{}) (*workflowservice.DescribeStreamNotifierResponse, error) {
			callbacks, err := n.describeCallbacks(cctx)
			if err != nil {
				return nil, err
			}
			return &workflowservice.DescribeStreamNotifierResponse{
				Counter:   n.GetCounter(),
				Position:  n.GetPosition(),
				Closed:    n.GetClosed(),
				Callbacks: callbacks,
			}, nil
		},
		struct{}{},
	)
	if err != nil {
		return nil, err
	}
	return &streamnotifierpb.DescribeStreamNotifierResponse{FrontendResponse: resp}, nil
}
