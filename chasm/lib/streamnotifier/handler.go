package streamnotifier

import (
	"context"
	"errors"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	streamnotifierpb "go.temporal.io/server/chasm/lib/streamnotifier/gen/streamnotifierpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/resource"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type handler struct {
	streamnotifierpb.UnimplementedStreamNotifierServiceServer

	config        *Config
	logger        log.Logger
	historyClient resource.HistoryClient
	owners        *ownerDescriber
}

// releaseTimeout bounds freeing a reader's place after an attach, which runs apart from the
// attach's own deadline, since a failed attach has often used it up.
const releaseTimeout = 5 * time.Second

func newHandler(
	config *Config,
	logger log.Logger,
	historyClient resource.HistoryClient,
	owners *ownerDescriber,
) *handler {
	return &handler{config: config, logger: logger, historyClient: historyClient, owners: owners}
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
			n.StartTime = timestamppb.New(mctx.Now(n))
			_, err := update(n, mctx, in)
			return n, err
		},
		update,
		input,
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly,
			chasm.BusinessIDConflictPolicyUseExisting,
		),
	)
	if _, ok := errors.AsType[*chasm.ExecutionAlreadyStartedError](err); ok {
		return serviceerror.NewFailedPrecondition(
			"the stream closed and its notifier no longer takes requests",
		)
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
	if err := h.owners.checkFirstRun(ctx, req.GetNamespaceId(), ns, fe.GetStreamRef()); err != nil {
		return nil, err
	}
	target := fe.GetWorkflowTarget()
	sourceRequest := func(mode enumsspb.StreamWakeMode) *historyservice.RecordStreamWakeRequest {
		return &historyservice.RecordStreamWakeRequest{
			NamespaceId: req.GetNamespaceId(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: target.GetWorkflowId(),
				RunId:      target.GetRunId(),
			},
			Wake: &streampb.StreamWake{SubscriptionId: target.GetSubscriptionId()},
			Mode: mode,
		}
	}
	reserved := false
	if target != nil {
		// The reader counts toward its run's readers now, so an attach past the limit fails, and a
		// run that already closed refuses it.
		resp, err := h.historyClient.RecordStreamWake(
			ctx,
			sourceRequest(enumsspb.STREAM_WAKE_MODE_RESERVE),
		)
		if err != nil {
			return nil, err
		}
		reserved = resp.GetReserved()
	}
	// created and refused tell what the update did. Its error alone can't, since a timeout may
	// come after the attach landed. A new notifier runs the update twice, and the second run finds
	// the callback the first one made, so created stays set.
	created, refused := false, false
	err := startOrUpdate(
		ctx,
		req.GetNamespaceId(),
		req.GetBusinessId(),
		fe.GetStreamRef(),
		func(
			n *StreamNotifier,
			mctx chasm.MutableContext,
			r *workflowservice.AttachStreamCallbackRequest,
		) (struct{}, error) {
			_, attached := n.Callbacks[r.GetRequestId()]
			err := n.attach(mctx, attachInput{
				requestID:          r.GetRequestId(),
				callback:           r.GetCallback(),
				workflowTarget:     r.GetWorkflowTarget(),
				recheckInitial:     h.config.RecheckInitial(ns),
				operationToken:     r.GetOperationToken(),
				idle:               r.GetIdle(),
				startTime:          r.GetStartTime(),
				maxCallbacks:       h.config.MaxCallbacks(ns),
				maxCallbackBytes:   h.config.MaxCallbackBytes(ns),
				maxCloseBytes:      h.config.BlobSizeLimitError(ns),
				idleTimeout:        h.config.IdleTimeout(ns),
				ownerCheckInterval: h.config.OwnerCheckInterval(ns),
			})
			created = created || err == nil && !attached
			refused = err != nil
			return struct{}{}, err
		},
		fe,
	)
	if _, ok := errors.AsType[*serviceerror.FailedPrecondition](err); ok {
		// The stream's notifier ended, so nothing was attached.
		refused = true
	}
	// The place this attach took is free when no new callback holds it: the attach was refused,
	// or it found the callback of an earlier attach. A reader that counted before this attach
	// keeps its place. An attach that failed in another way may have landed, so it keeps the
	// place for the caller's retry.
	if reserved && (refused || err == nil && !created) {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
		_, releaseErr := h.historyClient.RecordStreamWake(
			releaseCtx,
			sourceRequest(enumsspb.STREAM_WAKE_MODE_RELEASE),
		)
		cancel()
		if releaseErr != nil {
			h.logger.Warn("Failed to free a stream reader's place after an attach",
				tag.Error(releaseErr))
		}
	}
	if err != nil {
		return nil, err
	}
	return &streamnotifierpb.AttachStreamCallbackResponse{
		FrontendResponse: &workflowservice.AttachStreamCallbackResponse{},
	}, nil
}

func (h *handler) NotifyStream(
	ctx context.Context,
	req *streamnotifierpb.NotifyStreamRequest,
) (_ *streamnotifierpb.NotifyStreamResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	fe := req.GetFrontendRequest()
	ns := fe.GetNamespace()
	in := notifyInput{
		position:     fe.GetPosition(),
		counter:      fe.GetCounter(),
		metadata:     fe.GetMetadata(),
		close:        fe.GetClose(),
		closeResult:  fe.GetCloseResult(),
		closeFailure: fe.GetCloseFailure(),
		idleTimeout:  h.config.IdleTimeout(ns),
		retention:    h.config.ClosedRetention(ns),

		idleTimeoutNoCallbacks: h.config.IdleTimeoutNoCallbacks(ns),
		recheckInitial:         h.config.RecheckInitial(ns),
		debounceWindow:         h.config.DebounceWindow(ns),
	}
	var err error
	if fe.GetClose() {
		// A close starts a notifier when the stream has none, so a callback that attaches later
		// gets the stream's own close outcome.
		err = startOrUpdate(
			ctx,
			req.GetNamespaceId(),
			req.GetBusinessId(),
			fe.GetStreamRef(),
			func(n *StreamNotifier, mctx chasm.MutableContext, in notifyInput) (struct{}, error) {
				return struct{}{}, n.notify(mctx, in)
			},
			in,
		)
	} else {
		err = h.notifyExisting(ctx, req.GetNamespaceId(), req.GetBusinessId(), in)
	}
	if err != nil {
		return nil, err
	}
	return &streamnotifierpb.NotifyStreamResponse{
		FrontendResponse: &workflowservice.NotifyStreamResponse{},
	}, nil
}

// notifyExisting applies a notification that does not close the stream. It starts no notifier,
// since nobody waits on a stream that has none, and a reader reads the stream again after it
// subscribes. It writes nothing when the notifier ended or already holds the counter, so a repeat
// from an at-least-once driver costs only a read.
func (h *handler) notifyExisting(
	ctx context.Context,
	namespaceID, businessID string,
	in notifyInput,
) error {
	ref := chasm.NewComponentRef[*StreamNotifier](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  businessID,
	})
	changes, err := chasm.ReadComponent(
		ctx,
		ref,
		func(n *StreamNotifier, _ chasm.Context, counter int64) (bool, error) {
			return n.takesNotification(counter), nil
		},
		in.counter,
	)
	if err != nil || !changes {
		return ignoreNotFound(err)
	}
	_, _, err = chasm.UpdateComponent(
		ctx,
		ref,
		func(n *StreamNotifier, mctx chasm.MutableContext, in notifyInput) (struct{}, error) {
			// The notifier may have ended since the read.
			if !n.takesNotification(in.counter) {
				return struct{}{}, nil
			}
			return struct{}{}, n.notify(mctx, in)
		},
		in,
	)
	return ignoreNotFound(err)
}

// ignoreNotFound treats a stream with no notifier as one where nothing waits.
func ignoreNotFound(err error) error {
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return nil
	}
	return err
}

func (h *handler) WaitStreamCallback(
	ctx context.Context,
	req *streamnotifierpb.WaitStreamCallbackRequest,
) (_ *streamnotifierpb.WaitStreamCallbackResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	fe := req.GetFrontendRequest()
	idleTimeout := h.config.IdleTimeout(fe.GetNamespace())
	ref := chasm.NewComponentRef[*StreamNotifier](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetBusinessId(),
	})
	// A wait starts no notifier, since it names a callback that an attach put there.
	dropped, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(
			n *StreamNotifier,
			mctx chasm.MutableContext,
			r *workflowservice.WaitStreamCallbackRequest,
		) (bool, error) {
			return n.wait(mctx, r.GetRequestId(), r.GetAfterCounter(), idleTimeout)
		},
		fe,
	)
	if err != nil {
		return nil, err
	}
	if dropped {
		return nil, serviceerror.NewNotFoundf(
			"the callback with request ID %q no longer reaches its reader",
			fe.GetRequestId(),
		)
	}
	return &streamnotifierpb.WaitStreamCallbackResponse{
		FrontendResponse: &workflowservice.WaitStreamCallbackResponse{},
	}, nil
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
	return &streamnotifierpb.DetachStreamCallbackResponse{
		FrontendResponse: &workflowservice.DetachStreamCallbackResponse{},
	}, nil
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
	resp, err := chasm.ReadComponent(
		ctx,
		ref,
		func(
			n *StreamNotifier,
			cctx chasm.Context,
			_ struct{},
		) (*workflowservice.DescribeStreamNotifierResponse, error) {
			callbacks, err := n.describeCallbacks(cctx)
			if err != nil {
				return nil, err
			}
			return &workflowservice.DescribeStreamNotifierResponse{
				Counter:      n.GetCounter(),
				Position:     n.GetPosition(),
				Closed:       n.GetClosed(),
				Callbacks:    callbacks,
				CloseFailure: common.CloneProto(n.GetCloseFailure()),
			}, nil
		},
		struct{}{},
	)
	if err != nil {
		return nil, err
	}
	return &streamnotifierpb.DescribeStreamNotifierResponse{FrontendResponse: resp}, nil
}

// PollStreamNotifier waits until the notifier's counter passes the request's after_counter, the
// stream closes, or the wait limit passes. A poll writes nothing, except that it starts a notifier
// when the stream has none, so the notification the client waits for has somewhere to land. A
// notification that came before that start was dropped, so the client reads once more.
func (h *handler) PollStreamNotifier(
	ctx context.Context,
	req *streamnotifierpb.PollStreamNotifierRequest,
) (_ *streamnotifierpb.PollStreamNotifierResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	fe := req.GetFrontendRequest()
	ns := fe.GetNamespace()
	after := fe.GetAfterCounter()
	ref := chasm.NewComponentRef[*StreamNotifier](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetBusinessId(),
	})
	type snapshot struct {
		response *workflowservice.PollStreamNotifierResponse
		done     bool
		ended    bool
	}
	current, err := chasm.ReadComponent(
		ctx,
		ref,
		func(n *StreamNotifier, cctx chasm.Context, _ struct{}) (snapshot, error) {
			resp, err := n.pollResponse(cctx, struct{}{})
			return snapshot{
				response: resp,
				done:     n.pollDone(after),
				ended:    n.Expired && !n.Closed,
			}, err
		},
		struct{}{},
	)
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		current, err = snapshot{ended: true}, nil
	}
	if err != nil {
		return nil, err
	}
	if current.ended {
		// No notifier, or one that ended idle: start a new one and wait on it.
		idle := h.config.IdleTimeoutNoCallbacks(ns)
		if err := startOrUpdate(
			ctx,
			req.GetNamespaceId(),
			req.GetBusinessId(),
			fe.GetStreamRef(),
			func(n *StreamNotifier, mctx chasm.MutableContext, _ struct{}) (struct{}, error) {
				n.startIdle(mctx, idle)
				return struct{}{}, nil
			},
			struct{}{},
		); err != nil {
			return nil, err
		}
		current = snapshot{response: &workflowservice.PollStreamNotifierResponse{}}
	} else if current.done {
		return &streamnotifierpb.PollStreamNotifierResponse{
			FrontendResponse: current.response,
		}, nil
	}

	pollCtx, cancel := contextutil.WithDeadlineBuffer(
		ctx,
		h.config.PollMaxWait(ns),
		common.DefaultLongPollBuffer,
	)
	defer cancel()
	resp, _, err := chasm.PollComponent(
		pollCtx,
		ref,
		func(
			n *StreamNotifier,
			cctx chasm.Context,
			after int64,
		) (*workflowservice.PollStreamNotifierResponse, bool, error) {
			if !n.pollDone(after) {
				return nil, false, nil
			}
			resp, err := n.pollResponse(cctx, struct{}{})
			return resp, true, err
		},
		after,
	)
	if err != nil && pollCtx.Err() != nil && ctx.Err() == nil {
		// The wait limit passed. The client reads the stream again.
		err, resp = nil, nil
	}
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = current.response
	}
	return &streamnotifierpb.PollStreamNotifierResponse{FrontendResponse: resp}, nil
}
