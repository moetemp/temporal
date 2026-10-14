package streamnotifier

import (
	"context"
	"errors"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	streamnotifierpb "go.temporal.io/server/chasm/lib/streamnotifier/gen/streamnotifierpb/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/resource"
	"go.uber.org/fx"
)

// ownerEndedFailure is the failure message the callers get when the owner Workflow is gone, or its
// ID is in another chain, without having closed the stream.
const ownerEndedFailure = "the owner Workflow ended without closing the stream"

// ownerFailedType is the failure type the callers get when the owner Workflow failed, since the
// owner check reads no History and so has no failure of its own to hand them.
const ownerFailedType = "StreamOwnerFailed"

type expiryTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config
}

type expiryTaskHandlerOptions struct {
	fx.In

	Config *Config
}

func newExpiryTaskHandler(opts expiryTaskHandlerOptions) *expiryTaskHandler {
	return &expiryTaskHandler{config: opts.Config}
}

// Validate keeps only the expiry armed by the latest activity.
func (h *expiryTaskHandler) Validate(
	_ chasm.Context,
	n *StreamNotifier,
	_ chasm.TaskInvocation,
	task *streamnotifierpb.ExpiryTask,
) (bool, error) {
	return !n.Expired &&
		n.GetLastActivityTime().AsTime().Equal(task.GetLastActivityTime().AsTime()), nil
}

// Execute fails the callers of an idle open stream and keeps the stream open, or ends a notifier
// that is idle with nothing to wait on, or ends a closed notifier once its closed retention passed.
func (h *expiryTaskHandler) Execute(
	ctx chasm.MutableContext,
	n *StreamNotifier,
	_ chasm.TaskAttributes,
	_ *streamnotifierpb.ExpiryTask,
) error {
	if n.Closed {
		n.Expired = true
		return nil
	}
	failed, err := n.failIdleCallbacks(ctx)
	if err != nil {
		return err
	}
	if !failed {
		n.Expired = true
		return nil
	}
	// The failed callbacks still need their completions delivered, so the notifier stays.
	n.touch(ctx, h.config.IdleTimeout(ctx.NamespaceEntry().Name().String()))
	return nil
}

type ownerCheckTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*streamnotifierpb.OwnerCheckTask]
	config            *Config
	historyClient     resource.HistoryClient
	namespaceRegistry namespace.Registry
}

type ownerCheckTaskHandlerOptions struct {
	fx.In

	Config            *Config
	HistoryClient     resource.HistoryClient
	NamespaceRegistry namespace.Registry
}

func newOwnerCheckTaskHandler(opts ownerCheckTaskHandlerOptions) *ownerCheckTaskHandler {
	return &ownerCheckTaskHandler{
		config:            opts.Config,
		historyClient:     opts.HistoryClient,
		namespaceRegistry: opts.NamespaceRegistry,
	}
}

// Validate keeps only the latest armed check of an open notifier.
func (h *ownerCheckTaskHandler) Validate(
	_ chasm.Context,
	n *StreamNotifier,
	_ chasm.TaskInvocation,
	task *streamnotifierpb.OwnerCheckTask,
) (bool, error) {
	return !n.Closed && !n.Expired &&
		n.GetOwnerCheckTime().AsTime().Equal(task.GetScheduledTime().AsTime()), nil
}

// Execute describes the owner Workflow. If its run chain ended, the stream closes with a failure;
// otherwise the next check is armed while callbacks still wait.
func (h *ownerCheckTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *streamnotifierpb.OwnerCheckTask,
) error {
	stream, err := chasm.ReadComponent(
		ctx,
		ref,
		func(
			n *StreamNotifier,
			_ chasm.Context,
			_ struct{},
		) (*streamnotifierpb.NotifierState, error) {
			return n.NotifierState, nil
		},
		struct{}{},
	)
	if err != nil {
		return err
	}
	ns, err := h.namespaceRegistry.GetNamespaceByID(namespace.ID(ref.NamespaceID))
	if err != nil {
		return err
	}
	end, err := h.ownerEnded(ctx, ns, stream.GetStreamRef())
	if err != nil {
		return err
	}
	_, _, err = chasm.UpdateComponent(
		ctx,
		ref,
		func(n *StreamNotifier, mctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			if n.Closed || !n.GetOwnerCheckTime().AsTime().Equal(task.GetScheduledTime().AsTime()) {
				return struct{}{}, nil
			}
			n.OwnerCheckTime = nil
			// The Worker that ran the final task closes the stream with the Workflow's own result,
			// and the first close wins, so a just-closed run is left to it for the grace.
			waiting := end.workerCloses && end.closeTime != nil &&
				mctx.Now(n).Before(end.closeTime.Add(h.config.OwnerCloseGrace(ns.Name().String())))
			// A reader may subscribe before its owner starts, so a missing owner gets a grace from
			// the notifier's start.
			startGrace := h.config.OwnerStartGrace(ns.Name().String())
			if end.notFound && n.StartTime != nil &&
				mctx.Now(n).Before(n.StartTime.AsTime().Add(startGrace)) {
				waiting = true
			}
			if end.ended && !waiting {
				if err := n.close(mctx, nil, end.failure); err != nil {
					return struct{}{}, err
				}
				n.touch(mctx, h.config.ClosedRetention(ns.Name().String()))
				return struct{}{}, nil
			}
			n.armOwnerCheck(mctx, h.config.OwnerCheckInterval(ns.Name().String()))
			return struct{}{}, nil
		},
		struct{}{},
	)
	return err
}

// ownerEnded reports whether the owner Workflow's run chain is closed, and the failure its stream
// closes with, as a Workflow-run operation completes for the same close status: none for a
// completed owner, so its callers get an empty result. A Workflow that does not exist (or whose
// retention passed) counts as ended without closing the stream, and so does a current run of
// another chain that reused the Workflow ID.
func (h *ownerCheckTaskHandler) ownerEnded(
	ctx context.Context,
	ns *namespace.Namespace,
	stream *streampb.StreamReference,
) (ownerEnd, error) {
	workflowID := stream.GetWorkflowId()
	resp, err := h.historyClient.DescribeWorkflowExecution(
		ctx,
		&historyservice.DescribeWorkflowExecutionRequest{
			NamespaceId: ns.ID().String(),
			Request: &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: ns.Name().String(),
				Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
			},
		},
	)
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return ownerEnd{ended: true, failure: streamFailure(ownerEndedFailure), notFound: true}, nil
	}
	if err != nil {
		return ownerEnd{}, err
	}
	// The described run is the chain's current one, so a retry, a cron run or Continue-as-New has
	// already moved past a closed earlier run.
	if firstRunID := stream.GetRunId(); firstRunID != "" &&
		resp.GetWorkflowExecutionInfo().GetFirstRunId() != firstRunID {
		return ownerEnd{ended: true, failure: streamFailure(ownerEndedFailure)}, nil
	}
	info := resp.GetWorkflowExecutionInfo()
	var closeTime *time.Time
	if info.GetCloseTime() != nil {
		t := info.GetCloseTime().AsTime()
		closeTime = &t
	}
	// A completed, failed or canceled run ended through a final Workflow Task, whose Worker
	// closes the stream itself.
	byWorker := func(failure *failurepb.Failure) ownerEnd {
		return ownerEnd{ended: true, failure: failure, workerCloses: true, closeTime: closeTime}
	}
	switch info.GetStatus() {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED:
		return ownerEnd{}, nil
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return byWorker(nil), nil
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		// The check reads no History, so it has the status but not the run's failure.
		failure := streamFailure("the owner Workflow failed")
		failure.GetApplicationFailureInfo().Type = ownerFailedType
		return byWorker(failure), nil
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return byWorker(&failurepb.Failure{
			Message: "operation canceled",
			FailureInfo: &failurepb.Failure_CanceledFailureInfo{
				CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
			},
		}), nil
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return ownerEnd{ended: true, failure: &failurepb.Failure{
			Message: "operation terminated",
			FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
				TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{},
			},
		}}, nil
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return ownerEnd{ended: true, failure: &failurepb.Failure{
			Message: "operation exceeded internal timeout",
			FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
				TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{},
			},
		}}, nil
	default:
		return ownerEnd{ended: true, failure: streamFailure(ownerEndedFailure)}, nil
	}
}

// ownerEnd is what the owner check learned about the owner's run chain.
type ownerEnd struct {
	ended bool
	// The failure the stream closes with, or nil for an empty result.
	failure *failurepb.Failure
	// Set when the run ended through a final Workflow Task, whose Worker closes the stream.
	workerCloses bool
	// When the run closed, if History said.
	closeTime *time.Time
	// Set when no run of the Workflow ID exists, so the owner may not have started yet.
	notFound bool
}

type recheckTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config
}

type recheckTaskHandlerOptions struct {
	fx.In

	Config *Config
}

func newRecheckTaskHandler(opts recheckTaskHandlerOptions) *recheckTaskHandler {
	return &recheckTaskHandler{config: opts.Config}
}

// Validate keeps only the latest armed recheck of an open notifier.
func (h *recheckTaskHandler) Validate(
	_ chasm.Context,
	n *StreamNotifier,
	_ chasm.TaskInvocation,
	task *streamnotifierpb.RecheckTask,
) (bool, error) {
	return !n.Closed && !n.Expired &&
		n.GetRecheckTime().AsTime().Equal(task.GetScheduledTime().AsTime()), nil
}

// Execute asks the waiting Workflow readers to read again, since no notification came for the
// quiet period. A producer that appended and stopped before it notified leaves the record in the
// store, and the read finds it.
func (h *recheckTaskHandler) Execute(
	ctx chasm.MutableContext,
	n *StreamNotifier,
	_ chasm.TaskAttributes,
	_ *streamnotifierpb.RecheckTask,
) error {
	return n.recheck(ctx, h.config.RecheckMax(ctx.NamespaceEntry().Name().String()))
}
