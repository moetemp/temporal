package streamnotifier

import (
	"context"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
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

// ownerEndedFailure is the failure the callers get when the owner Workflow ended without closing
// the stream.
const ownerEndedFailure = "the owner Workflow ended without closing the stream"

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
func (h *expiryTaskHandler) Validate(_ chasm.Context, n *StreamNotifier, _ chasm.TaskInvocation, task *streamnotifierpb.ExpiryTask) (bool, error) {
	return !n.Expired && n.GetLastActivityTime().AsTime().Equal(task.GetLastActivityTime().AsTime()), nil
}

// Execute fails the callers of an idle open stream and keeps the stream open, or ends a notifier
// that is idle with nothing to wait on, or ends a closed notifier once its closed retention passed.
func (h *expiryTaskHandler) Execute(ctx chasm.MutableContext, n *StreamNotifier, _ chasm.TaskAttributes, _ *streamnotifierpb.ExpiryTask) error {
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
func (h *ownerCheckTaskHandler) Validate(_ chasm.Context, n *StreamNotifier, _ chasm.TaskInvocation, task *streamnotifierpb.OwnerCheckTask) (bool, error) {
	return !n.Closed && !n.Expired && n.GetOwnerCheckTime().AsTime().Equal(task.GetScheduledTime().AsTime()), nil
}

// Execute describes the owner Workflow. If its run chain ended, the stream closes with a failure;
// otherwise the next check is armed while callbacks still wait.
func (h *ownerCheckTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *streamnotifierpb.OwnerCheckTask,
) error {
	stream, err := chasm.ReadComponent(ctx, ref, func(n *StreamNotifier, _ chasm.Context, _ struct{}) (*streamnotifierpb.NotifierState, error) {
		return n.NotifierState, nil
	}, struct{}{})
	if err != nil {
		return err
	}
	ns, err := h.namespaceRegistry.GetNamespaceByID(namespace.ID(ref.NamespaceID))
	if err != nil {
		return err
	}
	ended, err := h.ownerEnded(ctx, ns, stream.GetStreamRef())
	if err != nil {
		return err
	}
	_, _, err = chasm.UpdateComponent(ctx, ref, func(n *StreamNotifier, mctx chasm.MutableContext, _ struct{}) (struct{}, error) {
		if n.Closed || !n.GetOwnerCheckTime().AsTime().Equal(task.GetScheduledTime().AsTime()) {
			return struct{}{}, nil
		}
		n.OwnerCheckTime = nil
		if ended {
			if err := n.close(mctx, nil, ownerEndedFailure); err != nil {
				return struct{}{}, err
			}
			n.touch(mctx, h.config.ClosedRetention(ns.Name().String()))
			return struct{}{}, nil
		}
		n.armOwnerCheck(mctx, h.config.OwnerCheckInterval(ns.Name().String()))
		return struct{}{}, nil
	}, struct{}{})
	return err
}

// ownerEnded reports whether the owner Workflow's run chain is closed. A Workflow that does not
// exist (or whose retention passed) counts as ended, and so does a current run of another chain
// that reused the Workflow ID.
func (h *ownerCheckTaskHandler) ownerEnded(ctx context.Context, ns *namespace.Namespace, stream *streampb.StreamReference) (bool, error) {
	workflowID := stream.GetWorkflowId()
	resp, err := h.historyClient.DescribeWorkflowExecution(ctx, &historyservice.DescribeWorkflowExecutionRequest{
		NamespaceId: ns.ID().String(),
		Request: &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns.Name().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		},
	})
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	// The described run is the chain's current one, so a retry, a cron run or Continue-as-New has
	// already moved past a closed earlier run.
	if firstRunID := stream.GetRunId(); firstRunID != "" && resp.GetWorkflowExecutionInfo().GetFirstRunId() != firstRunID {
		return true, nil
	}
	status := resp.GetWorkflowExecutionInfo().GetStatus()
	return status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && status != enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, nil
}
