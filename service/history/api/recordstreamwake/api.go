package recordstreamwake

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// maxSubscriptionIDBytes bounds a reader's subscription ID, which every wake copies into History.
const maxSubscriptionIDBytes = 256

// Invoke records a wake for one stream reader of a Workflow run. The wake writes no event: the
// Workflow keeps it, and the transaction that closes after this schedules the Workflow Task that
// carries it, unless a task is pending to take it.
func Invoke(
	ctx context.Context,
	req *historyservice.RecordStreamWakeRequest,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (*historyservice.RecordStreamWakeResponse, error) {
	execution := req.GetExecution()
	wake := req.GetWake()
	if execution.GetWorkflowId() == "" || execution.GetRunId() == "" {
		return nil, serviceerror.NewInvalidArgument("execution must name a workflow ID and a run ID")
	}
	if wake.GetSubscriptionId() == "" {
		return nil, serviceerror.NewInvalidArgument("wake.subscription_id is required")
	}
	if len(wake.GetSubscriptionId()) > maxSubscriptionIDBytes {
		return nil, serviceerror.NewInvalidArgument("wake.subscription_id is too long")
	}
	if req.GetReserveOnly() && req.GetRelease() {
		return nil, serviceerror.NewInvalidArgument("reserve_only and release can't both be set")
	}
	namespaceEntry, err := api.GetActiveNamespace(
		shard,
		namespace.ID(req.GetNamespaceId()),
		execution.GetWorkflowId(),
	)
	if err != nil {
		return nil, err
	}
	maxSources := shard.GetConfig().MaximumStreamWakeSources(namespaceEntry.Name().String())
	resp := &historyservice.RecordStreamWakeResponse{}

	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			namespaceEntry.ID().String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			resp.Reserved = false
			mutableState := workflowLease.GetMutableState()
			releaseFn := workflowLease.GetReleaseFn()
			if !mutableState.IsWorkflowExecutionRunning() {
				// A closed run reads no stream, so the callback stops sending it wakes.
				releaseFn(nil)
				return nil, consts.ErrWorkflowCompleted
			}
			if req.GetReserveOnly() || req.GetRelease() {
				view, viewCtx, err := mutableState.ChasmWorkflowComponentReadOnly(ctx)
				if err == nil &&
					view.HasStreamWakeSource(viewCtx, wake.GetSubscriptionId()) == req.GetReserveOnly() {
					return &api.UpdateWorkflowAction{Noop: true, CreateWorkflowTask: false}, nil
				}
			}
			// An error from here on returns without releasing, so the cache drops the mutable
			// state that resolving the CHASM Workflow may have touched.
			wf, chasmCtx, err := mutableState.ChasmWorkflowComponent(ctx)
			if err != nil {
				return nil, serviceerror.NewFailedPrecondition(
					"stream wakes need the CHASM Workflow, which this execution does not have",
				)
			}
			switch {
			case req.GetReserveOnly():
				err = wf.ReserveStreamWakeSource(chasmCtx, wake.GetSubscriptionId(), maxSources)
				resp.Reserved = err == nil
			case req.GetRelease():
				wf.ReleaseStreamWakeSource(chasmCtx, wake.GetSubscriptionId())
			default:
				err = wf.RecordStreamWake(chasmCtx, wake, maxSources)
			}
			if err != nil {
				return nil, err
			}
			return &api.UpdateWorkflowAction{Noop: false, CreateWorkflowTask: false}, nil
		},
		nil,
		shard,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
