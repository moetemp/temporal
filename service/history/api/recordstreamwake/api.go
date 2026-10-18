package recordstreamwake

import (
	"context"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
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
	mode := req.GetMode()
	switch mode {
	case enumsspb.STREAM_WAKE_MODE_WAKE,
		enumsspb.STREAM_WAKE_MODE_RESERVE,
		enumsspb.STREAM_WAKE_MODE_RELEASE:
	default:
		return nil, serviceerror.NewInvalidArgumentf("unknown stream wake mode %v", mode)
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
			view, viewCtx, err := mutableState.ChasmWorkflowComponentReadOnly(ctx)
			if err == nil {
				counted := view.HasStreamWakeSource(viewCtx, wake.GetSubscriptionId())
				switch {
				case mode == enumsspb.STREAM_WAKE_MODE_WAKE && !counted:
					// The reader's place is gone, after a close it took, a detach or a rebuild of
					// the run's state. NotFound ends its callback, so the reader attaches again.
					releaseFn(nil)
					return nil, serviceerror.NewNotFound(
						"the stream reader holds no place in this Workflow run",
					)
				case mode == enumsspb.STREAM_WAKE_MODE_RESERVE && counted,
					mode == enumsspb.STREAM_WAKE_MODE_RELEASE && !counted:
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
			switch mode {
			case enumsspb.STREAM_WAKE_MODE_RESERVE:
				err = wf.ReserveStreamWakeSource(chasmCtx, wake.GetSubscriptionId(), maxSources)
				resp.Reserved = err == nil
			case enumsspb.STREAM_WAKE_MODE_RELEASE:
				wf.ReleaseStreamWakeSource(chasmCtx, wake.GetSubscriptionId())
			default:
				wf.RecordStreamWake(chasmCtx, wake)
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
