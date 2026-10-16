package workflow

import (
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
)

// nexusProgressCarrier carries the progress of the Workflow's Nexus operations, keyed by
// operation, on the Workflow Task scheduled event.
type nexusProgressCarrier struct{}

func (nexusProgressCarrier) hasPending(wf *chasmworkflow.Workflow, ctx chasm.Context) bool {
	return wf.HasPendingNexusProgress(ctx)
}

func (nexusProgressCarrier) hasScheduled(wf *chasmworkflow.Workflow, ctx chasm.Context) bool {
	return wf.HasScheduledNexusProgress(ctx)
}

func (nexusProgressCarrier) take(
	wf *chasmworkflow.Workflow,
	ctx chasm.MutableContext,
	attrs *historypb.WorkflowTaskScheduledEventAttributes,
) {
	attrs.NexusOperationProgress = append(attrs.NexusOperationProgress, wf.TakeNexusProgress(ctx)...)
}

func (nexusProgressCarrier) clearScheduled(wf *chasmworkflow.Workflow, ctx chasm.MutableContext) {
	wf.ClearScheduledNexusProgress(ctx)
}

func (nexusProgressCarrier) readyAt(
	wf *chasmworkflow.Workflow,
	ctx chasm.Context,
	minInterval time.Duration,
) time.Time {
	return wf.NexusProgressReadyAt(ctx, minInterval)
}

func (nexusProgressCarrier) hold(
	wf *chasmworkflow.Workflow,
	ctx chasm.MutableContext,
	readyAt time.Time,
) {
	wf.HoldNexusProgress(ctx, readyAt)
}

func (nexusProgressCarrier) minInterval(ms *MutableStateImpl) time.Duration {
	if ms.config.NexusOperationProgressMinInterval == nil {
		return 0
	}
	return ms.config.NexusOperationProgressMinInterval(ms.GetNamespaceEntry().Name().String())
}
