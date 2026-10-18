package workflow

import (
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"google.golang.org/protobuf/types/known/anypb"
)

// nexusProgressCarrier carries the progress of the Workflow's Nexus operations, keyed by
// operation, in the attachments of the Workflow Task scheduled event.
type nexusProgressCarrier struct{}

func (nexusProgressCarrier) hasPending(wf *chasmworkflow.Workflow, ctx chasm.Context) bool {
	return wf.HasPendingNexusProgress(ctx)
}

func (nexusProgressCarrier) take(
	wf *chasmworkflow.Workflow,
	ctx chasm.MutableContext,
	attrs *historypb.WorkflowTaskScheduledEventAttributes,
) error {
	for _, progress := range wf.TakeNexusProgress(ctx) {
		attachment, err := anypb.New(progress)
		if err != nil {
			return err
		}
		attrs.Attachments = append(attrs.Attachments, attachment)
	}
	return nil
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
