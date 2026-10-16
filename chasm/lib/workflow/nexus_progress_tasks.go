package workflow

import (
	"go.temporal.io/server/chasm"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
)

type nexusProgressReleaseTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func (nexusProgressReleaseTaskHandler) Validate(
	ctx chasm.Context,
	wf *Workflow,
	_ chasm.TaskInvocation,
	_ *chasmworkflowpb.NexusProgressReleaseTask,
) (bool, error) {
	return wf.nexusProgressHeld(ctx), nil
}

func (nexusProgressReleaseTaskHandler) Execute(
	ctx chasm.MutableContext,
	wf *Workflow,
	_ chasm.TaskAttributes,
	_ *chasmworkflowpb.NexusProgressReleaseTask,
) error {
	wf.releaseNexusProgress(ctx)
	return nil
}
