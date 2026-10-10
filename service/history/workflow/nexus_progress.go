package workflow

import (
	"context"

	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/log/tag"
)

// chasmWorkflowView resolves the CHASM Workflow component through a read-only context, or answers
// false for a Workflow without one. It is asked on every transaction close, so it never takes the
// component for writing.
func (ms *MutableStateImpl) chasmWorkflowView() (*chasmworkflow.Workflow, chasm.Context, bool) {
	node, ok := ms.chasmTree.(*chasm.Node)
	if !ok {
		return nil, nil, false
	}
	chasmCtx := chasm.NewContext(context.Background(), node)
	rootComponent, err := node.ComponentByPath(chasmCtx, nil)
	if err != nil {
		return nil, nil, false
	}
	wf, ok := rootComponent.(*chasmworkflow.Workflow)
	return wf, chasmCtx, ok
}

// hasPendingNexusProgress reports whether a Nexus operation holds progress for the next Workflow
// Task scheduled event. Progress writes no event, so nothing else schedules the task that carries
// it.
func (ms *MutableStateImpl) hasPendingNexusProgress() bool {
	wf, chasmCtx, ok := ms.chasmWorkflowView()
	return ok && wf.HasPendingNexusProgress(chasmCtx)
}

// attachNexusProgress puts the pending Nexus operation progress on a WorkflowTaskScheduled event
// for a task no worker has seen yet. The event is the acknowledgment, so the progress is taken.
func (ms *MutableStateImpl) attachNexusProgress(event *historypb.HistoryEvent) {
	if ms.executionInfo.GetWorkflowTaskHoldsNexusProgress() || !ms.hasPendingNexusProgress() {
		return
	}
	wf, chasmCtx, err := ms.ChasmWorkflowComponent(context.Background())
	if err != nil {
		ms.logger.Warn("cannot attach Nexus operation progress", tag.Error(err))
		return
	}
	attrs := event.GetWorkflowTaskScheduledEventAttributes()
	attrs.NexusOperationProgress = append(attrs.NexusOperationProgress, wf.TakeNexusProgress(chasmCtx)...)
}

// clearScheduledNexusProgress forgets which operations the scheduled Workflow Task carries, once
// that task starts, fails or times out, so newer progress schedules a task of its own. Checked
// through a read-only view first, since almost no Workflow has any.
func (ms *MutableStateImpl) clearScheduledNexusProgress() {
	wf, chasmCtx, ok := ms.chasmWorkflowView()
	if !ok || !wf.HasScheduledNexusProgress(chasmCtx) {
		return
	}
	wf, mutableCtx, err := ms.ChasmWorkflowComponent(context.Background())
	if err != nil {
		ms.logger.Warn("cannot clear scheduled Nexus operation progress", tag.Error(err))
		return
	}
	wf.ClearScheduledNexusProgress(mutableCtx)
}

// scheduleWorkflowTaskForNexusProgress schedules a Workflow Task to carry pending Nexus operation
// progress when no task is pending to carry it.
func (ms *MutableStateImpl) scheduleWorkflowTaskForNexusProgress() error {
	if ms.HasPendingWorkflowTask() || ms.IsWorkflowExecutionStatusPaused() || !ms.hasPendingNexusProgress() {
		return nil
	}
	if _, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL); err != nil {
		return err
	}
	// A task that was scheduled for progress but did not carry it would leave the progress
	// pending, and every later transaction would schedule another task for it.
	if ms.hasPendingNexusProgress() {
		ms.logger.Warn("dropped Nexus operation progress that a scheduled Workflow Task did not carry")
		wf, chasmCtx, err := ms.ChasmWorkflowComponent(context.Background())
		if err != nil {
			return err
		}
		wf.DropPendingNexusProgress(chasmCtx)
	}
	return nil
}
