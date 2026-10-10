package workflow

import (
	"context"
	"time"

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

// nexusProgressMinInterval is the shortest time between a Workflow Task that carried progress and
// the next task progress schedules.
func (ms *MutableStateImpl) nexusProgressMinInterval() time.Duration {
	if ms.config.NexusOperationProgressMinInterval == nil {
		return 0
	}
	return ms.config.NexusOperationProgressMinInterval(ms.GetNamespaceEntry().Name().String())
}

// scheduleWorkflowTaskForNexusProgress schedules a Workflow Task to carry pending Nexus operation
// progress when no task is pending to carry it, unless a task carried progress too recently.
func (ms *MutableStateImpl) scheduleWorkflowTaskForNexusProgress() error {
	// Only a CHASM change (progress arrived, or a hold was released) or a Workflow Task change can
	// make progress need a task, so most transactions skip resolving the CHASM root.
	if !ms.workflowTaskUpdated && !ms.chasmTree.IsStateDirty() {
		return nil
	}
	if ms.HasPendingWorkflowTask() || ms.IsWorkflowExecutionStatusPaused() {
		return nil
	}
	wf, chasmCtx, ok := ms.chasmWorkflowView()
	if !ok || !wf.HasPendingNexusProgress(chasmCtx) {
		return nil
	}
	minInterval := ms.nexusProgressMinInterval()
	now := chasmCtx.Now(wf)
	if readyAt := wf.NexusProgressReadyAt(chasmCtx, minInterval); now.Before(readyAt) {
		return ms.waitNexusProgress(readyAt)
	}
	if _, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL); err != nil {
		return err
	}
	if !ms.hasPendingNexusProgress() {
		return nil
	}
	// The task can't carry the progress, for example after a failover converted it. Without a
	// hold, every later transaction would schedule another task for it. The floor stops that loop
	// when the interval is zero.
	ms.logger.Info("held Nexus operation progress that a scheduled Workflow Task can't carry")
	return ms.waitNexusProgress(now.Add(max(minInterval, time.Second)))
}

func (ms *MutableStateImpl) waitNexusProgress(readyAt time.Time) error {
	wf, chasmCtx, err := ms.ChasmWorkflowComponent(context.Background())
	if err != nil {
		return err
	}
	wf.HoldNexusProgress(chasmCtx, readyAt)
	return nil
}
