package workflow

import (
	"context"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
)

// hasPendingNexusProgress reports whether a Nexus operation of this Workflow holds progress for
// the next Workflow Task scheduled event. Progress writes no event, so nothing else schedules the
// task that carries it.
func (ms *MutableStateImpl) hasPendingNexusProgress() bool {
	wf, chasmCtx, ok := ms.chasmWorkflowView()
	return ok && wf.HasPendingNexusProgress(chasmCtx)
}

// attachNexusProgress puts the pending Nexus operation progress on a WorkflowTaskScheduled event
// for a task no worker has seen. The event is the acknowledgment, so the progress is cleared.
func (ms *MutableStateImpl) attachNexusProgress(event *historypb.HistoryEvent) {
	if !ms.hasPendingNexusProgress() {
		return
	}
	wf, chasmCtx, err := ms.ChasmWorkflowComponent(context.Background())
	if err != nil {
		return
	}
	attrs := event.GetWorkflowTaskScheduledEventAttributes()
	attrs.Notifications = append(attrs.Notifications, wf.TakeNexusProgress(chasmCtx)...)
}

// chasmWorkflowView resolves the Workflow component through a read-only context. It is asked on
// every transaction close, so it avoids taking the component for writing.
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
