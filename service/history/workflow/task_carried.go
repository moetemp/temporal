package workflow

import (
	"context"
	"time"

	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/protobuf/types/known/anypb"
)

// taskCarrier is one kind of state that writes no event and rides the attachments of the next
// Workflow Task scheduled event. The scheduling rules are the same for every kind. A pending task
// takes the state, state that lands on a scheduled task that has not started gets one follow-up
// task, a heartbeat task carries none, and tasks that carry only this state are rate limited. Each
// kind keeps its own index on the CHASM Workflow.
type taskCarrier interface {
	hasPending(wf *chasmworkflow.Workflow, ctx chasm.Context) bool
	hasScheduled(wf *chasmworkflow.Workflow, ctx chasm.Context) bool
	// take moves the pending state onto the attachments of a scheduled event being written.
	take(
		wf *chasmworkflow.Workflow,
		ctx chasm.MutableContext,
		attrs *historypb.WorkflowTaskScheduledEventAttributes,
	) error
	clearScheduled(wf *chasmworkflow.Workflow, ctx chasm.MutableContext)
	readyAt(wf *chasmworkflow.Workflow, ctx chasm.Context, minInterval time.Duration) time.Time
	hold(wf *chasmworkflow.Workflow, ctx chasm.MutableContext, readyAt time.Time)
	minInterval(ms *MutableStateImpl) time.Duration
}

// taskCarriers lists every kind of state that rides Workflow Task scheduled events.
var taskCarriers = []taskCarrier{streamWakeCarrier{}, nexusProgressCarrier{}}

type streamWakeCarrier struct{}

func (streamWakeCarrier) hasPending(wf *chasmworkflow.Workflow, ctx chasm.Context) bool {
	return wf.HasPendingStreamWakes(ctx)
}

func (streamWakeCarrier) hasScheduled(wf *chasmworkflow.Workflow, ctx chasm.Context) bool {
	return wf.HasScheduledStreamWakes(ctx)
}

func (streamWakeCarrier) take(
	wf *chasmworkflow.Workflow,
	ctx chasm.MutableContext,
	attrs *historypb.WorkflowTaskScheduledEventAttributes,
) error {
	for _, wake := range wf.TakeStreamWakes(ctx) {
		attachment, err := anypb.New(wake)
		if err != nil {
			return err
		}
		attrs.Attachments = append(attrs.Attachments, attachment)
	}
	return nil
}

func (streamWakeCarrier) clearScheduled(wf *chasmworkflow.Workflow, ctx chasm.MutableContext) {
	wf.ClearScheduledStreamWakes(ctx)
}

func (streamWakeCarrier) readyAt(
	wf *chasmworkflow.Workflow,
	ctx chasm.Context,
	minInterval time.Duration,
) time.Time {
	return wf.StreamWakesReadyAt(ctx, minInterval)
}

func (streamWakeCarrier) hold(
	wf *chasmworkflow.Workflow,
	ctx chasm.MutableContext,
	readyAt time.Time,
) {
	wf.HoldStreamWakes(ctx, readyAt)
}

func (streamWakeCarrier) minInterval(ms *MutableStateImpl) time.Duration {
	if ms.config.StreamWakeMinInterval == nil {
		return 0
	}
	return ms.config.StreamWakeMinInterval(ms.GetNamespaceEntry().Name().String())
}

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

// pendingCarriers answers the kinds whose state waits for the next Workflow Task scheduled event.
func (ms *MutableStateImpl) pendingCarriers() []taskCarrier {
	wf, chasmCtx, ok := ms.chasmWorkflowView()
	if !ok {
		return nil
	}
	var pending []taskCarrier
	for _, carrier := range taskCarriers {
		if carrier.hasPending(wf, chasmCtx) {
			pending = append(pending, carrier)
		}
	}
	return pending
}

// HoldCarriedState keeps carried state off the Workflow Task scheduled in this transaction and off
// its retries, so it waits for a later task.
func (ms *MutableStateImpl) HoldCarriedState() {
	ms.holdCarriedState = true
}

// attachCarriedState puts the pending carried state on a WorkflowTaskScheduled event for a task no
// worker has seen yet. The event is the acknowledgment, so the state is taken.
func (ms *MutableStateImpl) attachCarriedState(event *historypb.HistoryEvent) {
	if ms.executionInfo.GetWorkflowTaskHoldsCarriedState() {
		return
	}
	pending := ms.pendingCarriers()
	if len(pending) == 0 {
		return
	}
	wf, chasmCtx, err := ms.ChasmWorkflowComponent(context.Background())
	if err != nil {
		ms.logger.Warn("cannot attach carried state to a Workflow Task", tag.Error(err))
		return
	}
	attrs := event.GetWorkflowTaskScheduledEventAttributes()
	for _, carrier := range pending {
		if err := carrier.take(wf, chasmCtx, attrs); err != nil {
			ms.logger.Warn("cannot attach carried state to a Workflow Task", tag.Error(err))
		}
	}
}

// clearScheduledCarriedState forgets which state the scheduled Workflow Task carries, once that
// task starts, fails or times out, so newer state schedules a task of its own. Checked through a
// read-only view first, since almost no Workflow has any.
func (ms *MutableStateImpl) clearScheduledCarriedState() {
	wf, chasmCtx, ok := ms.chasmWorkflowView()
	if !ok {
		return
	}
	var scheduled []taskCarrier
	for _, carrier := range taskCarriers {
		if carrier.hasScheduled(wf, chasmCtx) {
			scheduled = append(scheduled, carrier)
		}
	}
	if len(scheduled) == 0 {
		return
	}
	wf, mutableCtx, err := ms.ChasmWorkflowComponent(context.Background())
	if err != nil {
		ms.logger.Warn("cannot clear the carried state of a Workflow Task", tag.Error(err))
		return
	}
	for _, carrier := range scheduled {
		carrier.clearScheduled(wf, mutableCtx)
	}
}

// scheduleWorkflowTaskForCarriedState schedules a Workflow Task to carry pending state when no task
// is pending to carry it, unless a task carried state of that kind too recently.
func (ms *MutableStateImpl) scheduleWorkflowTaskForCarriedState() error {
	// Only a CHASM change (state arrived, or a hold was released) or a Workflow Task change can
	// make state need a task, so most transactions skip resolving the CHASM root.
	if !ms.workflowTaskUpdated && !ms.chasmTree.IsStateDirty() {
		return nil
	}
	if ms.HasPendingWorkflowTask() || ms.IsWorkflowExecutionStatusPaused() {
		return nil
	}
	pending := ms.pendingCarriers()
	if len(pending) == 0 {
		return nil
	}
	wf, chasmCtx, _ := ms.chasmWorkflowView()
	now := chasmCtx.Now(wf)
	var ready bool
	var waitUntil time.Time
	for _, carrier := range pending {
		readyAt := carrier.readyAt(wf, chasmCtx, carrier.minInterval(ms))
		if !now.Before(readyAt) {
			ready = true
			break
		}
		if waitUntil.IsZero() || readyAt.Before(waitUntil) {
			waitUntil = readyAt
		}
	}
	if !ready {
		return ms.holdCarriers(pending, waitUntil)
	}
	_, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	if err != nil {
		return err
	}
	stillPending := ms.pendingCarriers()
	if len(stillPending) == 0 {
		return nil
	}
	// The task can't carry the state, for example after a failover converted it. Without a hold,
	// every later transaction would schedule another task for it. The floor stops that loop when
	// the interval is zero.
	ms.logger.Info("held carried state that a scheduled Workflow Task can't carry")
	var minInterval time.Duration
	for _, carrier := range stillPending {
		minInterval = max(minInterval, carrier.minInterval(ms))
	}
	return ms.holdCarriers(stillPending, now.Add(max(minInterval, time.Second)))
}

func (ms *MutableStateImpl) holdCarriers(carriers []taskCarrier, readyAt time.Time) error {
	wf, chasmCtx, err := ms.ChasmWorkflowComponent(context.Background())
	if err != nil {
		return err
	}
	for _, carrier := range carriers {
		carrier.hold(wf, chasmCtx, readyAt)
	}
	return nil
}
