package workflow

import (
	"cmp"
	"slices"
	"strconv"
	"time"

	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common"
)

var _ nexusoperation.ProgressStore = (*Workflow)(nil)

// nexusProgressKey is the scheduled event ID that keys an operation in the Workflow.
func nexusProgressKey(op *nexusoperation.Operation) (int64, bool) {
	parentData := &chasmworkflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return 0, false
	}
	return parentData.GetScheduledEventId(), true
}

// nexusProgressIndexKey is the key of an operation in the carried index.
func nexusProgressIndexKey(scheduledEventID int64) string {
	return strconv.FormatInt(scheduledEventID, 10)
}

// nexusProgressIndex answers the index, or an empty one when no operation ever had progress.
func (w *Workflow) nexusProgressIndex(ctx chasm.Context) *chasmworkflowpb.TaskCarriedIndex {
	if state, ok := w.NexusProgress.TryGet(ctx); ok && state.GetIndex() != nil {
		return state.GetIndex()
	}
	return &chasmworkflowpb.TaskCarriedIndex{}
}

// setNexusProgressIndex stores the index, or drops it when it is empty, so a Workflow whose
// operations report no progress carries nothing.
func (w *Workflow) setNexusProgressIndex(
	ctx chasm.MutableContext,
	index *chasmworkflowpb.TaskCarriedIndex,
) {
	if len(index.GetPending()) == 0 && len(index.GetScheduled()) == 0 &&
		len(index.GetFolded()) == 0 && index.GetLastCarriedTime() == nil {
		if _, ok := w.NexusProgress.TryGet(ctx); ok {
			w.NexusProgress = chasm.NewEmptyField[*chasmworkflowpb.NexusProgressState]()
		}
		return
	}
	w.NexusProgress = chasm.NewDataField(ctx, &chasmworkflowpb.NexusProgressState{Index: index})
}

// OnNexusOperationProgress records that an operation holds progress for the next Workflow Task
// scheduled event. Progress for an operation that rides a scheduled Workflow Task that has not
// started folds into that task: it asks for no task of its own until that one starts.
func (w *Workflow) OnNexusOperationProgress(
	ctx chasm.MutableContext,
	op *nexusoperation.Operation,
) error {
	key, ok := nexusProgressKey(op)
	if !ok {
		op.DropPendingProgress(ctx)
		return nil
	}
	w.setNexusProgressIndex(
		ctx,
		carriedIndexAdd(w.nexusProgressIndex(ctx), nexusProgressIndexKey(key)),
	)
	return nil
}

// HasPendingNexusProgress reports whether any operation holds progress for the next Workflow Task
// scheduled event. It reads one field, so it is cheap on every transaction close.
func (w *Workflow) HasPendingNexusProgress(ctx chasm.Context) bool {
	return len(w.nexusProgressIndex(ctx).GetPending()) > 0
}

// HasScheduledNexusProgress reports whether a scheduled Workflow Task carries progress.
func (w *Workflow) HasScheduledNexusProgress(ctx chasm.Context) bool {
	index := w.nexusProgressIndex(ctx)
	return len(index.GetScheduled()) > 0 || len(index.GetFolded()) > 0
}

// TakeNexusProgress returns the pending progress of every operation in scheduled event ID order,
// each naming its operation, for a Workflow Task scheduled event that is being written. The event
// acknowledges it, and until the task starts newer progress for those operations folds into it.
func (w *Workflow) TakeNexusProgress(ctx chasm.MutableContext) []*nexuspb.NexusOperationProgress {
	previous := w.nexusProgressIndex(ctx)
	index, keys := carriedIndexTake(previous, ctx.Now(w))
	var taken []*nexuspb.NexusOperationProgress
	for _, indexKey := range keys {
		key, err := strconv.ParseInt(indexKey, 10, 64)
		if err != nil {
			continue
		}
		field, ok := w.Operations[key]
		if !ok {
			continue
		}
		progress, ok := field.Get(ctx).TakePendingProgress(ctx)
		if !ok {
			continue
		}
		named := common.CloneProto(progress)
		named.Operation = &nexuspb.NexusOperationProgress_ScheduledEventId{ScheduledEventId: key}
		taken = append(taken, named)
	}
	// The index sorts its keys as strings, and the event lists operations by scheduled event ID.
	slices.SortFunc(taken, func(a, b *nexuspb.NexusOperationProgress) int {
		return cmp.Compare(a.GetScheduledEventId(), b.GetScheduledEventId())
	})
	if len(taken) == 0 {
		// No task carried progress, so the rate limit keeps its last mark.
		index.LastCarriedTime = previous.GetLastCarriedTime()
	}
	w.setNexusProgressIndex(ctx, index)
	return taken
}

// NexusProgressReadyAt answers when pending progress may schedule a Workflow Task of its own:
// minInterval after the last task that carried progress.
func (w *Workflow) NexusProgressReadyAt(ctx chasm.Context, minInterval time.Duration) time.Time {
	return carriedIndexReadyAt(w.nexusProgressIndex(ctx), minInterval)
}

// HoldNexusProgress keeps pending progress waiting until readyAt, when a NexusProgressReleaseTask
// lets it schedule a Workflow Task. A task scheduled for any other reason still takes it sooner.
func (w *Workflow) HoldNexusProgress(ctx chasm.MutableContext, readyAt time.Time) {
	index, arm := carriedIndexHold(w.nexusProgressIndex(ctx), ctx.Now(w), readyAt)
	if !arm {
		return
	}
	w.setNexusProgressIndex(ctx, index)
	ctx.AddTask(
		w,
		chasm.TaskAttributes{ScheduledTime: readyAt},
		&chasmworkflowpb.NexusProgressReleaseTask{},
	)
}

// releaseNexusProgress lets held progress schedule a Workflow Task. It changes the Workflow, so
// the transaction that closes after it schedules the task.
func (w *Workflow) releaseNexusProgress(ctx chasm.MutableContext) {
	index := cloneCarriedIndex(w.nexusProgressIndex(ctx))
	index.ReleaseTime = nil
	w.setNexusProgressIndex(ctx, index)
}

// nexusProgressHeld reports whether a release is armed for pending progress.
func (w *Workflow) nexusProgressHeld(ctx chasm.Context) bool {
	return carriedIndexHeld(w.nexusProgressIndex(ctx))
}

// ClearScheduledNexusProgress forgets which operations the scheduled Workflow Task carries, once
// that task starts, fails or times out.
func (w *Workflow) ClearScheduledNexusProgress(ctx chasm.MutableContext) {
	if !w.HasScheduledNexusProgress(ctx) {
		return
	}
	w.setNexusProgressIndex(ctx, carriedIndexClearScheduled(w.nexusProgressIndex(ctx)))
}

// forgetNexusProgress removes a closed operation from the index.
func (w *Workflow) forgetNexusProgress(ctx chasm.MutableContext, key int64) {
	indexKey := nexusProgressIndexKey(key)
	index := w.nexusProgressIndex(ctx)
	if !slices.Contains(index.GetPending(), indexKey) &&
		!slices.Contains(index.GetScheduled(), indexKey) &&
		!slices.Contains(index.GetFolded(), indexKey) {
		return
	}
	index = cloneCarriedIndex(index)
	isKey := func(k string) bool { return k == indexKey }
	index.Pending = slices.DeleteFunc(index.Pending, isKey)
	index.Scheduled = slices.DeleteFunc(index.Scheduled, isKey)
	index.Folded = slices.DeleteFunc(index.Folded, isKey)
	w.setNexusProgressIndex(ctx, index)
}
