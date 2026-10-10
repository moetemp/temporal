package workflow

import (
	"slices"
	"time"

	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// nexusProgressIndex answers the index, or an empty one when no operation ever had progress.
func (w *Workflow) nexusProgressIndex(ctx chasm.Context) *chasmworkflowpb.NexusProgressState {
	if index, ok := w.NexusProgress.TryGet(ctx); ok {
		return index
	}
	return &chasmworkflowpb.NexusProgressState{}
}

// setNexusProgressIndex stores the index, or drops it when it is empty, so a Workflow whose
// operations report no progress carries nothing.
func (w *Workflow) setNexusProgressIndex(
	ctx chasm.MutableContext,
	index *chasmworkflowpb.NexusProgressState,
) {
	if len(index.GetPending()) == 0 && len(index.GetScheduled()) == 0 &&
		len(index.GetFolded()) == 0 && index.GetLastCarriedTime() == nil {
		if _, ok := w.NexusProgress.TryGet(ctx); ok {
			w.NexusProgress = chasm.NewEmptyField[*chasmworkflowpb.NexusProgressState]()
		}
		return
	}
	w.NexusProgress = chasm.NewDataField(ctx, index)
}

// OnNexusOperationProgress records that an operation holds progress for the next Workflow Task
// scheduled event. Progress for an operation that rides a scheduled Workflow Task that has not
// started folds into that task: it asks for no task of its own until that one starts.
func (w *Workflow) OnNexusOperationProgress(ctx chasm.MutableContext, op *nexusoperation.Operation) error {
	key, ok := nexusProgressKey(op)
	if !ok {
		op.DropPendingProgress(ctx)
		return nil
	}
	index := common.CloneProto(w.nexusProgressIndex(ctx))
	waiting := &index.Pending
	if slices.Contains(index.Scheduled, key) {
		waiting = &index.Folded
	}
	if !slices.Contains(*waiting, key) {
		*waiting = append(*waiting, key)
		slices.Sort(*waiting)
	}
	w.setNexusProgressIndex(ctx, index)
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
	index := common.CloneProto(w.nexusProgressIndex(ctx))
	var taken []*nexuspb.NexusOperationProgress
	for _, key := range index.GetPending() {
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
		if !slices.Contains(index.Scheduled, key) {
			index.Scheduled = append(index.Scheduled, key)
		}
	}
	slices.Sort(index.Scheduled)
	index.Pending = nil
	index.ReleaseTime = nil
	if len(taken) > 0 {
		index.LastCarriedTime = timestamppb.New(ctx.Now(w))
	}
	w.setNexusProgressIndex(ctx, index)
	return taken
}

// NexusProgressReadyAt answers when pending progress may schedule a Workflow Task of its own:
// minInterval after the last task that carried progress.
func (w *Workflow) NexusProgressReadyAt(ctx chasm.Context, minInterval time.Duration) time.Time {
	last := w.nexusProgressIndex(ctx).GetLastCarriedTime()
	if last == nil {
		return time.Time{}
	}
	return last.AsTime().Add(minInterval)
}

// HoldNexusProgress keeps pending progress waiting until readyAt, when a NexusProgressReleaseTask
// lets it schedule a Workflow Task. A task scheduled for any other reason still takes it sooner.
func (w *Workflow) HoldNexusProgress(ctx chasm.MutableContext, readyAt time.Time) {
	index := w.nexusProgressIndex(ctx)
	// A release armed for the future and no later than readyAt already covers this. One that
	// fires early releases nothing, since the progress is held again until readyAt.
	if release := index.GetReleaseTime(); release != nil && release.AsTime().After(ctx.Now(w)) &&
		!release.AsTime().After(readyAt) {
		return
	}
	index = common.CloneProto(index)
	index.ReleaseTime = timestamppb.New(readyAt)
	w.setNexusProgressIndex(ctx, index)
	ctx.AddTask(w, chasm.TaskAttributes{ScheduledTime: readyAt}, &chasmworkflowpb.NexusProgressReleaseTask{})
}

// releaseNexusProgress lets held progress schedule a Workflow Task. It changes the Workflow, so
// the transaction that closes after it schedules the task.
func (w *Workflow) releaseNexusProgress(ctx chasm.MutableContext) {
	index := common.CloneProto(w.nexusProgressIndex(ctx))
	index.ReleaseTime = nil
	w.setNexusProgressIndex(ctx, index)
}

// nexusProgressHeld reports whether a release is armed for pending progress.
func (w *Workflow) nexusProgressHeld(ctx chasm.Context) bool {
	index := w.nexusProgressIndex(ctx)
	return index.GetReleaseTime() != nil && len(index.GetPending()) > 0
}

// ClearScheduledNexusProgress forgets which operations the scheduled Workflow Task carries. Called
// once that task starts, fails or times out: from then on newer progress needs a task of its own,
// and progress that folded into it waits for the next task, since its scheduled event carries an
// older counter.
func (w *Workflow) ClearScheduledNexusProgress(ctx chasm.MutableContext) {
	index := w.nexusProgressIndex(ctx)
	if len(index.GetScheduled()) == 0 && len(index.GetFolded()) == 0 {
		return
	}
	index = common.CloneProto(index)
	for _, key := range index.Folded {
		if !slices.Contains(index.Pending, key) {
			index.Pending = append(index.Pending, key)
		}
	}
	slices.Sort(index.Pending)
	index.Scheduled = nil
	index.Folded = nil
	w.setNexusProgressIndex(ctx, index)
}

// forgetNexusProgress removes a closed operation from the index.
func (w *Workflow) forgetNexusProgress(ctx chasm.MutableContext, key int64) {
	index := w.nexusProgressIndex(ctx)
	if !slices.Contains(index.GetPending(), key) && !slices.Contains(index.GetScheduled(), key) &&
		!slices.Contains(index.GetFolded(), key) {
		return
	}
	index = common.CloneProto(index)
	index.Pending = slices.DeleteFunc(index.Pending, func(k int64) bool { return k == key })
	index.Scheduled = slices.DeleteFunc(index.Scheduled, func(k int64) bool { return k == key })
	index.Folded = slices.DeleteFunc(index.Folded, func(k int64) bool { return k == key })
	w.setNexusProgressIndex(ctx, index)
}
