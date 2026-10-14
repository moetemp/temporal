package workflow

import (
	"maps"
	"slices"
	"time"

	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/server/chasm"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common"
)

// streamWakeState answers the stream wake state, or an empty one when no reader got a wake.
func (w *Workflow) streamWakeState(ctx chasm.Context) *chasmworkflowpb.StreamWakeState {
	if state, ok := w.StreamWakes.TryGet(ctx); ok {
		return state
	}
	return &chasmworkflowpb.StreamWakeState{}
}

func (w *Workflow) setStreamWakeState(
	ctx chasm.MutableContext,
	state *chasmworkflowpb.StreamWakeState,
) {
	w.StreamWakes = chasm.NewDataField(ctx, state)
}

// RecordStreamWake keeps the latest wake of one stream reader for the next Workflow Task scheduled
// event. A wake for a reader that rides a scheduled Workflow Task that has not started folds into
// that task, and one follow-up task carries it. A run takes wakes for at most maxSources readers.
func (w *Workflow) RecordStreamWake(
	ctx chasm.MutableContext,
	wake *streampb.StreamWake,
	maxSources int,
) error {
	key := wake.GetSubscriptionId()
	state := common.CloneProto(w.streamWakeState(ctx))
	if _, found := slices.BinarySearch(state.Sources, key); !found {
		if len(state.Sources) >= maxSources {
			return serviceerror.NewFailedPreconditionf(
				"the Workflow run takes stream wakes for at most %d readers",
				maxSources,
			)
		}
		state.Sources = append(state.Sources, key)
		slices.Sort(state.Sources)
	}
	if state.Wakes == nil {
		state.Wakes = map[string]*streampb.StreamWake{}
	}
	state.Wakes[key] = foldStreamWake(state.Wakes[key], wake)
	state.Index = carriedIndexAdd(state.GetIndex(), key)
	w.setStreamWakeState(ctx, state)
	return nil
}

// foldStreamWake merges a wake into the one a reader has not seen yet. The highest counter wins
// with its position and metadata. A recheck, a close and a resubscribe stay set once set, since
// the reader must act on each.
func foldStreamWake(seen, next *streampb.StreamWake) *streampb.StreamWake {
	if seen == nil {
		return common.CloneProto(next)
	}
	folded := common.CloneProto(seen)
	if next.GetCounter() > folded.GetCounter() {
		folded.Counter = next.GetCounter()
		folded.Position = next.GetPosition()
		folded.Metadata = maps.Clone(next.GetMetadata())
	}
	folded.Recheck = folded.GetRecheck() || next.GetRecheck()
	folded.Resubscribe = folded.GetResubscribe() || next.GetResubscribe()
	if next.GetClosed() && !folded.GetClosed() {
		folded.Closed = true
		folded.CloseResult = common.CloneProto(next.GetCloseResult())
		folded.CloseFailure = common.CloneProto(next.GetCloseFailure())
	}
	return folded
}

// HasPendingStreamWakes reports whether a reader's wake waits for the next Workflow Task scheduled
// event. It reads one field, so it is cheap on every transaction close.
func (w *Workflow) HasPendingStreamWakes(ctx chasm.Context) bool {
	return len(w.streamWakeState(ctx).GetIndex().GetPending()) > 0
}

// HasScheduledStreamWakes reports whether a scheduled Workflow Task carries wakes.
func (w *Workflow) HasScheduledStreamWakes(ctx chasm.Context) bool {
	index := w.streamWakeState(ctx).GetIndex()
	return len(index.GetScheduled()) > 0 || len(index.GetFolded()) > 0
}

// TakeStreamWakes returns the pending wake of every reader in subscription ID order, for a Workflow
// Task scheduled event that is being written. The event acknowledges them, and until the task
// starts newer wakes for those readers fold into it.
func (w *Workflow) TakeStreamWakes(ctx chasm.MutableContext) []*streampb.StreamWake {
	state := common.CloneProto(w.streamWakeState(ctx))
	index, keys := carriedIndexTake(state.GetIndex(), ctx.Now(w))
	state.Index = index
	var taken []*streampb.StreamWake
	for _, key := range keys {
		wake, ok := state.Wakes[key]
		if !ok {
			continue
		}
		delete(state.Wakes, key)
		taken = append(taken, wake)
	}
	w.setStreamWakeState(ctx, state)
	return taken
}

// ClearScheduledStreamWakes forgets which readers the scheduled Workflow Task carries, once that
// task starts, fails or times out.
func (w *Workflow) ClearScheduledStreamWakes(ctx chasm.MutableContext) {
	if !w.HasScheduledStreamWakes(ctx) {
		return
	}
	state := common.CloneProto(w.streamWakeState(ctx))
	state.Index = carriedIndexClearScheduled(state.GetIndex())
	w.setStreamWakeState(ctx, state)
}

// StreamWakesReadyAt answers when pending wakes may schedule a Workflow Task of their own.
func (w *Workflow) StreamWakesReadyAt(ctx chasm.Context, minInterval time.Duration) time.Time {
	return carriedIndexReadyAt(w.streamWakeState(ctx).GetIndex(), minInterval)
}

// HoldStreamWakes keeps pending wakes waiting until readyAt, when a StreamWakeReleaseTask lets
// them schedule a Workflow Task. A task scheduled for any other reason still takes them sooner.
func (w *Workflow) HoldStreamWakes(ctx chasm.MutableContext, readyAt time.Time) {
	state := w.streamWakeState(ctx)
	index, arm := carriedIndexHold(state.GetIndex(), ctx.Now(w), readyAt)
	if !arm {
		return
	}
	state = common.CloneProto(state)
	state.Index = index
	w.setStreamWakeState(ctx, state)
	ctx.AddTask(
		w,
		chasm.TaskAttributes{ScheduledTime: readyAt},
		&chasmworkflowpb.StreamWakeReleaseTask{},
	)
}

// releaseStreamWakes lets held wakes schedule a Workflow Task. It changes the Workflow, so the
// transaction that closes after it schedules the task.
func (w *Workflow) releaseStreamWakes(ctx chasm.MutableContext) {
	state := common.CloneProto(w.streamWakeState(ctx))
	if state.Index != nil {
		state.Index.ReleaseTime = nil
	}
	w.setStreamWakeState(ctx, state)
}

// streamWakesHeld reports whether a release is armed for pending wakes.
func (w *Workflow) streamWakesHeld(ctx chasm.Context) bool {
	return carriedIndexHeld(w.streamWakeState(ctx).GetIndex())
}

type streamWakeReleaseTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func (streamWakeReleaseTaskHandler) Validate(
	ctx chasm.Context,
	wf *Workflow,
	_ chasm.TaskInvocation,
	_ *chasmworkflowpb.StreamWakeReleaseTask,
) (bool, error) {
	return wf.streamWakesHeld(ctx), nil
}

func (streamWakeReleaseTaskHandler) Execute(
	ctx chasm.MutableContext,
	wf *Workflow,
	_ chasm.TaskAttributes,
	_ *chasmworkflowpb.StreamWakeReleaseTask,
) error {
	wf.releaseStreamWakes(ctx)
	return nil
}
