package workflow

import (
	"slices"
	"time"

	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The functions in this file keep a TaskCarriedIndex: the rules for state that writes no event
// and rides the next Workflow Task scheduled event, whatever the state is. Each kind of such state
// keeps its own index and its own entries by key. Each function returns a changed copy and leaves
// its input alone, since the input is persisted CHASM data.

// cloneCarriedIndex copies the index, or answers an empty one for a kind that has none yet.
func cloneCarriedIndex(index *chasmworkflowpb.TaskCarriedIndex) *chasmworkflowpb.TaskCarriedIndex {
	if index == nil {
		return &chasmworkflowpb.TaskCarriedIndex{}
	}
	return common.CloneProto(index)
}

// carriedIndexAdd records that the key holds state for the next Workflow Task scheduled event.
// State for a key that rides a scheduled task that has not started folds into that task: it asks
// for no task of its own until that one starts.
func carriedIndexAdd(
	index *chasmworkflowpb.TaskCarriedIndex,
	key string,
) *chasmworkflowpb.TaskCarriedIndex {
	index = cloneCarriedIndex(index)
	waiting := &index.Pending
	if slices.Contains(index.Scheduled, key) {
		waiting = &index.Folded
	}
	if !slices.Contains(*waiting, key) {
		*waiting = append(*waiting, key)
		slices.Sort(*waiting)
	}
	return index
}

// carriedIndexTake moves every pending key to scheduled, for a Workflow Task scheduled event that
// is being written and carries their state. It returns the keys it took, in order.
func carriedIndexTake(
	index *chasmworkflowpb.TaskCarriedIndex,
	now time.Time,
) (*chasmworkflowpb.TaskCarriedIndex, []string) {
	index = cloneCarriedIndex(index)
	taken := index.Pending
	for _, key := range taken {
		if !slices.Contains(index.Scheduled, key) {
			index.Scheduled = append(index.Scheduled, key)
		}
	}
	slices.Sort(index.Scheduled)
	index.Pending = nil
	index.ReleaseTime = nil
	if len(taken) > 0 {
		index.LastCarriedTime = timestamppb.New(now)
	}
	return index, taken
}

// carriedIndexClearScheduled forgets which keys the scheduled Workflow Task carries. Called once
// that task starts, fails or times out: from then on newer state needs a task of its own, and state
// that folded into it waits for the next task, since its scheduled event carries older state.
func carriedIndexClearScheduled(
	index *chasmworkflowpb.TaskCarriedIndex,
) *chasmworkflowpb.TaskCarriedIndex {
	index = cloneCarriedIndex(index)
	for _, key := range index.Folded {
		if !slices.Contains(index.Pending, key) {
			index.Pending = append(index.Pending, key)
		}
	}
	slices.Sort(index.Pending)
	index.Scheduled = nil
	index.Folded = nil
	return index
}

// carriedIndexReadyAt answers when pending state may schedule a Workflow Task of its own:
// minInterval after the last task that carried state of this kind.
func carriedIndexReadyAt(
	index *chasmworkflowpb.TaskCarriedIndex,
	minInterval time.Duration,
) time.Time {
	last := index.GetLastCarriedTime()
	if last == nil {
		return time.Time{}
	}
	return last.AsTime().Add(minInterval)
}

// carriedIndexHold arms a release at readyAt and reports whether a release task is needed. A
// release armed for the future and no later than readyAt already covers it. One that fires early
// releases nothing, since the state is held again until readyAt.
func carriedIndexHold(
	index *chasmworkflowpb.TaskCarriedIndex,
	now, readyAt time.Time,
) (*chasmworkflowpb.TaskCarriedIndex, bool) {
	if release := index.GetReleaseTime(); release != nil && release.AsTime().After(now) &&
		!release.AsTime().After(readyAt) {
		return index, false
	}
	index = cloneCarriedIndex(index)
	index.ReleaseTime = timestamppb.New(readyAt)
	return index, true
}

// carriedIndexHeld reports whether a release is armed for pending state.
func carriedIndexHeld(index *chasmworkflowpb.TaskCarriedIndex) bool {
	return index.GetReleaseTime() != nil && len(index.GetPending()) > 0
}
