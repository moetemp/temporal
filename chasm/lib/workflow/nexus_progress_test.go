package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	nexuspb "go.temporal.io/api/nexus/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestNexusProgressIndex(t *testing.T) {
	newWorkflowWithOperations := func(
		t *testing.T,
		ctx chasm.MutableContext,
		keys ...int64,
	) *Workflow {
		wf := &Workflow{}
		for _, key := range keys {
			parentData, err := anypb.New(
				&chasmworkflowpb.NexusOperationParentData{ScheduledEventId: key},
			)
			require.NoError(t, err)
			op := nexusoperation.NewOperation(&nexusoperationpb.OperationState{
				Status:     nexusoperationpb.OPERATION_STATUS_STARTED,
				ParentData: parentData,
			})
			op.Store = chasm.NewMockParentPtr[nexusoperation.OperationStore](wf)
			wf.addNexusOperation(ctx, key, op)
		}
		return wf
	}
	deliver := func(t *testing.T, ctx chasm.MutableContext, wf *Workflow, key, counter int64) {
		err := wf.Operations[key].Get(ctx).
			HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				Outcome: &persistencespb.ChasmNexusCompletion_Progress{
					Progress: &nexuspb.NexusOperationProgress{Counter: counter},
				},
			})
		require.NoError(t, err)
	}
	counters := func(taken []*nexuspb.NexusOperationProgress) map[int64]int64 {
		out := map[int64]int64{}
		for _, p := range taken {
			out[p.GetScheduledEventId()] = p.GetCounter()
		}
		return out
	}

	t.Run("TakesPendingProgressPerOperationInScheduledOrder", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newWorkflowWithOperations(t, ctx, 7, 5)
		require.False(t, wf.HasPendingNexusProgress(ctx))
		deliver(t, ctx, wf, 7, 1)
		deliver(t, ctx, wf, 5, 2)
		deliver(t, ctx, wf, 7, 3)
		require.True(t, wf.HasPendingNexusProgress(ctx))

		taken := wf.TakeNexusProgress(ctx)
		require.Len(t, taken, 2)
		require.Equal(t, int64(5), taken[0].GetScheduledEventId())
		require.Equal(t, map[int64]int64{5: 2, 7: 3}, counters(taken))
		require.False(t, wf.HasPendingNexusProgress(ctx))
		require.True(t, wf.HasScheduledNexusProgress(ctx))
	})

	t.Run("FoldsIntoTheUnstartedTaskThenWaitsForTheNextOne", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newWorkflowWithOperations(t, ctx, 5, 9)
		deliver(t, ctx, wf, 5, 1)
		require.Len(t, wf.TakeNexusProgress(ctx), 1)

		deliver(t, ctx, wf, 5, 2)
		deliver(t, ctx, wf, 5, 3)
		require.False(t, wf.HasPendingNexusProgress(ctx), "operation 5 rides the unstarted task")
		deliver(t, ctx, wf, 9, 1)
		require.True(t, wf.HasPendingNexusProgress(ctx), "operation 9 is not on that task")
		require.Equal(t, map[int64]int64{9: 1}, counters(wf.TakeNexusProgress(ctx)),
			"folded progress is not taken while its task waits to start")

		// The task started with counter 1 on its event, so counter 3 needs one more task.
		wf.ClearScheduledNexusProgress(ctx)
		require.False(t, wf.HasScheduledNexusProgress(ctx))
		require.True(t, wf.HasPendingNexusProgress(ctx))
		require.Equal(t, map[int64]int64{5: 3}, counters(wf.TakeNexusProgress(ctx)))
	})

	t.Run("ForgetsAClosedOperation", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newWorkflowWithOperations(t, ctx, 5, 9)
		deliver(t, ctx, wf, 5, 1)
		deliver(t, ctx, wf, 9, 1)
		require.Len(t, wf.TakeNexusProgress(ctx), 2)
		deliver(t, ctx, wf, 5, 2)
		deliver(t, ctx, wf, 9, 2)
		wf.removeNexusOperation(ctx, 5)
		wf.ClearScheduledNexusProgress(ctx)
		require.Equal(
			t,
			map[int64]int64{9: 2},
			counters(wf.TakeNexusProgress(ctx)),
			"a closed operation's folded progress is gone",
		)
		wf.removeNexusOperation(ctx, 9)
		require.False(t, wf.HasScheduledNexusProgress(ctx))
		index, ok := wf.NexusProgress.TryGet(ctx)
		require.True(t, ok)
		require.Empty(t, index.GetPending())
		require.Empty(t, index.GetScheduled())
		require.Empty(t, index.GetFolded())
		require.NotNil(
			t,
			index.GetLastCarriedTime(),
			"only the last carried time stays, for the rate limit",
		)
	})

	t.Run("StoresNoIndexWithoutProgress", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newWorkflowWithOperations(t, ctx, 5)
		wf.ClearScheduledNexusProgress(ctx)
		require.Empty(t, wf.TakeNexusProgress(ctx))
		_, ok := wf.NexusProgress.TryGet(ctx)
		require.False(t, ok)
	})

	t.Run("HoldsProgressUntilTheIntervalSinceTheLastCarry", func(t *testing.T) {
		now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
		ctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return now },
		}}
		wf := newWorkflowWithOperations(t, ctx, 5)
		require.True(t, wf.NexusProgressReadyAt(ctx, time.Second).IsZero(), "nothing carried yet")
		deliver(t, ctx, wf, 5, 1)
		require.Len(t, wf.TakeNexusProgress(ctx), 1)
		wf.ClearScheduledNexusProgress(ctx)
		readyAt := wf.NexusProgressReadyAt(ctx, time.Second)
		require.Equal(t, now.Add(time.Second), readyAt)

		deliver(t, ctx, wf, 5, 2)
		wf.HoldNexusProgress(ctx, readyAt)
		wf.HoldNexusProgress(ctx, readyAt)
		require.Len(t, ctx.Tasks, 1, "one armed release covers the same deadline")
		require.Equal(t, readyAt, ctx.Tasks[0].Attributes.ScheduledTime)
		require.True(t, wf.nexusProgressHeld(ctx))
		require.True(t, wf.HasPendingNexusProgress(ctx), "held progress still rides any other task")

		wf.releaseNexusProgress(ctx)
		require.False(t, wf.nexusProgressHeld(ctx))
		require.True(t, wf.HasPendingNexusProgress(ctx))

		wf.HoldNexusProgress(ctx, readyAt)
		require.Len(t, ctx.Tasks, 2, "a released hold arms again")
		now = now.Add(time.Second)
		require.Len(t, wf.TakeNexusProgress(ctx), 1)
		require.False(t, wf.nexusProgressHeld(ctx), "a task that takes the progress ends the hold")
		require.Equal(t, now.Add(time.Second), wf.NexusProgressReadyAt(ctx, time.Second))
	})
}
