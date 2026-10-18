package workflow

import (
	"strconv"
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
				Status:         nexusoperationpb.OPERATION_STATUS_STARTED,
				Endpoint:       "endpoint",
				Service:        "service",
				Operation:      "operation",
				OperationToken: strconv.FormatInt(key, 10),
				ParentData:     parentData,
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
	// Each test operation's token is its scheduled event ID.
	scheduledEventID := func(p *nexuspb.NexusOperationProgress) int64 {
		key, err := strconv.ParseInt(p.GetOperationKey().GetOperationToken(), 10, 64)
		require.NoError(t, err)
		return key
	}
	counters := func(taken []*nexuspb.NexusOperationProgress) map[int64]int64 {
		out := map[int64]int64{}
		for _, p := range taken {
			out[scheduledEventID(p)] = p.GetCounter()
		}
		return out
	}

	t.Run("TakesPendingProgressPerOperationInScheduledOrder", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		// The index keys sort as strings, where "12" comes before "5".
		wf := newWorkflowWithOperations(t, ctx, 12, 5)
		require.False(t, wf.HasPendingNexusProgress(ctx))
		deliver(t, ctx, wf, 12, 1)
		deliver(t, ctx, wf, 5, 2)
		deliver(t, ctx, wf, 12, 3)
		require.True(t, wf.HasPendingNexusProgress(ctx))

		taken := wf.TakeNexusProgress(ctx)
		require.Len(t, taken, 2)
		require.Equal(t, int64(5), scheduledEventID(taken[0]))
		require.Equal(t, &nexuspb.NexusOperationProgress_OperationKey{
			Endpoint:       "endpoint",
			Service:        "service",
			Operation:      "operation",
			OperationToken: "12",
		}, taken[1].GetOperationKey())
		require.Equal(t, map[int64]int64{5: 2, 12: 3}, counters(taken))
		require.False(t, wf.HasPendingNexusProgress(ctx))
	})

	t.Run("ProgressAfterATakeWaitsForTheNextTask", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newWorkflowWithOperations(t, ctx, 5, 9)
		deliver(t, ctx, wf, 5, 1)
		require.Len(t, wf.TakeNexusProgress(ctx), 1)

		deliver(t, ctx, wf, 5, 2)
		deliver(t, ctx, wf, 5, 3)
		deliver(t, ctx, wf, 9, 1)
		require.True(t, wf.HasPendingNexusProgress(ctx))
		require.Equal(t, map[int64]int64{5: 3, 9: 1}, counters(wf.TakeNexusProgress(ctx)),
			"the next task carries the latest progress of each operation")
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
		require.Equal(
			t,
			map[int64]int64{9: 2},
			counters(wf.TakeNexusProgress(ctx)),
			"a closed operation's pending progress is gone",
		)
		wf.removeNexusOperation(ctx, 9)
		state, ok := wf.NexusProgress.TryGet(ctx)
		require.True(t, ok)
		index := state.GetIndex()
		require.Empty(t, index.GetPending())
		require.NotNil(
			t,
			index.GetLastCarriedTime(),
			"only the last carried time stays, for the rate limit",
		)
	})

	t.Run("StoresNoIndexWithoutProgress", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newWorkflowWithOperations(t, ctx, 5)
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
