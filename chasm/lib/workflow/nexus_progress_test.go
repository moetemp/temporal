package workflow

import (
	"testing"

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
	newWorkflowWithOperations := func(t *testing.T, ctx chasm.MutableContext, keys ...int64) *Workflow {
		wf := &Workflow{}
		for _, key := range keys {
			parentData, err := anypb.New(&chasmworkflowpb.NexusOperationParentData{ScheduledEventId: key})
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
		err := wf.Operations[key].Get(ctx).HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
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

	t.Run("FoldsIntoTheUnstartedTaskUntilItStarts", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newWorkflowWithOperations(t, ctx, 5, 9)
		deliver(t, ctx, wf, 5, 1)
		require.Len(t, wf.TakeNexusProgress(ctx), 1)

		deliver(t, ctx, wf, 5, 2)
		require.False(t, wf.HasPendingNexusProgress(ctx), "operation 5 rides the unstarted task")
		deliver(t, ctx, wf, 9, 1)
		require.True(t, wf.HasPendingNexusProgress(ctx), "operation 9 is not on that task")

		wf.ClearScheduledNexusProgress(ctx)
		require.False(t, wf.HasScheduledNexusProgress(ctx))
		deliver(t, ctx, wf, 5, 3)
		require.Equal(t, map[int64]int64{5: 3, 9: 1}, counters(wf.TakeNexusProgress(ctx)))
	})

	t.Run("ForgetsAClosedOperation", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newWorkflowWithOperations(t, ctx, 5, 9)
		deliver(t, ctx, wf, 5, 1)
		deliver(t, ctx, wf, 9, 1)
		wf.removeNexusOperation(ctx, 5)
		require.Equal(t, map[int64]int64{9: 1}, counters(wf.TakeNexusProgress(ctx)))
		wf.removeNexusOperation(ctx, 9)
		require.False(t, wf.HasScheduledNexusProgress(ctx))
		_, ok := wf.NexusProgress.TryGet(ctx)
		require.False(t, ok, "an empty index is not stored")
	})

	t.Run("DropsPendingProgressAsALivelockGuard", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newWorkflowWithOperations(t, ctx, 5)
		deliver(t, ctx, wf, 5, 1)
		wf.DropPendingNexusProgress(ctx)
		require.False(t, wf.HasPendingNexusProgress(ctx))
		require.Empty(t, wf.TakeNexusProgress(ctx))
	})
}
