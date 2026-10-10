package nexusoperation

import (
	"testing"

	"github.com/stretchr/testify/require"
	nexuspb "go.temporal.io/api/nexus/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
)

// progressStoreComponent is a parent that takes progress.
type progressStoreComponent struct {
	mockStoreComponent

	pending int
}

func (s *progressStoreComponent) OnNexusOperationProgress(chasm.MutableContext, *Operation) error {
	s.pending++
	return nil
}

func TestHandleNexusProgress(t *testing.T) {
	progress := func(counter int64) *persistencespb.ChasmNexusCompletion {
		return &persistencespb.ChasmNexusCompletion{
			Outcome: &persistencespb.ChasmNexusCompletion_Progress{
				Progress: &nexuspb.NexusOperationProgress{Counter: counter},
			},
		}
	}
	newOp := func(status nexusoperationpb.OperationStatus, store OperationStore) *Operation {
		op := newTestOperation()
		op.Status = status
		if store != nil {
			op.Store = chasm.NewMockParentPtr(store)
		}
		return op
	}
	pendingCounter := func(ctx chasm.Context, op *Operation) int64 {
		p, _ := op.PendingProgress.TryGet(ctx)
		return p.GetCounter()
	}
	deliveredCounter := func(ctx chasm.Context, op *Operation) int64 {
		p, _ := op.DeliveredProgress.TryGet(ctx)
		return p.GetCounter()
	}

	for _, tc := range []struct {
		name        string
		status      nexusoperationpb.OperationStatus
		store       OperationStore
		counters    []int64
		wantPending int64
	}{
		{name: "HighestCounterWins", status: nexusoperationpb.OPERATION_STATUS_STARTED, store: &progressStoreComponent{}, counters: []int64{1, 3, 2}, wantPending: 3},
		{name: "DroppedBeforeStart", status: nexusoperationpb.OPERATION_STATUS_SCHEDULED, store: &progressStoreComponent{}, counters: []int64{1}},
		{name: "DroppedAfterClose", status: nexusoperationpb.OPERATION_STATUS_SUCCEEDED, store: &progressStoreComponent{}, counters: []int64{1}},
		{name: "DroppedWithoutCaller", status: nexusoperationpb.OPERATION_STATUS_STARTED, counters: []int64{1}},
		{name: "DroppedByAParentThatTakesNoProgress", status: nexusoperationpb.OPERATION_STATUS_STARTED, store: &mockStoreComponent{}, counters: []int64{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}
			op := newOp(tc.status, tc.store)
			for _, counter := range tc.counters {
				require.NoError(t, op.HandleNexusCompletion(ctx, progress(counter)))
			}
			require.Equal(t, tc.wantPending, pendingCounter(ctx, op))
			require.Equal(t, tc.status, op.Status, "progress never changes the operation status")
		})
	}

	t.Run("StaleAfterDelivery", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		store := &progressStoreComponent{}
		op := newOp(nexusoperationpb.OPERATION_STATUS_STARTED, store)
		require.NoError(t, op.HandleNexusCompletion(ctx, progress(5)))
		taken, ok := op.TakePendingProgress(ctx)
		require.True(t, ok)
		require.Equal(t, int64(5), taken.GetCounter())
		require.Zero(t, pendingCounter(ctx, op))

		require.NoError(t, op.HandleNexusCompletion(ctx, progress(4)))
		require.Zero(t, pendingCounter(ctx, op), "a counter at or below the delivered one is stale")
		require.NoError(t, op.HandleNexusCompletion(ctx, progress(6)))
		require.Equal(t, int64(6), pendingCounter(ctx, op))
		require.Equal(t, 2, store.pending)
	})

	t.Run("NewerProgressWaitsUntilATaskCarriesIt", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		store := &progressStoreComponent{}
		op := newOp(nexusoperationpb.OPERATION_STATUS_STARTED, store)
		require.NoError(t, op.HandleNexusCompletion(ctx, progress(1)))
		_, ok := op.TakePendingProgress(ctx)
		require.True(t, ok)

		// Whether it folds into a task that has not started is the parent's call; the operation
		// keeps it until a task carries it.
		require.NoError(t, op.HandleNexusCompletion(ctx, progress(2)))
		require.Equal(t, int64(2), pendingCounter(ctx, op))
		require.Equal(t, int64(1), deliveredCounter(ctx, op))
		require.Equal(t, 2, store.pending)
	})

	t.Run("DropPendingProgress", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		op := newOp(nexusoperationpb.OPERATION_STATUS_STARTED, &progressStoreComponent{})
		require.NoError(t, op.HandleNexusCompletion(ctx, progress(1)))
		op.DropPendingProgress(ctx)
		require.Zero(t, pendingCounter(ctx, op))
		require.Zero(t, deliveredCounter(ctx, op), "dropped progress is not delivered")
	})
}
