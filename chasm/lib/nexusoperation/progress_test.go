package nexusoperation

import (
	"testing"

	"github.com/stretchr/testify/require"
	notificationpb "go.temporal.io/api/notification/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
)

func TestHandleNexusProgress(t *testing.T) {
	progress := func(counter int64) *persistencespb.ChasmNexusCompletion {
		return &persistencespb.ChasmNexusCompletion{
			Outcome: &persistencespb.ChasmNexusCompletion_Progress{
				Progress: &notificationpb.Notification{Counter: counter},
			},
		}
	}
	newOp := func(status nexusoperationpb.OperationStatus, withStore bool) *Operation {
		op := newTestOperation()
		op.Status = status
		if withStore {
			op.Store = chasm.NewMockParentPtr[OperationStore](&mockStoreComponent{})
		}
		return op
	}

	testCases := []struct {
		name      string
		status    nexusoperationpb.OperationStatus
		withStore bool
		counters  []int64
		// Counter of the pending progress after the deliveries, or zero for none.
		wantPending int64
	}{
		{name: "HighestCounterWins", status: nexusoperationpb.OPERATION_STATUS_STARTED, withStore: true, counters: []int64{1, 3, 2}, wantPending: 3},
		{name: "DroppedBeforeStart", status: nexusoperationpb.OPERATION_STATUS_SCHEDULED, withStore: true, counters: []int64{1}},
		{name: "DroppedAfterClose", status: nexusoperationpb.OPERATION_STATUS_SUCCEEDED, withStore: true, counters: []int64{1}},
		{name: "DroppedWithoutCallerWorkflow", status: nexusoperationpb.OPERATION_STATUS_STARTED, counters: []int64{1}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}
			op := newOp(tc.status, tc.withStore)
			for _, counter := range tc.counters {
				require.NoError(t, op.HandleNexusCompletion(ctx, progress(counter)))
			}
			pending, ok := op.PendingProgress.TryGet(ctx)
			require.Equal(t, tc.wantPending != 0, ok)
			require.Equal(t, tc.wantPending, pending.GetCounter())
			require.Equal(t, tc.status, op.Status, "progress never changes the operation status")
		})
	}

	t.Run("StaleAfterDelivery", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		op := newOp(nexusoperationpb.OPERATION_STATUS_STARTED, true)
		require.NoError(t, op.HandleNexusCompletion(ctx, progress(5)))
		taken, ok := op.TakePendingProgress(ctx)
		require.True(t, ok)
		require.Equal(t, int64(5), taken.GetCounter())
		require.False(t, op.HasPendingProgress(ctx))

		require.NoError(t, op.HandleNexusCompletion(ctx, progress(4)))
		require.False(t, op.HasPendingProgress(ctx), "a counter at or below the delivered one is stale")
		require.NoError(t, op.HandleNexusCompletion(ctx, progress(6)))
		require.True(t, op.HasPendingProgress(ctx))
	})
}
