package nexusoperation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		{
			name:        "HighestCounterWins",
			status:      nexusoperationpb.OPERATION_STATUS_STARTED,
			store:       &progressStoreComponent{},
			counters:    []int64{1, 3, 2},
			wantPending: 3,
		},
		{
			name:     "DroppedBeforeStart",
			status:   nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			store:    &progressStoreComponent{},
			counters: []int64{1},
		},
		{
			name:     "DroppedAfterClose",
			status:   nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
			store:    &progressStoreComponent{},
			counters: []int64{1},
		},
		{
			name:     "DroppedByAParentThatTakesNoProgress",
			status:   nexusoperationpb.OPERATION_STATUS_STARTED,
			store:    &mockStoreComponent{},
			counters: []int64{1},
		},
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

func newRelayCallback(cb *callbackspb.Callback) *callback.Callback {
	return callback.NewCallback("request-id", timestamppb.New(time.Now()), cb)
}

func nexusRelayCallback() *callbackspb.Callback {
	return &callbackspb.Callback{Variant: &callbackspb.Callback_Nexus_{
		Nexus: &callbackspb.Callback_Nexus{Url: "http://localhost/callback"},
	}}
}

func nexusHandlerRelayCallback(progressOperation string) *callbackspb.Callback {
	return &callbackspb.Callback{Variant: &callbackspb.Callback_NexusHandler_{
		NexusHandler: &callbackspb.Callback_NexusHandler{
			TaskQueueName:     "tq",
			Service:           "NotificationService",
			Operation:         "OnComplete",
			ProgressOperation: progressOperation,
		},
	}}
}

func TestStandaloneOperationRelaysProgress(t *testing.T) {
	progress := func(counter int64) *persistencespb.ChasmNexusCompletion {
		return &persistencespb.ChasmNexusCompletion{
			Outcome: &persistencespb.ChasmNexusCompletion_Progress{
				Progress: &nexuspb.NexusOperationProgress{Counter: counter},
			},
		}
	}
	newStandalone := func(
		ctx chasm.MutableContext,
		status nexusoperationpb.OperationStatus,
		callbacks ...*callback.Callback,
	) *Operation {
		op := newTestOperation()
		op.Status = status
		op.Callbacks = make(chasm.Map[string, *callback.Callback], len(callbacks))
		for i, cb := range callbacks {
			op.Callbacks[completionCallbackID("request-id", i)] = chasm.NewComponentField(ctx, cb)
		}
		return op
	}
	progressTasks := func(ctx *chasm.MockMutableContext) int {
		n := 0
		for _, task := range ctx.Tasks {
			if _, ok := task.Payload.(*callbackspb.ProgressTask); ok {
				n++
			}
		}
		return n
	}

	t.Run("RelaysToEachCallback", func(t *testing.T) {
		ctx := newCallbackTestContext()
		nexusCb := newRelayCallback(nexusRelayCallback())
		handlerCb := newRelayCallback(nexusHandlerRelayCallback("OnProgress"))
		op := newStandalone(ctx, nexusoperationpb.OPERATION_STATUS_STARTED, nexusCb, handlerCb)
		require.NoError(t, op.HandleNexusCompletion(ctx, progress(1)))
		require.NoError(t, op.HandleNexusCompletion(ctx, progress(2)))
		for _, cb := range []*callback.Callback{nexusCb, handlerCb} {
			require.Equal(t, int64(1), cb.GetProgressInFlight(), "each callback has a delivery")
			require.Equal(t, int64(2), cb.GetPendingProgress().GetCounter(), "and folds on its own")
		}
		require.Equal(t, 2, progressTasks(ctx))
		_, ok := op.PendingProgress.TryGet(ctx)
		require.False(t, ok, "a started operation keeps no progress of its own")
	})

	t.Run("ProgressBeforeTheStartReachesTheCallbacksAfterIt", func(t *testing.T) {
		ctx := newCallbackTestContext()
		handlerCb := newRelayCallback(nexusHandlerRelayCallback("OnProgress"))
		op := newStandalone(ctx, nexusoperationpb.OPERATION_STATUS_SCHEDULED, handlerCb)
		require.NoError(t, op.HandleNexusCompletion(ctx, progress(3)))
		require.NoError(t, op.HandleNexusCompletion(ctx, progress(2)))
		require.Zero(t, progressTasks(ctx))
		require.Nil(t, handlerCb.GetPendingProgress())

		require.NoError(t, op.onStarted(ctx, "handler-token", nil, nil))
		require.Equal(t, nexusoperationpb.OPERATION_STATUS_STARTED, op.Status)
		require.Equal(t, int64(3), handlerCb.GetPendingProgress().GetCounter())
		require.Equal(t, 1, progressTasks(ctx))
		_, ok := op.PendingProgress.TryGet(ctx)
		require.False(t, ok)
	})

	for _, tc := range []struct {
		name      string
		callbacks func() []*callback.Callback
	}{
		{name: "NoCallback", callbacks: func() []*callback.Callback { return nil }},
		{
			name: "ACompletionOnlyCallback",
			callbacks: func() []*callback.Callback {
				return []*callback.Callback{newRelayCallback(nexusHandlerRelayCallback(""))}
			},
		},
		{
			name: "ACallbackThatTurnedProgressOff",
			callbacks: func() []*callback.Callback {
				cb := newRelayCallback(nexusRelayCallback())
				cb.ProgressDisabled = true
				return []*callback.Callback{cb}
			},
		},
	} {
		t.Run("RefusesWith"+tc.name, func(t *testing.T) {
			for _, status := range []nexusoperationpb.OperationStatus{
				nexusoperationpb.OPERATION_STATUS_SCHEDULED,
				nexusoperationpb.OPERATION_STATUS_STARTED,
			} {
				ctx := newCallbackTestContext()
				op := newStandalone(ctx, status, tc.callbacks()...)
				err := op.HandleNexusCompletion(ctx, progress(1))
				var failedPrecondition *serviceerror.FailedPrecondition
				require.ErrorAs(t, err, &failedPrecondition)
				require.Zero(t, progressTasks(ctx))
			}
		})
	}

	t.Run("DroppedAfterClose", func(t *testing.T) {
		ctx := newCallbackTestContext()
		handlerCb := newRelayCallback(nexusHandlerRelayCallback("OnProgress"))
		op := newStandalone(ctx, nexusoperationpb.OPERATION_STATUS_SUCCEEDED, handlerCb)
		require.NoError(t, op.HandleNexusCompletion(ctx, progress(1)))
		require.Zero(t, progressTasks(ctx))
	})

	t.Run("GivesItsHandlerTokenToCallbacks", func(t *testing.T) {
		op := newTestOperation()
		op.OperationToken = "handler-token"
		source, ok := any(op).(callback.HandlerTokenSource)
		require.True(t, ok, "a callback's parent operation has a handler token")
		require.Equal(t, "handler-token", source.HandlerToken(newCallbackTestContext()))
	})
}
