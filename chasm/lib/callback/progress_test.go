package callback

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type progressTest struct {
	t       *testing.T
	handler *progressTaskHandler
	ctx     context.Context
	ref     chasm.ComponentRef
	// sent holds the progress of each delivery, in order.
	sent []*callbackspb.Progress
	// answer is how the receiver answers the next delivery.
	answer progressResult
	// during runs while a delivery is on the wire.
	during func()
}

// newProgressTest gives the Nexus variant a fake progress delivery, so the state machine can be
// tested apart from any receiver.
func newProgressTest(t *testing.T, cb *Callback) *progressTest {
	pt := &progressTest{t: t, answer: progressDelivered}
	saved := progressDeliveryFor
	t.Cleanup(func() { progressDeliveryFor = saved })
	progressDeliveryFor = func(c *callbackspb.Callback) progressDelivery {
		if c.GetNexus() == nil {
			return nil
		}
		return pt.deliver
	}

	ctrl := gomock.NewController(t)
	nsRegistry := namespace.NewMockRegistry(ctrl)
	ns := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "ns"}, nil, "active")
	nsRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil).AnyTimes()
	pt.handler = newProgressTaskHandler(invocationTaskHandlerOptions{
		Config: &Config{
			RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
			RetryPolicy: func() backoff.RetryPolicy {
				return backoff.NewExponentialRetryPolicy(time.Second)
			},
			ProgressBehindInterval: func(string) time.Duration { return time.Second },
		},
		NamespaceRegistry: nsRegistry,
		MetricsHandler:    metrics.NoopMetricsHandler,
		Logger:            log.NewTestLogger(),
	})
	pt.ctx, pt.ref = newInvocationTaskTest(
		t,
		pt.handler.invocation,
		cb,
		nexusrpc.CompleteOperationOptions{},
	)
	return pt
}

func (pt *progressTest) deliver(
	_ context.Context,
	_ *progressTaskHandler,
	_ chasm.ComponentRef,
	invocation progressInvocation,
) (progressResult, error) {
	pt.sent = append(pt.sent, invocation.progress)
	if pt.during != nil {
		pt.during()
		pt.during = nil
	}
	if pt.answer == progressDelivered {
		return progressDelivered, nil
	}
	return pt.answer, errors.New("receiver answered with an error")
}

func (pt *progressTest) report(counter int64) {
	pt.t.Helper()
	_, _, err := chasm.UpdateComponent(
		pt.ctx,
		pt.ref,
		func(c *Callback, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			return struct{}{}, c.DeliverProgress(ctx, &callbackspb.Progress{
				Position: "p",
				Counter:  counter,
				Metadata: map[string]string{"topic": "t"},
			})
		},
		struct{}{},
	)
	require.NoError(pt.t, err)
}

func (pt *progressTest) state() *callbackspb.CallbackState {
	pt.t.Helper()
	var state *callbackspb.CallbackState
	readCallbackState(pt.ctx, pt.t, pt.ref, func(_ chasm.Context, c *Callback) {
		state = c.CallbackState
	})
	return state
}

// runInFlight runs the progress task for the delivery in flight, as the outbound queue would.
func (pt *progressTest) runInFlight() error {
	pt.t.Helper()
	state := pt.state()
	task := &callbackspb.ProgressTask{
		Delivery: state.GetProgressInFlight(),
		Attempt:  state.GetProgressAttempt(),
	}
	var valid bool
	readCallbackState(pt.ctx, pt.t, pt.ref, func(ctx chasm.Context, c *Callback) {
		var err error
		valid, err = pt.handler.Validate(ctx, c, chasm.TaskInvocation{}, task)
		require.NoError(pt.t, err)
	})
	require.True(pt.t, valid, "the delivery in flight has a valid task")
	return pt.handler.Execute(
		pt.ctx,
		pt.ref,
		chasm.TaskAttributes{Destination: "http://localhost"},
		task,
	)
}

func newNexusCallback(url string, header map[string]string) *Callback {
	return &Callback{
		CallbackState: &callbackspb.CallbackState{
			RequestId:        "request-id",
			RegistrationTime: timestamppb.New(time.Now()),
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{Url: url, Header: header},
				},
			},
			Status: callbackspb.CALLBACK_STATUS_STANDBY,
		},
	}
}

func TestProgressDelivery(t *testing.T) {
	t.Run("OneInFlightAndABurstFoldsIntoTheNext", func(t *testing.T) {
		pt := newProgressTest(t, newNexusCallback("http://localhost/callback", nil))
		pt.report(1)
		pt.report(3)
		pt.report(2) // stale behind 3
		state := pt.state()
		require.Equal(t, int64(1), state.GetProgressInFlightCounter(), "one delivery in flight")
		require.Equal(t, int64(3), state.GetPendingProgress().GetCounter(),
			"later progress waits, folded")

		require.NoError(t, pt.runInFlight())
		require.Len(t, pt.sent, 1)
		require.Equal(t, int64(3), pt.sent[0].GetCounter(),
			"the delivery carries the newest pending progress")
		require.Equal(t, "p", pt.sent[0].GetPosition())
		require.Equal(t, map[string]string{"topic": "t"}, pt.sent[0].GetMetadata())
		state = pt.state()
		require.Equal(t, int64(3), state.GetDeliveredProgressCounter())
		require.Zero(t, state.GetProgressInFlight())
		require.Nil(t, state.GetPendingProgress())

		pt.report(3)
		require.Zero(t, pt.state().GetProgressInFlight(),
			"a counter already delivered starts nothing")
	})

	t.Run("ProgressDuringADeliveryGetsAnotherOne", func(t *testing.T) {
		pt := newProgressTest(t, newNexusCallback("http://localhost/callback", nil))
		pt.report(1)
		pt.during = func() { pt.report(5) }
		require.NoError(t, pt.runInFlight())
		state := pt.state()
		require.Equal(t, int64(1), state.GetDeliveredProgressCounter())
		require.Equal(t, int64(5), state.GetProgressInFlightCounter(),
			"the newer progress is the next delivery")
		require.NoError(t, pt.runInFlight())
		require.Equal(t, int64(5), pt.sent[1].GetCounter())
		require.Zero(t, pt.state().GetProgressInFlight())
	})

	t.Run("ARefusalTurnsProgressOff", func(t *testing.T) {
		pt := newProgressTest(t, newNexusCallback("http://localhost/callback", nil))
		pt.report(1)
		pt.answer = progressRefused
		require.NoError(t, pt.runInFlight())
		state := pt.state()
		require.True(t, state.GetProgressDisabled())
		require.False(t, state.GetReceiverClosed())
		require.Zero(t, state.GetProgressInFlight())
		pt.report(2)
		require.Nil(t, pt.state().GetPendingProgress(), "progress stays off for the callback")
		require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, pt.state().GetStatus(),
			"the completion is still delivered")
	})

	t.Run("AClosedReceiverMarksTheCallback", func(t *testing.T) {
		pt := newProgressTest(t, newNexusCallback("http://localhost/callback", nil))
		pt.report(1)
		pt.answer = progressReceiverClosed
		require.NoError(t, pt.runInFlight())
		state := pt.state()
		require.True(t, state.GetProgressDisabled())
		require.True(t, state.GetReceiverClosed(), "the parent may drop the callback")
	})

	t.Run("ARetryTakesTheNewestProgress", func(t *testing.T) {
		pt := newProgressTest(t, newNexusCallback("http://localhost/callback", nil))
		pt.report(1)
		pt.answer = progressRetry
		require.NoError(t, pt.runInFlight())
		state := pt.state()
		require.False(t, state.GetProgressDisabled())
		require.Equal(t, int64(1), state.GetProgressInFlightCounter())
		require.Equal(t, int32(1), state.GetProgressAttempt())

		pt.report(2)
		pt.answer = progressDelivered
		require.NoError(t, pt.runInFlight())
		require.Equal(t, int64(2), pt.sent[1].GetCounter())
		require.Equal(t, int64(2), pt.state().GetDeliveredProgressCounter())
	})

	t.Run("TheCompletionNeverWaits", func(t *testing.T) {
		pt := newProgressTest(t, newNexusCallback("http://localhost/callback", nil))
		pt.report(1)
		pt.during = func() {
			_, _, err := chasm.UpdateComponent(
				pt.ctx,
				pt.ref,
				func(c *Callback, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
					return struct{}{}, TransitionScheduled.Apply(c, ctx, EventScheduled{})
				},
				struct{}{},
			)
			require.NoError(t, err)
		}
		task := &callbackspb.ProgressTask{Delivery: pt.state().GetProgressInFlight()}
		require.NoError(t, pt.handler.Execute(
			pt.ctx, pt.ref, chasm.TaskAttributes{Destination: "http://localhost"}, task))
		state := pt.state()
		require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, state.GetStatus())
		require.Zero(t, state.GetProgressInFlight(),
			"a result after the completion was scheduled is ignored")
		require.Nil(t, state.GetPendingProgress())
		pt.report(2)
		require.Nil(t, pt.state().GetPendingProgress(), "progress after the completion is dropped")
	})

	t.Run("AVariantWithNoDeliveryTakesNoProgress", func(t *testing.T) {
		cb := newNexusCallback("http://localhost/callback", nil)
		cb.Callback.Variant = &callbackspb.Callback_NexusHandler_{
			NexusHandler: &callbackspb.Callback_NexusHandler{TaskQueueName: "tq"},
		}
		pt := newProgressTest(t, cb)
		pt.report(1)
		require.Nil(t, pt.state().GetPendingProgress())
		require.Zero(t, pt.state().GetProgressInFlight())
	})
}

func TestWhichVariantsTakeProgress(t *testing.T) {
	require.Nil(t, variantProgressDelivery(&callbackspb.Callback{
		Variant: &callbackspb.Callback_NexusHandler_{
			NexusHandler: &callbackspb.Callback_NexusHandler{TaskQueueName: "tq"},
		},
	}), "a worker-hosted handler takes progress only when it names an operation for it")
	for _, cb := range []*callbackspb.Callback{
		newNexusCallback("http://localhost/callback", nil).GetCallback(),
		newNexusHandlerProgressCallback("OnProgress").GetCallback(),
		{Variant: &callbackspb.Callback_StreamWake_{StreamWake: &callbackspb.Callback_StreamWake{}}},
	} {
		require.NotNil(t, variantProgressDelivery(cb))
	}
}

func TestProgressRecheck(t *testing.T) {
	recheck := func(pt *progressTest, counter int64) {
		pt.t.Helper()
		_, _, err := chasm.UpdateComponent(
			pt.ctx,
			pt.ref,
			func(c *Callback, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
				return struct{}{}, c.DeliverProgress(ctx, &callbackspb.Progress{
					Counter: counter,
					Recheck: true,
				})
			},
			struct{}{},
		)
		require.NoError(pt.t, err)
	}

	t.Run("ARecheckGoesOutWithNoNewCounter", func(t *testing.T) {
		pt := newProgressTest(t, newNexusCallback("http://localhost/callback", nil))
		pt.report(3)
		require.NoError(t, pt.runInFlight())
		recheck(pt, 3)
		require.NotZero(t, pt.state().GetProgressInFlight())
		require.NoError(t, pt.runInFlight())
		require.Len(t, pt.sent, 2)
		require.True(t, pt.sent[1].GetRecheck())
		require.Equal(t, int64(3), pt.sent[1].GetCounter())
		require.Zero(t, pt.state().GetProgressInFlight(), "the recheck asks for nothing more")
	})

	t.Run("ARecheckOnAnEmptyStreamIsDelivered", func(t *testing.T) {
		pt := newProgressTest(t, newNexusCallback("http://localhost/callback", nil))
		recheck(pt, 0)
		require.NotZero(t, pt.state().GetProgressInFlight(),
			"a delivery in flight is told apart from none even at counter zero")
		require.NoError(t, pt.runInFlight())
		require.True(t, pt.sent[0].GetRecheck())
		require.Zero(t, pt.state().GetProgressInFlight())
	})

	t.Run("ARecheckDuringADeliveryGetsAnotherOne", func(t *testing.T) {
		pt := newProgressTest(t, newNexusCallback("http://localhost/callback", nil))
		pt.report(1)
		pt.during = func() { recheck(pt, 1) }
		require.NoError(t, pt.runInFlight())
		require.NotZero(t, pt.state().GetProgressInFlight())
		require.NoError(t, pt.runInFlight())
		require.True(t, pt.sent[1].GetRecheck())
		require.Zero(t, pt.state().GetProgressInFlight())
	})

	t.Run("ARecheckFoldsIntoNewerProgress", func(t *testing.T) {
		pt := newProgressTest(t, newNexusCallback("http://localhost/callback", nil))
		pt.report(1)
		recheck(pt, 1)
		pt.report(2)
		require.NoError(t, pt.runInFlight())
		require.Len(t, pt.sent, 1)
		require.Equal(t, int64(2), pt.sent[0].GetCounter())
		require.True(t, pt.sent[0].GetRecheck(), "one delivery carries both")
		require.Zero(t, pt.state().GetProgressInFlight())
	})
}
