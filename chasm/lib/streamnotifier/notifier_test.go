package streamnotifier

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	streamnotifierpb "go.temporal.io/server/chasm/lib/streamnotifier/gen/streamnotifierpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
)

type notifierTest struct {
	t       *testing.T
	ctx     context.Context
	handler *handler
	expiry  *expiryTaskHandler
	ref     *streampb.StreamReference
}

func newNotifierTest(t *testing.T, maxCallbacks int) *notifierTest {
	config := &Config{
		Enabled:         dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		MaxCallbacks:    dynamicconfig.GetIntPropertyFnFilteredByNamespace(maxCallbacks),
		IdleTimeout:     dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Hour),
		ClosedRetention: dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Minute),
	}
	h := newHandler(config, log.NewTestLogger())
	expiry := newExpiryTaskHandler(expiryTaskHandlerOptions{Config: config})
	registry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(callback.NewNilLibrary()))
	require.NoError(t, registry.Register(newLibrary(h, expiry)))
	return &notifierTest{
		t:       t,
		ctx:     chasm.NewEngineContext(context.Background(), chasmtest.NewEngine(t, registry)),
		handler: h,
		expiry:  expiry,
		ref: &streampb.StreamReference{
			OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
			WorkflowId: "wf/1",
			Topic:      "tokens",
		},
	}
}

func (nt *notifierTest) key() chasm.ExecutionKey {
	return chasm.ExecutionKey{NamespaceID: "namespace-id", BusinessID: BusinessID(nt.ref)}
}

func (nt *notifierTest) attach(requestID string) error {
	_, err := nt.handler.AttachStreamCallback(nt.ctx, &streamnotifierpb.AttachStreamCallbackRequest{
		NamespaceId: "namespace-id",
		BusinessId:  BusinessID(nt.ref),
		FrontendRequest: &workflowservice.AttachStreamCallbackRequest{
			Namespace: "ns",
			StreamRef: nt.ref,
			RequestId: requestID,
			Callback:  &commonpb.Callback_Nexus{Url: "http://caller/" + requestID},
		},
	})
	return err
}

func (nt *notifierTest) notify(counter int64, closeStream bool) error {
	_, err := nt.handler.NotifyStream(nt.ctx, &streamnotifierpb.NotifyStreamRequest{
		NamespaceId: "namespace-id",
		BusinessId:  BusinessID(nt.ref),
		FrontendRequest: &workflowservice.NotifyStreamRequest{
			Namespace:   "ns",
			StreamRef:   nt.ref,
			Position:    "p",
			Counter:     counter,
			Metadata:    map[string]string{"k": "v"},
			Close:       closeStream,
			CloseResult: map[bool]*commonpb.Payload{true: {Data: []byte("summary")}}[closeStream],
		},
	})
	return err
}

func (nt *notifierTest) describe() *workflowservice.DescribeStreamNotifierResponse {
	resp, err := nt.handler.DescribeStreamNotifier(nt.ctx, &streamnotifierpb.DescribeStreamNotifierRequest{
		NamespaceId:     "namespace-id",
		BusinessId:      BusinessID(nt.ref),
		FrontendRequest: &workflowservice.DescribeStreamNotifierRequest{Namespace: "ns", StreamRef: nt.ref},
	})
	require.NoError(nt.t, err)
	return resp.GetFrontendResponse()
}

// read runs fn against the notifier's committed state.
func (nt *notifierTest) read(fn func(*StreamNotifier, chasm.Context)) {
	_, err := chasm.ReadComponent(nt.ctx, chasm.NewComponentRef[*StreamNotifier](nt.key()),
		func(n *StreamNotifier, ctx chasm.Context, _ struct{}) (struct{}, error) {
			fn(n, ctx)
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(nt.t, err)
}

func (nt *notifierTest) callbacks() map[string]*callbackspb.CallbackState {
	out := map[string]*callbackspb.CallbackState{}
	nt.read(func(n *StreamNotifier, ctx chasm.Context) {
		for id, field := range n.Callbacks {
			out[id] = field.Get(ctx).CallbackState
		}
	})
	return out
}

// expire runs the expiry the latest activity armed.
func (nt *notifierTest) expire() {
	_, _, err := chasm.UpdateComponent(nt.ctx, chasm.NewComponentRef[*StreamNotifier](nt.key()),
		func(n *StreamNotifier, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			task := &streamnotifierpb.ExpiryTask{LastActivityTime: n.GetLastActivityTime()}
			valid, err := nt.expiry.Validate(ctx, n, chasm.TaskInvocation{}, task)
			require.NoError(nt.t, err)
			require.True(nt.t, valid)
			return struct{}{}, nt.expiry.Execute(ctx, n, chasm.TaskAttributes{}, task)
		}, struct{}{})
	require.NoError(nt.t, err)
}

func TestStreamNotifier(t *testing.T) {
	t.Run("NotificationsReachEveryCallbackAsFoldedProgress", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attach("a"))
		require.NoError(t, nt.attach("a"), "attaching again with the request ID changes nothing")
		require.NoError(t, nt.notify(1, false))
		require.NoError(t, nt.notify(3, false))
		require.NoError(t, nt.notify(2, false), "a lower counter is accepted and ignored")
		require.NoError(t, nt.attach("b"))

		cbs := nt.callbacks()
		require.Len(t, cbs, 2)
		require.Equal(t, int64(1), cbs["a"].GetProgressInFlight(), "one delivery in flight")
		require.Equal(t, int64(3), cbs["a"].GetPendingProgress().GetCounter(), "later progress folds into the next one")
		require.Equal(t, int64(3), cbs["b"].GetPendingProgress().GetCounter(), "a late callback starts from the latest notification")
		require.Equal(t, "p", cbs["b"].GetPendingProgress().GetPosition())
		require.Equal(t, map[string]string{"k": "v"}, cbs["b"].GetPendingProgress().GetMetadata())

		desc := nt.describe()
		require.Equal(t, int64(3), desc.GetCounter())
		require.False(t, desc.GetClosed())
		require.Equal(t, []string{"a", "b"}, []string{desc.GetCallbacks()[0].GetRequestId(), desc.GetCallbacks()[1].GetRequestId()})
		require.Equal(t, enumspb.CALLBACK_STATE_STANDBY, desc.GetCallbacks()[0].GetState())
	})

	t.Run("CloseCompletesEveryCallbackAndLateAttachesRightAway", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attach("a"))
		require.NoError(t, nt.notify(1, false))
		require.NoError(t, nt.notify(2, true))
		require.NoError(t, nt.attach("late"))
		cbs := nt.callbacks()
		for id, cb := range cbs {
			require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, cb.GetStatus(), id)
			require.Nil(t, cb.GetPendingProgress(), "the completion never waits for progress")
		}
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			completion, err := n.GetNexusCompletion(ctx, "a")
			require.NoError(t, err)
			require.Equal(t, []byte("summary"), completion.Result.(*commonpb.Payload).GetData())
			require.Nil(t, completion.Error)
		})
		require.NoError(t, nt.notify(3, false), "a notification after close is ignored")
		require.Equal(t, int64(2), nt.describe().GetCounter())
		require.True(t, nt.describe().GetClosed())
	})

	t.Run("DetachForgetsACallback", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attach("a"))
		_, err := nt.handler.DetachStreamCallback(nt.ctx, &streamnotifierpb.DetachStreamCallbackRequest{
			NamespaceId:     "namespace-id",
			BusinessId:      BusinessID(nt.ref),
			FrontendRequest: &workflowservice.DetachStreamCallbackRequest{Namespace: "ns", StreamRef: nt.ref, RequestId: "a"},
		})
		require.NoError(t, err)
		require.Empty(t, nt.callbacks())
		nt.ref = &streampb.StreamReference{OwnerKind: enumspb.STREAM_OWNER_KIND_WORKFLOW, WorkflowId: "other", Topic: "t"}
		_, err = nt.handler.DetachStreamCallback(nt.ctx, &streamnotifierpb.DetachStreamCallbackRequest{
			NamespaceId:     "namespace-id",
			BusinessId:      BusinessID(nt.ref),
			FrontendRequest: &workflowservice.DetachStreamCallbackRequest{Namespace: "ns", StreamRef: nt.ref, RequestId: "a"},
		})
		require.NoError(t, err, "detaching from a stream with no notifier changes nothing")
	})

	t.Run("HoldsAtMostTheCallbackLimit", func(t *testing.T) {
		nt := newNotifierTest(t, 2)
		require.NoError(t, nt.attach("a"))
		require.NoError(t, nt.attach("b"))
		var failed *serviceerror.FailedPrecondition
		require.ErrorAs(t, nt.attach("c"), &failed)
	})

	t.Run("AnIdleStreamClosesWithAFailureThenStopsTakingAttaches", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attach("a"))
		nt.expire()
		require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, nt.callbacks()["a"].GetStatus())
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			completion, err := n.GetNexusCompletion(ctx, "a")
			require.NoError(t, err)
			require.NotNil(t, completion.Error, "an idle close fails the callbacks")
			require.Equal(t, chasm.LifecycleStateRunning, n.LifecycleState(ctx), "late attaches still complete")
		})
		require.NoError(t, nt.attach("late"))

		nt.expire()
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			require.Equal(t, chasm.LifecycleStateCompleted, n.LifecycleState(ctx))
		})
		var failed *serviceerror.FailedPrecondition
		require.ErrorAs(t, nt.attach("too-late"), &failed)
	})

	t.Run("BusinessIDsDoNotCollide", func(t *testing.T) {
		require.NotEqual(t,
			BusinessID(&streampb.StreamReference{WorkflowId: "a/b", Topic: "c"}),
			BusinessID(&streampb.StreamReference{WorkflowId: "a", Topic: "b/c"}),
		)
		require.Equal(t,
			BusinessID(&streampb.StreamReference{WorkflowId: "a", Topic: "t"}),
			BusinessID(&streampb.StreamReference{WorkflowId: "a", RunId: "r", Topic: "t"}),
			"one notifier serves the stream across runs",
		)
	})
}
