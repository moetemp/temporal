package streamnotifier

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	streamnotifierpb "go.temporal.io/server/chasm/lib/streamnotifier/gen/streamnotifierpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type notifierTest struct {
	t          *testing.T
	ctx        context.Context
	handler    *handler
	expiry     *expiryTaskHandler
	ownerCheck *ownerCheckTaskHandler
	history    *historyservicemock.MockHistoryServiceClient
	ref        *streampb.StreamReference
	// ownerFirstRun is the first run of the owner's chain as an attach sees it; empty means the
	// owner does not exist.
	ownerFirstRun string
}

func newNotifierTest(t *testing.T, maxCallbacks int) *notifierTest {
	config := &Config{
		Enabled:            dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		MaxCallbacks:       dynamicconfig.GetIntPropertyFnFilteredByNamespace(maxCallbacks),
		IdleTimeout:        dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Hour),
		ClosedRetention:    dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Minute),
		OwnerCheckInterval: dynamicconfig.GetDurationPropertyFnFilteredByNamespace(5 * time.Minute),
		MaxIDLength:        dynamicconfig.GetIntPropertyFn(1000),
	}
	ctrl := gomock.NewController(t)
	history := historyservicemock.NewMockHistoryServiceClient(ctrl)
	registryMock := namespace.NewMockRegistry(ctrl)
	registryMock.EXPECT().GetNamespaceByID(gomock.Any()).Return(
		namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: "namespace-id", Name: "ns"}, nil, "active"), nil,
	).AnyTimes()
	nt := &notifierTest{}
	attachHistory := historyservicemock.NewMockHistoryServiceClient(ctrl)
	attachHistory.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *historyservice.DescribeWorkflowExecutionRequest, ...grpc.CallOption) (*historyservice.DescribeWorkflowExecutionResponse, error) {
			if nt.ownerFirstRun == "" {
				return nil, serviceerror.NewNotFound("workflow not found")
			}
			return &historyservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{FirstRunId: nt.ownerFirstRun},
			}, nil
		}).AnyTimes()
	h := newHandler(config, log.NewTestLogger(), attachHistory)
	expiry := newExpiryTaskHandler(expiryTaskHandlerOptions{Config: config})
	ownerCheck := newOwnerCheckTaskHandler(ownerCheckTaskHandlerOptions{
		Config:            config,
		HistoryClient:     history,
		NamespaceRegistry: registryMock,
	})
	registry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(callback.NewNilLibrary()))
	require.NoError(t, registry.Register(newLibrary(h, expiry, ownerCheck)))
	*nt = notifierTest{
		t:          t,
		ctx:        chasm.NewEngineContext(context.Background(), chasmtest.NewEngine(t, registry)),
		handler:    h,
		expiry:     expiry,
		ownerCheck: ownerCheck,
		history:    history,
		ref: &streampb.StreamReference{
			OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
			WorkflowId: "wf/1",
			Topic:      "tokens",
		},
	}
	return nt
}

func (nt *notifierTest) key() chasm.ExecutionKey {
	return chasm.ExecutionKey{NamespaceID: "namespace-id", BusinessID: BusinessID(nt.ref)}
}

// attachStarted attaches a callback with the start the handler answered its caller with.
func (nt *notifierTest) attachStarted(requestID, token string, start time.Time) error {
	_, err := nt.handler.AttachStreamCallback(nt.ctx, &streamnotifierpb.AttachStreamCallbackRequest{
		NamespaceId: "namespace-id",
		BusinessId:  BusinessID(nt.ref),
		FrontendRequest: &workflowservice.AttachStreamCallbackRequest{
			Namespace:      "ns",
			StreamRef:      nt.ref,
			RequestId:      requestID,
			Callback:       &commonpb.Callback_Nexus{Url: "http://caller/" + requestID},
			OperationToken: token,
			StartTime:      timestamppb.New(start),
		},
	})
	return err
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

// updateCallback changes one callback's state, standing in for its delivery tasks.
func (nt *notifierTest) updateCallback(requestID string, fn func(*callback.Callback)) {
	_, _, err := chasm.UpdateComponent(nt.ctx, chasm.NewComponentRef[*StreamNotifier](nt.key()),
		func(n *StreamNotifier, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			fn(n.Callbacks[requestID].Get(ctx))
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(nt.t, err)
}

// runOwnerCheck runs the armed owner check, with History describing the owner as answered.
func (nt *notifierTest) runOwnerCheck(status enumspb.WorkflowExecutionStatus, err error) {
	nt.runOwnerCheckOf(status, nt.ref.GetRunId(), err)
}

// runOwnerCheckOf is runOwnerCheck with the described run's chain named by its first run ID.
func (nt *notifierTest) runOwnerCheckOf(status enumspb.WorkflowExecutionStatus, firstRunID string, err error) {
	var armed *streamnotifierpb.OwnerCheckTask
	nt.read(func(n *StreamNotifier, _ chasm.Context) {
		require.NotNil(nt.t, n.GetOwnerCheckTime(), "an owner check is armed")
		armed = &streamnotifierpb.OwnerCheckTask{ScheduledTime: n.GetOwnerCheckTime()}
	})
	nt.history.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *historyservice.DescribeWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.DescribeWorkflowExecutionResponse, error) {
			require.Equal(nt.t, nt.ref.GetWorkflowId(), req.GetRequest().GetExecution().GetWorkflowId())
			require.Empty(nt.t, req.GetRequest().GetExecution().GetRunId(), "the chain's current run is described")
			if err != nil {
				return nil, err
			}
			return &historyservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: status, FirstRunId: firstRunID},
			}, nil
		})
	ref := chasm.NewComponentRef[*StreamNotifier](nt.key())
	require.NoError(nt.t, nt.ownerCheck.Execute(nt.ctx, ref, chasm.TaskAttributes{}, armed))
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

	t.Run("DetachCompletesTheCallbackAsCanceled", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attach("a"))
		require.NoError(t, nt.attach("b"))
		_, err := nt.handler.DetachStreamCallback(nt.ctx, &streamnotifierpb.DetachStreamCallbackRequest{
			NamespaceId:     "namespace-id",
			BusinessId:      BusinessID(nt.ref),
			FrontendRequest: &workflowservice.DetachStreamCallbackRequest{Namespace: "ns", StreamRef: nt.ref, RequestId: "a"},
		})
		require.NoError(t, err)
		require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, nt.callbacks()["a"].GetStatus(), "the canceled caller gets a completion")
		require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, nt.callbacks()["b"].GetStatus())
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			completion, err := n.GetNexusCompletion(ctx, "a")
			require.NoError(t, err)
			require.NotNil(t, completion.Error)
			require.Equal(t, nexus.OperationStateCanceled, completion.Error.State)
		})
		nt.ref = &streampb.StreamReference{OwnerKind: enumspb.STREAM_OWNER_KIND_WORKFLOW, WorkflowId: "other", Topic: "t"}
		_, err = nt.handler.DetachStreamCallback(nt.ctx, &streamnotifierpb.DetachStreamCallbackRequest{
			NamespaceId:     "namespace-id",
			BusinessId:      BusinessID(nt.ref),
			FrontendRequest: &workflowservice.DetachStreamCallbackRequest{Namespace: "ns", StreamRef: nt.ref, RequestId: "a"},
		})
		require.NoError(t, err, "detaching from a stream with no notifier changes nothing")
	})

	t.Run("TheCallersStartRidesItsProgressAndCompletion", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		start := time.Unix(1700000000, 0).UTC()
		require.NoError(t, nt.attachStarted("a", "token-a", start))
		require.NoError(t, nt.notify(1, true))
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			progress, err := n.GetNexusProgressOptions(ctx, "a")
			require.NoError(t, err)
			require.Equal(t, "token-a", progress.OperationToken)
			require.Equal(t, start, progress.StartTime)
			completion, err := n.GetNexusCompletion(ctx, "a")
			require.NoError(t, err)
			require.Equal(t, "token-a", completion.OperationToken, "a completion before the start response still names the operation")
			require.Equal(t, start, completion.StartTime)
		})
	})

	t.Run("AnAttachNamingAnotherRunThanTheChainsFirstIsRefused", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		nt.ref = &streampb.StreamReference{OwnerKind: enumspb.STREAM_OWNER_KIND_WORKFLOW, WorkflowId: "wf", RunId: "a-later-run", Topic: "t"}
		nt.ownerFirstRun = "first-run"
		var invalid *serviceerror.InvalidArgument
		require.ErrorAs(t, nt.attach("a"), &invalid, "a run that is not the chain's first would key another notifier")
		nt.ref.RunId = "first-run"
		require.NoError(t, nt.attach("a"))
	})

	t.Run("AReusedWorkflowIDGetsAFreshNotifier", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		nt.ref = &streampb.StreamReference{OwnerKind: enumspb.STREAM_OWNER_KIND_WORKFLOW, WorkflowId: "wf", RunId: "first-chain", Topic: "t"}
		require.NoError(t, nt.notify(1, true))
		require.True(t, nt.describe().GetClosed())
		nt.ref = &streampb.StreamReference{OwnerKind: enumspb.STREAM_OWNER_KIND_WORKFLOW, WorkflowId: "wf", RunId: "second-chain", Topic: "t"}
		require.NoError(t, nt.attach("a"))
		require.False(t, nt.describe().GetClosed(), "the later chain's stream is not the closed one")
		require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, nt.callbacks()["a"].GetStatus())
	})

	t.Run("AnOwnerIDReusedByAnotherChainCountsAsEnded", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		nt.ref = &streampb.StreamReference{OwnerKind: enumspb.STREAM_OWNER_KIND_WORKFLOW, WorkflowId: "wf", RunId: "first-chain", Topic: "t"}
		require.NoError(t, nt.attach("a"))
		nt.runOwnerCheckOf(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, "second-chain", nil)
		require.True(t, nt.describe().GetClosed())
	})

	t.Run("HoldsAtMostTheCallbackLimit", func(t *testing.T) {
		nt := newNotifierTest(t, 2)
		require.NoError(t, nt.attach("a"))
		require.NoError(t, nt.attach("b"))
		var failed *serviceerror.FailedPrecondition
		require.ErrorAs(t, nt.attach("c"), &failed)
	})

	t.Run("AnIdleTimeoutFailsTheCallersAndKeepsTheStreamOpen", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attach("a"))
		nt.expire()
		require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, nt.callbacks()["a"].GetStatus())
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			completion, err := n.GetNexusCompletion(ctx, "a")
			require.NoError(t, err)
			require.NotNil(t, completion.Error, "the idle timeout fails the waiting callers")
			require.False(t, n.GetClosed(), "the stream stays open")
		})

		require.NoError(t, nt.notify(1, false), "the producer goes on")
		require.NoError(t, nt.attach("b"))
		require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, nt.callbacks()["b"].GetStatus())
		require.Equal(t, int64(1), nt.callbacks()["b"].GetPendingProgress().GetCounter())
		require.NoError(t, nt.notify(2, true))
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			completion, err := n.GetNexusCompletion(ctx, "b")
			require.NoError(t, err)
			require.Nil(t, completion.Error, "a later caller gets the close result")
		})
	})

	t.Run("AnIdleNotifierWithNoCallersEndsAndANewOneStarts", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.notify(1, false))
		nt.expire()
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			require.Equal(t, chasm.LifecycleStateFailed, n.LifecycleState(ctx))
		})
		require.NoError(t, nt.attach("later"), "a later attach starts a new notifier")
		require.Contains(t, nt.callbacks(), "later")
	})

	t.Run("AClosedStreamStaysClosed", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.notify(1, true))
		nt.expire()
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			require.Equal(t, chasm.LifecycleStateCompleted, n.LifecycleState(ctx))
		})
		var failed *serviceerror.FailedPrecondition
		require.ErrorAs(t, nt.attach("too-late"), &failed)
	})

	t.Run("ACallbackWhoseCallerClosedIsDropped", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attach("a"))
		require.NoError(t, nt.attach("b"))
		nt.updateCallback("a", func(cb *callback.Callback) { cb.CallerOperationClosed = true })
		require.NoError(t, nt.notify(1, false))
		require.NotContains(t, nt.callbacks(), "a")
		require.Contains(t, nt.callbacks(), "b")
	})

	t.Run("AFullNotifierNeverDropsACallerThatStillWaits", func(t *testing.T) {
		nt := newNotifierTest(t, 100)
		for i := range 100 {
			require.NoError(t, nt.attach(fmt.Sprintf("caller-%d", i)))
		}
		// Every caller refused progress, the way an HSM caller does, and still waits for the close.
		for i := range 100 {
			nt.updateCallback(fmt.Sprintf("caller-%d", i), func(cb *callback.Callback) { cb.ProgressDisabled = true })
		}
		var failed *serviceerror.FailedPrecondition
		require.ErrorAs(t, nt.attach("caller-100"), &failed, "a full notifier of waiting callers refuses the attach")
		cbs := nt.callbacks()
		require.Len(t, cbs, 100)
		for id, cb := range cbs {
			require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, cb.GetStatus(), "%s still waits for its completion", id)
		}
	})

	t.Run("AFullNotifierMakesRoomFromCallbacksThatAreDone", func(t *testing.T) {
		nt := newNotifierTest(t, 2)
		require.NoError(t, nt.attach("a"))
		require.NoError(t, nt.attach("b"))
		require.NoError(t, nt.notify(1, true))
		nt.updateCallback("a", func(cb *callback.Callback) { cb.Status = callbackspb.CALLBACK_STATUS_SUCCEEDED })
		require.NoError(t, nt.attach("late"), "a callback that is done makes room for a late attach")
		require.NotContains(t, nt.callbacks(), "a")
		require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, nt.callbacks()["late"].GetStatus())
	})

	t.Run("AnOwnerThatEndedWithoutClosingClosesTheStreamWithAFailure", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			status enumspb.WorkflowExecutionStatus
			err    error
		}{
			{name: "completed", status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED},
			{name: "terminated", status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED},
			{name: "gone", err: serviceerror.NewNotFound("workflow not found")},
		} {
			t.Run(tc.name, func(t *testing.T) {
				nt := newNotifierTest(t, 10)
				require.NoError(t, nt.attach("a"))
				nt.runOwnerCheck(tc.status, tc.err)
				require.True(t, nt.describe().GetClosed())
				require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, nt.callbacks()["a"].GetStatus())
				nt.read(func(n *StreamNotifier, ctx chasm.Context) {
					completion, err := n.GetNexusCompletion(ctx, "a")
					require.NoError(t, err)
					require.NotNil(t, completion.Error)
					require.ErrorContains(t, completion.Error.Cause, ownerEndedFailure)
				})
			})
		}
	})

	t.Run("ARunningOwnerArmsTheNextCheck", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attach("a"))
		var first time.Time
		nt.read(func(n *StreamNotifier, _ chasm.Context) { first = n.GetOwnerCheckTime().AsTime() })
		nt.runOwnerCheck(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, nil)
		require.False(t, nt.describe().GetClosed())
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			require.NotNil(t, n.GetOwnerCheckTime())
			require.False(t, n.GetOwnerCheckTime().AsTime().Before(first), "the next check is armed")
		})
	})

	t.Run("BusinessIDsNameTheOwnerKind", func(t *testing.T) {
		require.Equal(t, "workflow/wf%2F1/run-1/tokens", BusinessID(&streampb.StreamReference{
			OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
			WorkflowId: "wf/1",
			RunId:      "run-1",
			Topic:      "tokens",
		}))
	})

	t.Run("BusinessIDsDoNotCollide", func(t *testing.T) {
		require.NotEqual(t,
			BusinessID(&streampb.StreamReference{WorkflowId: "a/b", Topic: "c"}),
			BusinessID(&streampb.StreamReference{WorkflowId: "a", Topic: "b/c"}),
		)
		require.NotEqual(t,
			BusinessID(&streampb.StreamReference{WorkflowId: "a", RunId: "first-chain", Topic: "t"}),
			BusinessID(&streampb.StreamReference{WorkflowId: "a", RunId: "second-chain", Topic: "t"}),
			"a run chain that reuses the Workflow ID gets its own notifier",
		)
	})
}

func TestClosedRetentionCoversTheStoreRetention(t *testing.T) {
	dc := dynamicconfig.NewNoopCollection()
	require.Equal(t, 7*24*time.Hour, configProvider(dc).ClosedRetention("ns"),
		"a closed stream answers late attaches as long as the store keeps its records")
}
