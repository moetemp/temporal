package streamnotifier

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
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
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type notifierTest struct {
	t          *testing.T
	metrics    *metricstest.CaptureHandler
	ctx        context.Context
	engine     *chasmtest.Engine
	handler    *handler
	expiry     *expiryTaskHandler
	ownerCheck *ownerCheckTaskHandler
	history    *historyservicemock.MockHistoryServiceClient
	ref        *streampb.StreamReference
	// ownerFirstRun is the first run of the owner's chain as an attach sees it; empty means the
	// owner does not exist.
	ownerFirstRun string
	// runChains maps a run ID to its chain's first run ID, for a describe that names a run. A run
	// that is not in it answers as ownerFirstRun does.
	runChains map[string]string
	// ownerCloseTime is when the owner's run closed, as the owner check's describe answers.
	ownerCloseTime *timestamppb.Timestamp
	recheck        *recheckTaskHandler
	debounce       *debounceTaskHandler
	// reserveErr is History's answer to the reservation of a Workflow reader's attach.
	reserveErr error
	reserved   []*historyservice.RecordStreamWakeRequest
	// counted makes History answer that a reserve's reader counted before.
	counted bool
	// activity describes the owner of an Activity-owned stream.
	activity *fakeActivityClient
}

func newNotifierTest(t *testing.T, maxCallbacks int) *notifierTest {
	config := &Config{
		Enabled:            dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		MaxCallbacks:       dynamicconfig.GetIntPropertyFnFilteredByNamespace(maxCallbacks),
		MaxCallbackBytes:   MaxCallbackBytes.Get(dynamicconfig.NewNoopCollection()),
		BlobSizeLimitError: dynamicconfig.BlobSizeLimitError.Get(dynamicconfig.NewNoopCollection()),
		IdleTimeout:        dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Hour),
		IdleTimeoutNoCallbacks: dynamicconfig.GetDurationPropertyFnFilteredByNamespace(
			10 * time.Minute,
		),
		PollMaxWait:        dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Second),
		RecheckInitial:     dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Minute),
		RecheckMax:         dynamicconfig.GetDurationPropertyFnFilteredByNamespace(5 * time.Minute),
		ClosedRetention:    dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Minute),
		OwnerCheckInterval: dynamicconfig.GetDurationPropertyFnFilteredByNamespace(5 * time.Minute),
		OwnerCloseGrace:    dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Minute),
		OwnerStartGrace:    dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Minute),
		// Most tests check one delivery per notification. The debounce tests turn it on.
		DebounceWindow: dynamicconfig.GetDurationPropertyFnFilteredByNamespace(0),
		MaxIDLength:    dynamicconfig.GetIntPropertyFn(1000),
	}
	ctrl := gomock.NewController(t)
	history := historyservicemock.NewMockHistoryServiceClient(ctrl)
	registryMock := namespace.NewMockRegistry(ctrl)
	ns := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: "namespace-id", Name: "ns"}, nil, "active")
	registryMock.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil).AnyTimes()
	nt := &notifierTest{}
	activity := &fakeActivityClient{t: t}
	attachHistory := historyservicemock.NewMockHistoryServiceClient(ctrl)
	attachHistory.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			req *historyservice.DescribeWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*historyservice.DescribeWorkflowExecutionResponse, error) {
			firstRun, ok := nt.runChains[req.GetRequest().GetExecution().GetRunId()]
			if !ok {
				firstRun = nt.ownerFirstRun
			}
			if firstRun == "" {
				return nil, serviceerror.NewNotFound("workflow not found")
			}
			return &historyservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					FirstRunId: firstRun,
				},
			}, nil
		}).
		AnyTimes()
	attachHistory.EXPECT().RecordStreamWake(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			req *historyservice.RecordStreamWakeRequest,
			_ ...grpc.CallOption,
		) (*historyservice.RecordStreamWakeResponse, error) {
			nt.reserved = append(nt.reserved, req)
			return &historyservice.RecordStreamWakeResponse{
				Reserved: req.GetMode() == enumsspb.STREAM_WAKE_MODE_RESERVE && !nt.counted,
			}, nt.reserveErr
		}).
		AnyTimes()
	h := newHandler(
		config,
		log.NewTestLogger(),
		attachHistory,
		newOwnerDescriber(attachHistory, activity),
	)
	expiry := newExpiryTaskHandler(expiryTaskHandlerOptions{Config: config})
	ownerCheck := newOwnerCheckTaskHandler(ownerCheckTaskHandlerOptions{
		Config:            config,
		Owners:            newOwnerDescriber(history, activity),
		NamespaceRegistry: registryMock,
	})
	registry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(callback.NewNilLibrary()))
	recheck := newRecheckTaskHandler(recheckTaskHandlerOptions{Config: config})
	debounce := newDebounceTaskHandler(debounceTaskHandlerOptions{Config: config})
	require.NoError(t, registry.Register(newLibrary(h, expiry, ownerCheck, recheck, debounce)))
	metricsHandler := metricstest.NewCaptureHandler()
	engine := chasmtest.NewEngine(t, registry, chasmtest.WithMetricsHandler(metricsHandler))
	*nt = notifierTest{
		t:          t,
		metrics:    metricsHandler,
		ctx:        chasm.NewEngineContext(context.Background(), engine),
		engine:     engine,
		handler:    h,
		expiry:     expiry,
		ownerCheck: ownerCheck,
		recheck:    recheck,
		debounce:   debounce,
		history:    history,
		activity:   activity,
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
	return nt.attachWithToken(requestID, "secret-"+requestID)
}

func (nt *notifierTest) attachWithToken(requestID, token string) error {
	_, err := nt.handler.AttachStreamCallback(nt.ctx, &streamnotifierpb.AttachStreamCallbackRequest{
		NamespaceId: "namespace-id",
		BusinessId:  BusinessID(nt.ref),
		FrontendRequest: &workflowservice.AttachStreamCallbackRequest{
			Namespace: "ns",
			StreamRef: nt.ref,
			RequestId: requestID,
			Callback: &commonpb.Callback_Nexus{
				Url:    "http://caller/" + requestID,
				Header: map[string]string{"temporal-callback-token": token},
			},
		},
	})
	return err
}

// attachWorkflow attaches a Workflow reader's target, named by its subscription.
func (nt *notifierTest) attachWorkflow(requestID, subscription string) error {
	_, err := nt.handler.AttachStreamCallback(nt.ctx, &streamnotifierpb.AttachStreamCallbackRequest{
		NamespaceId: "namespace-id",
		BusinessId:  BusinessID(nt.ref),
		FrontendRequest: &workflowservice.AttachStreamCallbackRequest{
			Namespace: "ns",
			StreamRef: nt.ref,
			RequestId: requestID,
			WorkflowTarget: &streampb.WorkflowWakeTarget{
				WorkflowId:     "reader",
				RunId:          "reader-run",
				SubscriptionId: subscription,
			},
		},
	})
	return err
}

// runDebounce runs the task of the open debounce window.
func (nt *notifierTest) runDebounce() {
	_, _, err := chasm.UpdateComponent(nt.ctx, chasm.NewComponentRef[*StreamNotifier](nt.key()),
		func(n *StreamNotifier, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			task := &streamnotifierpb.DebounceTask{ScheduledTime: n.GetDebounceUntil()}
			valid, err := nt.debounce.Validate(ctx, n, chasm.TaskInvocation{}, task)
			require.NoError(nt.t, err)
			require.True(nt.t, valid, "a debounce window is open")
			return struct{}{}, nt.debounce.Execute(ctx, n, chasm.TaskAttributes{}, task)
		}, struct{}{})
	require.NoError(nt.t, err)
}

// runRecheck runs the armed recheck task until it fires at the recheck time, as the timer queue
// would.
func (nt *notifierTest) runRecheck() {
	for due := false; !due; {
		_, _, err := chasm.UpdateComponent(nt.ctx, chasm.NewComponentRef[*StreamNotifier](nt.key()),
			func(n *StreamNotifier, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
				task := &streamnotifierpb.RecheckTask{ScheduledTime: n.GetRecheckTaskTime()}
				due = !task.GetScheduledTime().AsTime().Before(n.GetRecheckTime().AsTime())
				valid, err := nt.recheck.Validate(ctx, n, chasm.TaskInvocation{}, task)
				require.NoError(nt.t, err)
				require.True(nt.t, valid)
				return struct{}{}, nt.recheck.Execute(ctx, n, chasm.TaskAttributes{}, task)
			}, struct{}{})
		require.NoError(nt.t, err)
	}
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

// notifyWith sends a notification with the fields of fe, on the test's stream.
func (nt *notifierTest) notifyWith(fe *workflowservice.NotifyStreamRequest) error {
	fe.Namespace = "ns"
	fe.StreamRef = nt.ref
	_, err := nt.handler.NotifyStream(nt.ctx, &streamnotifierpb.NotifyStreamRequest{
		NamespaceId:     "namespace-id",
		BusinessId:      BusinessID(nt.ref),
		FrontendRequest: fe,
	})
	return err
}

// closeWithFailure closes the stream with failure instead of a result.
func (nt *notifierTest) closeWithFailure(failure *failurepb.Failure) error {
	_, err := nt.handler.NotifyStream(nt.ctx, &streamnotifierpb.NotifyStreamRequest{
		NamespaceId: "namespace-id",
		BusinessId:  BusinessID(nt.ref),
		FrontendRequest: &workflowservice.NotifyStreamRequest{
			Namespace:    "ns",
			StreamRef:    nt.ref,
			Counter:      1,
			Close:        true,
			CloseFailure: failure,
		},
	})
	return err
}

// completionOf is the state a callback completes in, and the Temporal failure it carries, or nil
// for a success.
func (nt *notifierTest) completionOf(requestID string) (nexus.OperationState, *failurepb.Failure) {
	var state nexus.OperationState
	var failure *failurepb.Failure
	nt.read(func(n *StreamNotifier, ctx chasm.Context) {
		completion, err := n.GetNexusCompletion(ctx, requestID)
		require.NoError(nt.t, err)
		if completion.Error == nil {
			state = nexus.OperationStateSucceeded
			return
		}
		var opErr *nexus.OperationError
		require.ErrorAs(nt.t, completion.Error, &opErr)
		state = opErr.State
		var cause *nexus.FailureError
		require.ErrorAs(nt.t, opErr.Cause, &cause)
		failure, err = commonnexus.NexusFailureToTemporalFailure(cause.Failure)
		require.NoError(nt.t, err)
	})
	return state, failure
}

func (nt *notifierTest) describe() *workflowservice.DescribeStreamNotifierResponse {
	resp, err := nt.handler.DescribeStreamNotifier(
		nt.ctx,
		&streamnotifierpb.DescribeStreamNotifierRequest{
			NamespaceId: "namespace-id",
			BusinessId:  BusinessID(nt.ref),
			FrontendRequest: &workflowservice.DescribeStreamNotifierRequest{
				Namespace: "ns",
				StreamRef: nt.ref,
			},
		},
	)
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

// expire runs the armed expiry task until it fires at the expiry time, as the timer queue would.
func (nt *notifierTest) expire() {
	for due := false; !due; {
		_, _, err := chasm.UpdateComponent(nt.ctx, chasm.NewComponentRef[*StreamNotifier](nt.key()),
			func(n *StreamNotifier, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
				task := &streamnotifierpb.ExpiryTask{ScheduledTime: n.GetExpiryTaskTime()}
				due = !task.GetScheduledTime().AsTime().Before(n.GetExpiryTime().AsTime())
				valid, err := nt.expiry.Validate(ctx, n, chasm.TaskInvocation{}, task)
				require.NoError(nt.t, err)
				require.True(nt.t, valid)
				return struct{}{}, nt.expiry.Execute(ctx, n, chasm.TaskAttributes{}, task)
			}, struct{}{})
		require.NoError(nt.t, err)
	}
}

// hasTimer reports whether the notifier scheduled a timer task for at.
func (nt *notifierTest) hasTimer(at time.Time) bool {
	byCategory, err := nt.engine.Tasks(chasm.NewComponentRef[*StreamNotifier](nt.key()))
	require.NoError(nt.t, err)
	for _, task := range byCategory[tasks.CategoryTimer] {
		if task.GetVisibilityTime().Equal(at) {
			return true
		}
	}
	return false
}

// start starts the notifier the way a client's first poll does, with no wait.
func (nt *notifierTest) start() {
	_, err := nt.poll(-1)
	require.NoError(nt.t, err)
}

// backdateStart moves the notifier's start back by d.
func (nt *notifierTest) backdateStart(d time.Duration) {
	_, _, err := chasm.UpdateComponent(nt.ctx, chasm.NewComponentRef[*StreamNotifier](nt.key()),
		func(n *StreamNotifier, _ chasm.MutableContext, _ struct{}) (struct{}, error) {
			n.StartTime = timestamppb.New(n.StartTime.AsTime().Add(-d))
			return struct{}{}, nil
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

// now is the engine's time, which the notifier reads.
func (nt *notifierTest) now() time.Time {
	var now time.Time
	nt.read(func(n *StreamNotifier, ctx chasm.Context) { now = ctx.Now(n) })
	return now
}

// runOwnerCheck runs the armed owner check, with History describing the owner as answered.
func (nt *notifierTest) runOwnerCheck(status enumspb.WorkflowExecutionStatus, err error) {
	nt.runOwnerCheckOf(status, nt.ref.GetRunId(), err)
}

// runOwnerCheckOf is runOwnerCheck with the described run's chain named by its first run ID.
func (nt *notifierTest) runOwnerCheckOf(
	status enumspb.WorkflowExecutionStatus,
	firstRunID string,
	err error,
) {
	nt.history.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			req *historyservice.DescribeWorkflowExecutionRequest,
			_ ...grpc.CallOption,
		) (*historyservice.DescribeWorkflowExecutionResponse, error) {
			require.Equal(
				nt.t,
				nt.ref.GetWorkflowId(),
				req.GetRequest().GetExecution().GetWorkflowId(),
			)
			require.Empty(
				nt.t,
				req.GetRequest().GetExecution().GetRunId(),
				"the chain's current run is described",
			)
			if err != nil {
				return nil, err
			}
			return &historyservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Status:     status,
					FirstRunId: firstRunID,
					CloseTime:  nt.ownerCloseTime,
				},
			}, nil
		})
	nt.runArmedOwnerCheck()
}

// runArmedOwnerCheck runs the armed owner check against the describers as they are set up.
func (nt *notifierTest) runArmedOwnerCheck() {
	var armed *streamnotifierpb.OwnerCheckTask
	nt.read(func(n *StreamNotifier, _ chasm.Context) {
		require.NotNil(nt.t, n.GetOwnerCheckTime(), "an owner check is armed")
		armed = &streamnotifierpb.OwnerCheckTask{ScheduledTime: n.GetOwnerCheckTime()}
	})
	ref := chasm.NewComponentRef[*StreamNotifier](nt.key())
	require.NoError(nt.t, nt.ownerCheck.Execute(nt.ctx, ref, chasm.TaskAttributes{}, armed))
}

func TestStreamNotifier(t *testing.T) {
	t.Run("ANexusCallbackGetsOnlyTheCompletion", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attach("a"))
		require.NoError(t, nt.attach("a"), "attaching again with the request ID changes nothing")
		require.NoError(t, nt.notify(1, false))
		require.NoError(t, nt.notify(3, false))
		require.NoError(t, nt.notify(2, false), "a lower counter is accepted and ignored")
		require.NoError(t, nt.attach("b"))

		cbs := nt.callbacks()
		require.Len(t, cbs, 2)
		for _, id := range []string{"a", "b"} {
			require.Zero(t, cbs[id].GetProgressInFlight(), "the Nexus variant takes no progress")
			require.Nil(t, cbs[id].GetPendingProgress())
		}

		desc := nt.describe()
		require.Equal(t, int64(3), desc.GetCounter())
		require.False(t, desc.GetClosed())
		require.Equal(
			t,
			[]string{"a", "b"},
			[]string{desc.GetCallbacks()[0].GetRequestId(), desc.GetCallbacks()[1].GetRequestId()},
		)
		require.Equal(t, enumspb.CALLBACK_STATE_STANDBY, desc.GetCallbacks()[0].GetState())
		require.Equal(
			t,
			"http://caller/a",
			desc.GetCallbacks()[0].GetCallback().GetNexus().GetUrl(),
		)
		require.Equal(t, map[string]string{"temporal-callback-token": ""},
			desc.GetCallbacks()[0].GetCallback().GetNexus().GetHeader(),
			"header values hold the caller's token, which would let a reader forge a completion")
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
		_, err := nt.handler.DetachStreamCallback(
			nt.ctx,
			&streamnotifierpb.DetachStreamCallbackRequest{
				NamespaceId: "namespace-id",
				BusinessId:  BusinessID(nt.ref),
				FrontendRequest: &workflowservice.DetachStreamCallbackRequest{
					Namespace: "ns",
					StreamRef: nt.ref,
					RequestId: "a",
				},
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			callbackspb.CALLBACK_STATUS_SCHEDULED,
			nt.callbacks()["a"].GetStatus(),
			"the canceled caller gets a completion",
		)
		require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, nt.callbacks()["b"].GetStatus())
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			completion, err := n.GetNexusCompletion(ctx, "a")
			require.NoError(t, err)
			require.NotNil(t, completion.Error)
			require.Equal(t, nexus.OperationStateCanceled, completion.Error.State)
		})
		nt.ref = &streampb.StreamReference{
			OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
			WorkflowId: "other",
			Topic:      "t",
		}
		_, err = nt.handler.DetachStreamCallback(
			nt.ctx,
			&streamnotifierpb.DetachStreamCallbackRequest{
				NamespaceId: "namespace-id",
				BusinessId:  BusinessID(nt.ref),
				FrontendRequest: &workflowservice.DetachStreamCallbackRequest{
					Namespace: "ns",
					StreamRef: nt.ref,
					RequestId: "a",
				},
			},
		)
		require.NoError(t, err, "detaching from a stream with no notifier changes nothing")
	})

	t.Run("TheCallersStartRidesItsCompletion", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		start := time.Unix(1700000000, 0).UTC()
		require.NoError(t, nt.attachStarted("a", "token-a", start))
		require.NoError(t, nt.notify(1, true))
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			completion, err := n.GetNexusCompletion(ctx, "a")
			require.NoError(t, err)
			require.Equal(
				t,
				"token-a",
				completion.OperationToken,
				"a completion before the start response still names the operation",
			)
			require.Equal(t, start, completion.StartTime)
		})
	})

	t.Run("AnAttachNamingAnotherRunThanTheChainsFirstIsRefused", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		nt.ref = &streampb.StreamReference{
			OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
			WorkflowId: "wf",
			RunId:      "a-later-run",
			Topic:      "t",
		}
		nt.ownerFirstRun = "first-run"
		var invalid *serviceerror.InvalidArgument
		require.ErrorAs(
			t,
			nt.attach("a"),
			&invalid,
			"a run that is not the chain's first would key another notifier",
		)
		nt.ref.RunId = "first-run"
		require.NoError(t, nt.attach("a"))
	})

	t.Run("AnAttachToAnEarlierChainOfAReusedWorkflowIDIsTaken", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		nt.ref = &streampb.StreamReference{
			OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
			WorkflowId: "wf",
			RunId:      "first-chain",
			Topic:      "t",
		}
		// The Workflow ID now belongs to a second chain.
		nt.ownerFirstRun = "second-chain"
		nt.runChains = map[string]string{
			"first-chain":       "first-chain",
			"first-chain-retry": "first-chain",
			"second-chain":      "second-chain",
		}
		require.NoError(t, nt.attach("a"))
		nt.ref.RunId = "first-chain-retry"
		var invalid *serviceerror.InvalidArgument
		require.ErrorAs(
			t,
			nt.attach("b"),
			&invalid,
			"a later run of the earlier chain is still not its first",
		)
	})

	t.Run("AReusedWorkflowIDGetsAFreshNotifier", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		nt.ref = &streampb.StreamReference{
			OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
			WorkflowId: "wf",
			RunId:      "first-chain",
			Topic:      "t",
		}
		require.NoError(t, nt.notify(1, true))
		require.True(t, nt.describe().GetClosed())
		nt.ref = &streampb.StreamReference{
			OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
			WorkflowId: "wf",
			RunId:      "second-chain",
			Topic:      "t",
		}
		require.NoError(t, nt.attach("a"))
		require.False(
			t,
			nt.describe().GetClosed(),
			"the later chain's stream is not the closed one",
		)
		require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, nt.callbacks()["a"].GetStatus())
	})

	t.Run("AnOwnerIDReusedByAnotherChainCountsAsEnded", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		nt.ref = &streampb.StreamReference{
			OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
			WorkflowId: "wf",
			RunId:      "first-chain",
			Topic:      "t",
		}
		require.NoError(t, nt.attach("a"))
		nt.runOwnerCheckOf(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, "second-chain", nil)
		require.True(t, nt.describe().GetClosed())
		state, failure := nt.completionOf("a")
		require.Equal(t, nexus.OperationStateFailed, state)
		require.Equal(t, workflowOwnerEndedFailure, failure.GetMessage())
	})

	t.Run("HoldsAtMostTheCallbackLimit", func(t *testing.T) {
		nt := newNotifierTest(t, 2)
		require.NoError(t, nt.attach("a"))
		require.NoError(t, nt.attach("b"))
		var failed *serviceerror.ResourceExhausted
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
		require.NoError(t, nt.notify(2, true))
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			completion, err := n.GetNexusCompletion(ctx, "b")
			require.NoError(t, err)
			require.Nil(t, completion.Error, "a later caller gets the close result")
		})
	})

	t.Run("AnIdleNotifierWithNoCallersEndsAndANewOneStarts", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attach("a"))
		_, err := nt.handler.DetachStreamCallback(
			nt.ctx,
			&streamnotifierpb.DetachStreamCallbackRequest{
				NamespaceId: "namespace-id",
				BusinessId:  BusinessID(nt.ref),
				FrontendRequest: &workflowservice.DetachStreamCallbackRequest{
					Namespace: "ns", StreamRef: nt.ref, RequestId: "a",
				},
			},
		)
		require.NoError(t, err)
		nt.expire()
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			require.Equal(t, chasm.LifecycleStateFailed, n.LifecycleState(ctx))
		})
		require.NoError(t, nt.notify(1, false), "a notification to an ended notifier is dropped")
		require.NoError(t, nt.attach("later"), "a later attach starts a new notifier")
		require.Contains(t, nt.callbacks(), "later")
	})

	t.Run("ANotificationStartsNoNotifier", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.notify(1, false), "nobody waits on a stream with no notifier")
		_, err := chasm.ReadComponent(nt.ctx, chasm.NewComponentRef[*StreamNotifier](nt.key()),
			func(*StreamNotifier, chasm.Context, struct{}) (struct{}, error) {
				return struct{}{}, nil
			}, struct{}{})
		var notFound *serviceerror.NotFound
		require.ErrorAs(t, err, &notFound, "the notification wrote nothing")

		nt = newNotifierTest(t, 10)
		nt.start()
		require.NoError(t, nt.notify(1, false))
		require.Equal(t, int64(1), nt.describe().GetCounter())
		require.True(t, nt.hasTimer(nt.now().Add(10*time.Minute)),
			"a notifier with no callbacks ends after the short idle timeout")

		nt = newNotifierTest(t, 10)
		require.NoError(t, nt.attach("a"))
		require.NoError(t, nt.notify(2, false))
		require.Equal(t, int64(2), nt.describe().GetCounter())
		require.True(t, nt.hasTimer(nt.now().Add(time.Hour)),
			"a notifier with a waiting callback keeps the full idle timeout")
		require.False(t, nt.hasTimer(nt.now().Add(10*time.Minute)))
	})

	t.Run("ARepeatedNotificationWritesNothing", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attach("a"))
		require.NoError(t, nt.notify(5, false))
		// A write would touch the notifier and move its activity time off this mark.
		mark := timestamppb.New(time.Unix(1, 0))
		_, _, err := chasm.UpdateComponent(nt.ctx, chasm.NewComponentRef[*StreamNotifier](nt.key()),
			func(n *StreamNotifier, _ chasm.MutableContext, _ struct{}) (struct{}, error) {
				n.LastActivityTime = mark
				return struct{}{}, nil
			}, struct{}{})
		require.NoError(t, err)
		require.NoError(t, nt.notify(5, false))
		require.NoError(t, nt.notify(3, false))
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			require.Equal(t, int64(5), n.GetCounter())
			require.True(t, n.GetLastActivityTime().AsTime().Equal(mark.AsTime()),
				"a counter at or below the one held costs only a read")
		})
		require.NoError(t, nt.notify(6, false))
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			require.False(t, n.GetLastActivityTime().AsTime().Equal(mark.AsTime()),
				"a higher counter writes")
		})
	})

	t.Run("ANotificationAfterTheStreamEndedChangesNothing", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.notify(1, true))
		nt.expire()
		require.NoError(t, nt.notify(2, false), "nothing waits on a closed stream")
		require.True(t, nt.describe().GetClosed())
	})

	t.Run("ActivityMovesTheExpiryWithoutANewTask", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attach("a"))
		var armed time.Time
		nt.read(func(n *StreamNotifier, _ chasm.Context) { armed = n.GetExpiryTaskTime().AsTime() })
		require.NoError(t, nt.notify(1, false))
		require.NoError(t, nt.notify(2, false))
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			require.Equal(t, armed, n.GetExpiryTaskTime().AsTime(), "the armed task stays")
		})

		later := armed.Add(time.Minute)
		_, _, err := chasm.UpdateComponent(nt.ctx, chasm.NewComponentRef[*StreamNotifier](nt.key()),
			func(n *StreamNotifier, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
				n.ExpiryTime = timestamppb.New(later)
				task := &streamnotifierpb.ExpiryTask{ScheduledTime: timestamppb.New(armed)}
				return struct{}{}, nt.expiry.Execute(ctx, n, chasm.TaskAttributes{}, task)
			}, struct{}{})
		require.NoError(t, err)
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			require.Equal(t, chasm.LifecycleStateRunning, n.LifecycleState(ctx))
			require.Equal(t, later, n.GetExpiryTaskTime().AsTime(), "an early task arms the next")
		})
		require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, nt.callbacks()["a"].GetStatus())
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

	t.Run("ACallbackWhoseReceiverClosedIsDropped", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attach("a"))
		require.NoError(t, nt.attach("b"))
		nt.updateCallback("a", func(cb *callback.Callback) { cb.ReceiverClosed = true })
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
			nt.updateCallback(
				fmt.Sprintf("caller-%d", i),
				func(cb *callback.Callback) { cb.ProgressDisabled = true },
			)
		}
		var failed *serviceerror.ResourceExhausted
		require.ErrorAs(
			t,
			nt.attach("caller-100"),
			&failed,
			"a full notifier of waiting callers refuses the attach",
		)
		cbs := nt.callbacks()
		require.Len(t, cbs, 100)
		for id, cb := range cbs {
			require.Equal(
				t,
				callbackspb.CALLBACK_STATUS_STANDBY,
				cb.GetStatus(),
				"%s still waits for its completion",
				id,
			)
		}
	})

	t.Run("AFullNotifierMakesRoomFromCallbacksThatAreDone", func(t *testing.T) {
		nt := newNotifierTest(t, 2)
		require.NoError(t, nt.attach("a"))
		require.NoError(t, nt.attach("b"))
		require.NoError(t, nt.notify(1, true))
		nt.updateCallback(
			"a",
			func(cb *callback.Callback) { cb.Status = callbackspb.CALLBACK_STATUS_SUCCEEDED },
		)
		require.NoError(
			t,
			nt.attach("late"),
			"a callback that is done makes room for a late attach",
		)
		require.NotContains(t, nt.callbacks(), "a")
		require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, nt.callbacks()["late"].GetStatus())
	})

	t.Run("AnEmptySuccessCarriesTheNilEncoding", func(t *testing.T) {
		// A typed caller decodes the result by its encoding, so an empty one must say it is null.
		for _, tc := range []struct {
			name  string
			close func(*notifierTest)
		}{
			{
				name: "completed owner",
				close: func(nt *notifierTest) {
					nt.runOwnerCheck(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, nil)
				},
			},
			{
				name: "close without a result",
				close: func(nt *notifierTest) {
					_, err := nt.handler.NotifyStream(nt.ctx, &streamnotifierpb.NotifyStreamRequest{
						NamespaceId: "namespace-id",
						BusinessId:  BusinessID(nt.ref),
						FrontendRequest: &workflowservice.NotifyStreamRequest{
							Namespace: "ns",
							StreamRef: nt.ref,
							Counter:   1,
							Close:     true,
						},
					})
					require.NoError(nt.t, err)
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				nt := newNotifierTest(t, 10)
				require.NoError(t, nt.attach("a"))
				tc.close(nt)
				nt.read(func(n *StreamNotifier, ctx chasm.Context) {
					completion, err := n.GetNexusCompletion(ctx, "a")
					require.NoError(t, err)
					require.Nil(t, completion.Error)
					payload, ok := completion.Result.(*commonpb.Payload)
					require.True(t, ok, "%T", completion.Result)
					require.Equal(t, "binary/null", string(payload.GetMetadata()["encoding"]))
					require.Empty(t, payload.GetData())
				})
			})
		}
	})

	t.Run("ACloseWithAFailureCompletesTheCallersAsFailed", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attach("a"))
		require.NoError(t, nt.closeWithFailure(&failurepb.Failure{
			Message: "the agent gave up",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "AgentGaveUp"},
			},
		}))
		require.NoError(t, nt.attach("late"))
		for _, id := range []string{"a", "late"} {
			state, failure := nt.completionOf(id)
			require.Equal(t, nexus.OperationStateFailed, state, id)
			require.Equal(t, "AgentGaveUp", failure.GetApplicationFailureInfo().GetType(), id)
			require.Equal(t, "the agent gave up", failure.GetMessage(), id)
		}
		desc := nt.describe()
		require.True(t, desc.GetClosed())
		require.Equal(t, "AgentGaveUp", desc.GetCloseFailure().GetApplicationFailureInfo().GetType())
	})

	t.Run("ACloseWithACanceledFailureCompletesTheCallersAsCanceled", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attach("a"))
		require.NoError(t, nt.closeWithFailure(&failurepb.Failure{
			Message: "canceled",
			FailureInfo: &failurepb.Failure_CanceledFailureInfo{
				CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
			},
		}))
		state, failure := nt.completionOf("a")
		require.Equal(t, nexus.OperationStateCanceled, state)
		require.NotNil(t, failure.GetCanceledFailureInfo())
	})

	t.Run("AnEndedOwnerClosesTheStreamAsAWorkflowRunOperationCompletes", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			status enumspb.WorkflowExecutionStatus
			err    error
			state  nexus.OperationState
			// check is nil for a success.
			check func(*failurepb.Failure) bool
		}{
			{
				name:   "completed",
				status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				state:  nexus.OperationStateSucceeded,
			},
			{
				name:   "failed",
				status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
				state:  nexus.OperationStateFailed,
				check: func(f *failurepb.Failure) bool {
					return f.GetApplicationFailureInfo().GetType() == ownerFailedType
				},
			},
			{
				name:   "canceled",
				status: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
				state:  nexus.OperationStateCanceled,
				check:  func(f *failurepb.Failure) bool { return f.GetCanceledFailureInfo() != nil },
			},
			{
				name:   "terminated",
				status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
				state:  nexus.OperationStateFailed,
				check:  func(f *failurepb.Failure) bool { return f.GetTerminatedFailureInfo() != nil },
			},
			{
				name:   "timed out",
				status: enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
				state:  nexus.OperationStateFailed,
				check:  func(f *failurepb.Failure) bool { return f.GetTimeoutFailureInfo() != nil },
			},
			{
				name:  "gone",
				err:   serviceerror.NewNotFound("workflow not found"),
				state: nexus.OperationStateFailed,
				check: func(f *failurepb.Failure) bool {
					return f.GetMessage() == workflowOwnerEndedFailure
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				nt := newNotifierTest(t, 10)
				require.NoError(t, nt.attach("a"))
				nt.backdateStart(2 * time.Minute)
				nt.runOwnerCheck(tc.status, tc.err)
				require.True(t, nt.describe().GetClosed())
				require.Equal(
					t,
					callbackspb.CALLBACK_STATUS_SCHEDULED,
					nt.callbacks()["a"].GetStatus(),
				)
				state, failure := nt.completionOf("a")
				require.Equal(t, tc.state, state)
				if tc.check == nil {
					require.Nil(t, failure)
					return
				}
				require.True(t, tc.check(failure), "%v", failure)
			})
		}
	})

	t.Run("AnOwnerThatAWorkerClosesGetsTheGraceBeforeTheCheckCloses", func(t *testing.T) {
		// The Worker closes the stream after the final task, with the Workflow's own result, and
		// the notifier keeps the first close. So the check leaves a just-closed run to it.
		for _, status := range []enumspb.WorkflowExecutionStatus{
			enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		} {
			t.Run(status.String(), func(t *testing.T) {
				nt := newNotifierTest(t, 10)
				require.NoError(t, nt.attach("a"))
				nt.ownerCloseTime = timestamppb.New(nt.now().Add(-time.Minute + time.Second))
				nt.runOwnerCheck(status, nil)
				require.False(t, nt.describe().GetClosed(), "inside the grace")
				nt.read(func(n *StreamNotifier, _ chasm.Context) {
					require.NotNil(t, n.GetOwnerCheckTime(), "the next check is armed")
				})
				nt.ownerCloseTime = timestamppb.New(nt.now().Add(-time.Minute))
				nt.runOwnerCheck(status, nil)
				require.True(t, nt.describe().GetClosed(), "once the grace passed")
			})
		}
	})

	t.Run("AnOwnerNoWorkerClosesClosesAtOnce", func(t *testing.T) {
		for _, status := range []enumspb.WorkflowExecutionStatus{
			enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
		} {
			t.Run(status.String(), func(t *testing.T) {
				nt := newNotifierTest(t, 10)
				require.NoError(t, nt.attach("a"))
				nt.ownerCloseTime = timestamppb.New(nt.now())
				nt.runOwnerCheck(status, nil)
				require.True(t, nt.describe().GetClosed())
			})
		}
	})

	t.Run("AMissingOwnerGetsTheStartGrace", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attach("a"))
		gone := serviceerror.NewNotFound("workflow not found")
		nt.runOwnerCheck(enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, gone)
		require.False(t, nt.describe().GetClosed(), "the owner may not have started yet")
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			require.NotNil(t, n.GetOwnerCheckTime(), "the next check is armed")
		})

		nt.backdateStart(2 * time.Minute)
		nt.runOwnerCheck(enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, gone)
		require.True(t, nt.describe().GetClosed(), "an owner that never started closes the stream")
		_, failure := nt.completionOf("a")
		require.Equal(t, workflowOwnerEndedFailure, failure.GetMessage())

		nt = newNotifierTest(t, 10)
		require.NoError(t, nt.attach("a"))
		_, _, err := chasm.UpdateComponent(nt.ctx, chasm.NewComponentRef[*StreamNotifier](nt.key()),
			func(n *StreamNotifier, _ chasm.MutableContext, _ struct{}) (struct{}, error) {
				n.StartTime = nil
				return struct{}{}, nil
			}, struct{}{})
		require.NoError(t, err)
		nt.runOwnerCheck(enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, gone)
		require.True(t, nt.describe().GetClosed(),
			"a notifier with no start time gets no grace")
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
			require.False(
				t,
				n.GetOwnerCheckTime().AsTime().Before(first),
				"the next check is armed",
			)
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
		require.NotEqual(
			t,
			BusinessID(
				&streampb.StreamReference{WorkflowId: "a", RunId: "first-chain", Topic: "t"},
			),
			BusinessID(
				&streampb.StreamReference{WorkflowId: "a", RunId: "second-chain", Topic: "t"},
			),
			"a run chain that reuses the Workflow ID gets its own notifier",
		)
	})
}

func TestClosedRetentionCoversTheStoreRetention(t *testing.T) {
	dc := dynamicconfig.NewNoopCollection()
	require.Equal(t, 7*24*time.Hour, configProvider(dc).ClosedRetention("ns"),
		"a closed stream answers late attaches as long as the store keeps its records")
}

func TestARecheckComesAtMostFiveMinutesLate(t *testing.T) {
	require.Equal(t, 5*time.Minute,
		configProvider(dynamicconfig.NewNoopCollection()).RecheckMax("ns"),
		"a reader with no notification wakes within the cap")
}

func TestTheOwnerCheckRunsEveryThirtySeconds(t *testing.T) {
	require.Equal(t, 30*time.Second,
		configProvider(dynamicconfig.NewNoopCollection()).OwnerCheckInterval("ns"),
		"an implicit close reaches waiting callers within about 30 s plus the grace")
}

func TestTheDefaultHoldsAThousandWaitingCallers(t *testing.T) {
	maxCallbacks := configProvider(dynamicconfig.NewNoopCollection()).MaxCallbacks("ns")
	nt := newNotifierTest(t, maxCallbacks)
	attached := 0
	var err error
	for attached < maxCallbacks {
		if err = nt.attach(fmt.Sprintf("caller-%d", attached)); err != nil {
			break
		}
		attached++
	}
	var full *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &full, "the caller after the limit is refused")
	require.Greater(t, attached, 1000)
	require.Len(t, nt.callbacks(), attached)
}

// The budget counts the progress and the close outcome that come after the attaches, so a full
// notifier stays under the mutable state limit when both arrive.
func TestAFullNotifierStaysUnderTheBudgetAfterProgressAndClose(t *testing.T) {
	nt := newNotifierTest(t, 2000)
	attached := 0
	for ; attached < 2000; attached++ {
		if nt.attach(fmt.Sprintf("caller-%d", attached)) != nil {
			break
		}
	}
	require.Less(t, attached, 2000, "the byte limit is reached first")
	// Every receiver is down, so each callback keeps the largest progress.
	require.NoError(t, nt.notifyWith(&workflowservice.NotifyStreamRequest{
		Position: strings.Repeat("p", maxPositionBytes),
		Counter:  1,
		Metadata: map[string]string{"k": strings.Repeat("m", maxMetadataBytes-1)},
	}))
	blobLimit := dynamicconfig.BlobSizeLimitError.Get(dynamicconfig.NewNoopCollection())("ns")
	require.NoError(t, nt.notifyWith(&workflowservice.NotifyStreamRequest{
		Counter:     2,
		Close:       true,
		CloseResult: &commonpb.Payload{Data: make([]byte, blobLimit-64)},
	}))
	maxBytes := MaxCallbackBytes.Get(dynamicconfig.NewNoopCollection())("ns")
	nt.read(func(n *StreamNotifier, ctx chasm.Context) {
		held := proto.Size(n.CloseResult)
		for _, field := range n.Callbacks {
			held += proto.Size(field.Get(ctx).CallbackState)
		}
		require.LessOrEqual(t, held, maxBytes)
	})
}

// A request ID is a key or an entry in more than one place of the state, so the limit counts
// each copy.
func TestLongRequestIDsCountOncePerCopy(t *testing.T) {
	nt := newNotifierTest(t, 2000)
	attached := 0
	var err error
	for attached < 2000 {
		if err = nt.attach(fmt.Sprintf("%0999d", attached)); err != nil {
			break
		}
		attached++
	}
	var full *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &full)
	dc := dynamicconfig.NewNoopCollection()
	room := MaxCallbackBytes.Get(dc)("ns") - dynamicconfig.BlobSizeLimitError.Get(dc)("ns")
	require.LessOrEqual(t, attached, room/((1+requestIDCopies)*999+progressReserve))
}

func TestLargeCallbacksHitTheByteLimitBeforeTheCountLimit(t *testing.T) {
	nt := newNotifierTest(t, 2000)
	// A header near frontend.callbackHeaderMaxLength.
	large := strings.Repeat("t", 8*1024-64)
	attached := 0
	var err error
	for attached < 2000 {
		if err = nt.attachWithToken(fmt.Sprintf("caller-%d", attached), large); err != nil {
			break
		}
		attached++
	}
	var failed *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &failed, "an attach past the byte limit is refused")
	dc := dynamicconfig.NewNoopCollection()
	room := MaxCallbackBytes.Get(dc)("ns") - dynamicconfig.BlobSizeLimitError.Get(dc)("ns")
	require.LessOrEqual(t, attached, room/(8*1024-64+progressReserve),
		"each callback counts its header and its progress")
	require.Greater(t, attached, room/(9*1024+progressReserve), "the callbacks fill the byte limit")
	require.Len(t, nt.callbacks(), attached)

	// The notifier makes room from callbacks that are done, as it does for the count limit.
	// A close outcome as large as the limit takes the room counted for it.
	closeResult := make([]byte, dynamicconfig.BlobSizeLimitError.Get(dc)("ns")-64)
	require.NoError(t, nt.notifyWith(&workflowservice.NotifyStreamRequest{
		Counter:     1,
		Close:       true,
		CloseResult: &commonpb.Payload{Data: closeResult},
	}))
	nt.updateCallback(
		"caller-0",
		func(cb *callback.Callback) { cb.Status = callbackspb.CALLBACK_STATUS_SUCCEEDED },
	)
	require.NoError(t, nt.attachWithToken("late", large))
	require.NotContains(t, nt.callbacks(), "caller-0")
	require.Contains(t, nt.callbacks(), "late")
}

func (nt *notifierTest) poll(after int64) (*workflowservice.PollStreamNotifierResponse, error) {
	resp, err := nt.handler.PollStreamNotifier(nt.ctx, &streamnotifierpb.PollStreamNotifierRequest{
		NamespaceId: "namespace-id",
		BusinessId:  BusinessID(nt.ref),
		FrontendRequest: &workflowservice.PollStreamNotifierRequest{
			Namespace:    "ns",
			StreamRef:    nt.ref,
			AfterCounter: after,
		},
	})
	return resp.GetFrontendResponse(), err
}

func TestPollStreamNotifier(t *testing.T) {
	t.Run("ACounterAlreadyAboveReturnsAtOnce", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		nt.start()
		require.NoError(t, nt.notify(42, false))
		started := time.Now()
		resp, err := nt.poll(41)
		require.NoError(t, err)
		require.Equal(t, int64(42), resp.GetCounter(),
			"a notification that landed before the poll is not lost")
		require.Equal(t, "p", resp.GetPosition())
		require.Equal(t, map[string]string{"k": "v"}, resp.GetMetadata())
		require.Less(t, time.Since(started), 500*time.Millisecond)
	})

	t.Run("ANotificationWakesAWaitingPoll", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		nt.start()
		require.NoError(t, nt.notify(1, false))
		done := make(chan *workflowservice.PollStreamNotifierResponse, 1)
		go func() {
			resp, err := nt.poll(1)
			assert.NoError(t, err)
			done <- resp
		}()
		require.NoError(t, nt.notify(2, false))
		resp := <-done
		require.Equal(t, int64(2), resp.GetCounter())
	})

	t.Run("TheWaitLimitReturnsNoChange", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		nt.start()
		require.NoError(t, nt.notify(5, false))
		resp, err := nt.poll(5)
		require.NoError(t, err)
		require.Equal(t, int64(5), resp.GetCounter(), "the counter is not above the one asked for")
		require.False(t, resp.GetClosed())
	})

	t.Run("APollOnAMissingNotifierStartsIt", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		resp, err := nt.poll(41)
		require.NoError(t, err)
		require.Zero(t, resp.GetCounter())
		require.Zero(t, nt.describe().GetCounter(), "the poll started the notifier")
		require.True(t, nt.hasTimer(nt.now().Add(10*time.Minute)),
			"it ends after the short idle timeout if nothing happens")
	})

	t.Run("APollOnAnEndedNotifierStartsANewOne", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		nt.start()
		require.NoError(t, nt.notify(1, false))
		nt.expire()
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			require.Equal(t, chasm.LifecycleStateFailed, n.LifecycleState(ctx))
		})
		resp, err := nt.poll(1)
		require.NoError(t, err)
		require.Zero(t, resp.GetCounter(), "the poll waited on a new notifier")
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			require.Equal(t, chasm.LifecycleStateRunning, n.LifecycleState(ctx))
		})
	})

	t.Run("TheCloseEndsThePollWithItsOutcome", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.notify(3, true))
		resp, err := nt.poll(100)
		require.NoError(t, err)
		require.True(t, resp.GetClosed())
		require.Equal(t, []byte("summary"), resp.GetCloseResult().GetData())

		nt = newNotifierTest(t, 10)
		require.NoError(t, nt.closeWithFailure(streamFailure("boom")))
		resp, err = nt.poll(100)
		require.NoError(t, err)
		require.True(t, resp.GetClosed())
		require.Equal(t, "boom", resp.GetCloseFailure().GetMessage())
	})
}

func TestWorkflowTargets(t *testing.T) {
	t.Run("NotificationsReachEveryReaderAsFoldedProgress", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachWorkflow("a", "sub-a"))
		require.NoError(t, nt.attachWorkflow("a", "sub-a"),
			"attaching again with the request ID changes nothing")
		require.NoError(t, nt.notify(1, false))
		require.NoError(t, nt.notify(3, false))
		require.NoError(t, nt.notify(2, false), "a lower counter is accepted and ignored")
		require.NoError(t, nt.attachWorkflow("b", "sub-b"))

		cbs := nt.callbacks()
		require.Len(t, cbs, 2)
		require.Equal(t, int64(1), cbs["a"].GetProgressInFlightCounter(), "one delivery in flight")
		require.Equal(t, int64(3), cbs["a"].GetPendingProgress().GetCounter(),
			"later progress folds into the next one")
		require.Equal(t, int64(3), cbs["b"].GetPendingProgress().GetCounter(),
			"a late reader starts from the latest notification")
		require.Equal(t, "p", cbs["b"].GetPendingProgress().GetPosition())
		require.Equal(t, map[string]string{"k": "v"}, cbs["b"].GetPendingProgress().GetMetadata())
		require.Equal(t, "sub-a", cbs["a"].GetCallback().GetStreamWake().GetSubscriptionId())
		require.Equal(t, "reader-run", cbs["a"].GetCallback().GetStreamWake().GetRunId())
	})

	t.Run("AnAttachReservesTheReader", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachWorkflow("a", "sub-a"))
		require.Len(t, nt.reserved, 1)
		require.Equal(t, enumsspb.STREAM_WAKE_MODE_RESERVE, nt.reserved[0].GetMode())
		require.Equal(t, "sub-a", nt.reserved[0].GetWake().GetSubscriptionId())
		require.Equal(t, "reader", nt.reserved[0].GetExecution().GetWorkflowId())

		nt.reserveErr = serviceerror.NewFailedPrecondition("too many readers")
		var failed *serviceerror.FailedPrecondition
		require.ErrorAs(t, nt.attachWorkflow("b", "sub-b"), &failed,
			"an attach past the readers per run fails where user code sees it")
		require.NotContains(t, nt.callbacks(), "b")
	})

	t.Run("AFailedAttachFreesItsReserve", func(t *testing.T) {
		nt := newNotifierTest(t, 1)
		require.NoError(t, nt.attachWorkflow("a", "sub-a"))
		var failed *serviceerror.ResourceExhausted
		require.ErrorAs(t, nt.attachWorkflow("b", "sub-b"), &failed, "the notifier is full")
		require.Len(t, nt.reserved, 3)
		require.Equal(t, enumsspb.STREAM_WAKE_MODE_RELEASE, nt.reserved[2].GetMode())
		require.Equal(t, "sub-b", nt.reserved[2].GetWake().GetSubscriptionId())

		nt.counted = true
		require.ErrorAs(t, nt.attachWorkflow("c", "sub-a"), &failed)
		require.Len(t, nt.reserved, 4, "a reader that counted before keeps its place")
	})

	t.Run("ARepeatedAttachFreesTheReserveNoCallbackHolds", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachWorkflow("a", "sub-a"))
		require.Len(t, nt.reserved, 1)
		// The reader's close was taken, so its place was freed, and the attach comes again.
		require.NoError(t, nt.attachWorkflow("a", "sub-a"))
		require.Len(t, nt.reserved, 3)
		require.Equal(t, enumsspb.STREAM_WAKE_MODE_RELEASE, nt.reserved[2].GetMode())
		require.Equal(t, "sub-a", nt.reserved[2].GetWake().GetSubscriptionId())

		nt.counted = true
		require.NoError(t, nt.attachWorkflow("a", "sub-a"))
		require.Len(t, nt.reserved, 4, "a reader that counted before keeps its place")
	})

	t.Run("DescribeShowsTheTarget", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachWorkflow("a", "sub-a"))
		desc := nt.describe()
		require.Len(t, desc.GetCallbacks(), 1)
		require.Nil(t, desc.GetCallbacks()[0].GetCallback())
		require.Equal(t, "sub-a", desc.GetCallbacks()[0].GetWorkflowTarget().GetSubscriptionId())
		require.Equal(t, "reader", desc.GetCallbacks()[0].GetWorkflowTarget().GetWorkflowId())
	})

	t.Run("TheCompletionWakesTheReader", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachWorkflow("a", "sub-a"))
		require.NoError(t, nt.notify(4, true))
		require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, nt.callbacks()["a"].GetStatus())
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			wake, err := n.GetStreamWakeCompletion(ctx, "a")
			require.NoError(t, err)
			require.True(t, wake.GetClosed())
			require.Nil(t, wake.GetCloseResult(), "the reader reads the result from the store")
			require.Equal(t, int64(4), wake.GetCounter())
		})

		nt = newNotifierTest(t, 10)
		require.NoError(t, nt.attachWorkflow("a", "sub-a"))
		require.NoError(t, nt.closeWithFailure(streamFailure("boom")))
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			wake, err := n.GetStreamWakeCompletion(ctx, "a")
			require.NoError(t, err)
			require.True(t, wake.GetClosed())
			require.Equal(t, "boom", wake.GetCloseFailure().GetMessage())
		})

		nt = newNotifierTest(t, 10)
		require.NoError(t, nt.attachWorkflow("a", "sub-a"))
		require.NoError(t, nt.closeWithFailure(streamFailure(strings.Repeat("x", 5000))))
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			wake, err := n.GetStreamWakeCompletion(ctx, "a")
			require.NoError(t, err)
			require.True(t, wake.GetClosed())
			require.Nil(t, wake.GetCloseFailure(), "a large failure stays in the store")
		})
	})

	t.Run("AnIdleTimeoutAsksTheReaderToResubscribe", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachWorkflow("a", "sub-a"))
		nt.expire()
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			wake, err := n.GetStreamWakeCompletion(ctx, "a")
			require.NoError(t, err)
			require.True(t, wake.GetResubscribe())
			require.False(t, wake.GetClosed(), "the stream stays open")
		})
	})

	t.Run("ADetachedReaderGetsNoWake", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachWorkflow("a", "sub-a"))
		_, err := nt.handler.DetachStreamCallback(
			nt.ctx,
			&streamnotifierpb.DetachStreamCallbackRequest{
				NamespaceId: "namespace-id",
				BusinessId:  BusinessID(nt.ref),
				FrontendRequest: &workflowservice.DetachStreamCallbackRequest{
					Namespace: "ns", StreamRef: nt.ref, RequestId: "a",
				},
			},
		)
		require.NoError(t, err)
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			wake, err := n.GetStreamWakeCompletion(ctx, "a")
			require.NoError(t, err)
			require.Nil(t, wake)
		})
	})

	t.Run("AReaderOfAClosedRunIsDroppedAndFreesItsPlace", func(t *testing.T) {
		nt := newNotifierTest(t, 1)
		require.NoError(t, nt.attachWorkflow("a", "sub-a"))
		var failed *serviceerror.ResourceExhausted
		require.ErrorAs(t, nt.attachWorkflow("b", "sub-b"), &failed, "the notifier is full")
		// A delivery found the reader's run closed.
		nt.updateCallback("a", func(cb *callback.Callback) {
			cb.ProgressDisabled = true
			cb.ReceiverClosed = true
		})
		require.NoError(t, nt.attachWorkflow("b", "sub-b"),
			"the dead callback no longer counts toward the limit")
		require.NotContains(t, nt.callbacks(), "a")
	})
}

func TestRechecks(t *testing.T) {
	t.Run("AQuietPeriodAsksWorkflowReadersToReadAgain", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachWorkflow("a", "sub-a"))
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			require.True(t, nt.now().Add(time.Minute).Equal(n.GetRecheckTime().AsTime()))
		})
		nt.runRecheck()
		cb := nt.callbacks()["a"]
		require.True(t, cb.GetPendingProgress().GetRecheck(), "the reader reads whatever the counter")
		require.NotZero(t, cb.GetProgressInFlight(), "the recheck goes out with no new counter")
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			require.Equal(t, 2*time.Minute, n.GetRecheckGap().AsDuration(), "the gap doubles")
		})
		nt.runRecheck()
		nt.runRecheck()
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			require.Equal(t, 5*time.Minute, n.GetRecheckGap().AsDuration(),
				"streamnotifier.recheckMax caps the gap")
		})

		require.NoError(t, nt.notify(1, false))
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			require.Equal(t, time.Minute, n.GetRecheckGap().AsDuration(),
				"a notification sets the gap back")
			require.True(t, nt.now().Add(time.Minute).Equal(n.GetRecheckTime().AsTime()))
		})
	})

	t.Run("NotificationsMoveTheRecheckWithoutANewTask", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachWorkflow("a", "sub-a"))
		var armed time.Time
		nt.read(func(n *StreamNotifier, _ chasm.Context) { armed = n.GetRecheckTaskTime().AsTime() })
		require.NoError(t, nt.notify(1, false))
		require.NoError(t, nt.notify(2, false))
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			require.Equal(t, armed, n.GetRecheckTaskTime().AsTime(), "the armed task stays")
		})

		later := armed.Add(time.Minute)
		_, _, err := chasm.UpdateComponent(nt.ctx, chasm.NewComponentRef[*StreamNotifier](nt.key()),
			func(n *StreamNotifier, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
				n.RecheckTime = timestamppb.New(later)
				task := &streamnotifierpb.RecheckTask{ScheduledTime: timestamppb.New(armed)}
				return struct{}{}, nt.recheck.Execute(ctx, n, chasm.TaskAttributes{}, task)
			}, struct{}{})
		require.NoError(t, err)
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			require.Equal(t, later, n.GetRecheckTaskTime().AsTime(), "an early task arms the next")
		})
		require.False(t, nt.callbacks()["a"].GetPendingProgress().GetRecheck(),
			"an early task asks for no read")
	})

	t.Run("OnlyWorkflowReadersArmARecheck", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attach("nexus"))
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			require.Nil(t, n.GetRecheckTime(), "a Nexus caller takes no wakes")
		})
	})

	t.Run("TheCloseStopsRechecks", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachWorkflow("a", "sub-a"))
		require.NoError(t, nt.notify(1, true))
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			require.Nil(t, n.GetRecheckTime())
		})
	})
}

func TestDebounce(t *testing.T) {
	newDebounced := func(t *testing.T) *notifierTest {
		nt := newNotifierTest(t, 10)
		nt.handler.config.DebounceWindow = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(
			time.Hour,
		)
		require.NoError(t, nt.attachWorkflow("a", "sub"))
		return nt
	}
	// handed is the highest counter handed to the callback, waiting, in flight or delivered.
	handed := func(nt *notifierTest) int64 {
		var counter int64
		nt.read(func(n *StreamNotifier, ctx chasm.Context) {
			cb := n.Callbacks["a"].Get(ctx)
			counter = max(
				cb.GetPendingProgress().GetCounter(),
				cb.GetProgressInFlightCounter(),
				cb.GetDeliveredProgressCounter(),
			)
		})
		return counter
	}

	t.Run("TheFirstNotificationGoesOutAtOnce", func(t *testing.T) {
		nt := newDebounced(t)
		require.NoError(t, nt.notify(1, false))
		require.Equal(t, int64(1), handed(nt))
		now := nt.now()
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			require.True(t, now.Add(time.Hour).Equal(n.GetDebounceUntil().AsTime()),
				"the delivery opened a window")
			require.False(t, n.GetDebounceHeld())
		})
	})

	t.Run("ABurstGivesOneLeadingAndOneTrailingDelivery", func(t *testing.T) {
		nt := newDebounced(t)
		for counter := int64(1); counter <= 100; counter++ {
			require.NoError(t, nt.notify(counter, false))
		}
		require.Equal(t, int64(1), handed(nt), "the window holds the other 99")
		require.Equal(t, int64(100), nt.describe().GetCounter(), "the notifier keeps the newest")
		nt.runDebounce()
		require.Equal(t, int64(100), handed(nt), "the window's end delivers the newest")
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			require.NotNil(t, n.GetDebounceUntil(), "the trailing delivery opens the next window")
			require.False(t, n.GetDebounceHeld())
		})
		nt.runDebounce()
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			require.Nil(t, n.GetDebounceUntil(), "a quiet window closes")
		})
		require.NoError(t, nt.notify(101, false))
		require.Equal(t, int64(101), handed(nt), "after a quiet window the next one goes out at once")
	})

	t.Run("ACloseGoesOutAtOnce", func(t *testing.T) {
		nt := newDebounced(t)
		require.NoError(t, nt.notify(1, false))
		require.NoError(t, nt.notify(2, false))
		require.NoError(t, nt.notify(3, true))
		require.True(t, nt.describe().GetClosed())
		require.NotEqual(t, callbackspb.CALLBACK_STATUS_STANDBY, nt.callbacks()["a"].GetStatus(),
			"the close completes the reader at once")
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			require.Nil(t, n.GetDebounceUntil(), "a closed stream has no window")
		})
	})

	t.Run("APollSeesAHeldCounter", func(t *testing.T) {
		nt := newDebounced(t)
		require.NoError(t, nt.notify(1, false))
		require.NoError(t, nt.notify(2, false))
		resp, err := nt.poll(1)
		require.NoError(t, err)
		require.Equal(t, int64(2), resp.GetCounter(), "a poll costs no delivery, so it does not wait")
	})

	t.Run("ZeroTurnsTheDebounceOff", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachWorkflow("a", "sub"))
		require.NoError(t, nt.notify(1, false))
		require.NoError(t, nt.notify(2, false))
		require.Equal(t, int64(2), handed(nt))
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			require.Nil(t, n.GetDebounceUntil())
		})
	})
}

// attachIdleReader attaches a Workflow reader's target that gets a wake only when it waits.
func (nt *notifierTest) attachIdleReader(requestID, subscription string) error {
	_, err := nt.handler.AttachStreamCallback(nt.ctx, &streamnotifierpb.AttachStreamCallbackRequest{
		NamespaceId: "namespace-id",
		BusinessId:  BusinessID(nt.ref),
		FrontendRequest: &workflowservice.AttachStreamCallbackRequest{
			Namespace: "ns",
			StreamRef: nt.ref,
			RequestId: requestID,
			WorkflowTarget: &streampb.WorkflowWakeTarget{
				WorkflowId:     "reader",
				RunId:          "reader-run",
				SubscriptionId: subscription,
			},
			Idle: true,
		},
	})
	return err
}

// waitForReader asks for one wake of an idle reader after the counter.
func (nt *notifierTest) waitForReader(requestID string, after int64) error {
	_, err := nt.handler.WaitStreamCallback(nt.ctx, &streamnotifierpb.WaitStreamCallbackRequest{
		NamespaceId: "namespace-id",
		BusinessId:  BusinessID(nt.ref),
		FrontendRequest: &workflowservice.WaitStreamCallbackRequest{
			Namespace:    "ns",
			StreamRef:    nt.ref,
			RequestId:    requestID,
			AfterCounter: after,
		},
	})
	return err
}

// wokeTo is the highest counter handed to a reader, waiting, in flight or delivered.
func (nt *notifierTest) wokeTo(requestID string) int64 {
	cb := nt.callbacks()[requestID]
	return max(
		cb.GetPendingProgress().GetCounter(),
		cb.GetProgressInFlightCounter(),
		cb.GetDeliveredProgressCounter(),
	)
}

func TestIdleWorkflowReaders(t *testing.T) {
	t.Run("ABusyStreamWakesAnIdleReaderNever", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachIdleReader("idle", "sub-idle"))
		require.NoError(t, nt.attachWorkflow("standing", "sub-standing"))
		for counter := int64(1); counter <= 20; counter++ {
			require.NoError(t, nt.notify(counter, false))
		}
		require.Zero(t, nt.wokeTo("idle"), "a reader that keeps up gets no wake")
		require.Equal(t, int64(20), nt.wokeTo("standing"), "a reader that is not idle gets them")
	})

	t.Run("AnIdleReaderAttachedLateGetsNoWake", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		nt.start()
		require.NoError(t, nt.notify(5, false))
		require.NoError(t, nt.attachIdleReader("idle", "sub"))
		require.Zero(t, nt.wokeTo("idle"))
	})

	t.Run("AWaitWakesOnceThenTheReaderIsIdleAgain", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachIdleReader("idle", "sub"))
		require.NoError(t, nt.notify(3, false))
		require.NoError(t, nt.waitForReader("idle", 3))
		require.Zero(t, nt.wokeTo("idle"), "the counter has not passed the wait")
		require.NoError(t, nt.notify(4, false))
		require.Equal(t, int64(4), nt.wokeTo("idle"), "the next notification wakes it")
		require.NoError(t, nt.notify(9, false))
		require.Equal(t, int64(4), nt.wokeTo("idle"), "the wake is used up")
		require.NoError(t, nt.waitForReader("idle", 4))
		require.Equal(t, int64(9), nt.wokeTo("idle"), "a passed counter wakes it at once")
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			require.Empty(t, n.GetWaits())
		})
	})

	t.Run("AWaitOnACallbackWhoseReceiverClosedDropsIt", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachIdleReader("idle", "sub"))
		// A wake found the reader's place gone, after a rebuild of its run's state.
		nt.updateCallback("idle", func(cb *callback.Callback) { cb.ReceiverClosed = true })
		var notFound *serviceerror.NotFound
		require.ErrorAs(t, nt.waitForReader("idle", 0), &notFound,
			"the reader attaches again")
		require.NotContains(t, nt.callbacks(), "idle")
		require.NoError(t, nt.attachIdleReader("idle", "sub"), "the attach makes a new callback")
		require.Contains(t, nt.callbacks(), "idle")
	})

	t.Run("AWaitBelowADeliveredCounterIsCounted", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		capture := nt.metrics.StartCapture()
		defer nt.metrics.StopCapture(capture)
		require.NoError(t, nt.attachIdleReader("idle", "sub"))
		require.NoError(t, nt.notify(3, false))
		require.NoError(t, nt.waitForReader("idle", 2))
		require.Empty(t, capture.SnapshotMetric(WaitBelowDelivered.Name()),
			"the reader did not get the counter yet")
		nt.updateCallback("idle", func(cb *callback.Callback) {
			cb.PendingProgress = nil
			cb.DeliveredProgressCounter = 3
		})
		// The reader's read found the store below the counter it was woken for.
		require.NoError(t, nt.waitForReader("idle", 2))
		require.Len(t, capture.SnapshotMetric(WaitBelowDelivered.Name()), 1)
		require.NoError(t, nt.waitForReader("idle", 3))
		require.Len(t, capture.SnapshotMetric(WaitBelowDelivered.Name()), 1)
	})

	t.Run("ANewerWaitReplacesAnOlderOne", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachIdleReader("idle", "sub"))
		require.NoError(t, nt.waitForReader("idle", 2))
		require.NoError(t, nt.waitForReader("idle", 5))
		require.NoError(t, nt.notify(3, false))
		require.Zero(t, nt.wokeTo("idle"))
		require.NoError(t, nt.notify(6, false))
		require.Equal(t, int64(6), nt.wokeTo("idle"))
	})

	t.Run("AWakeInsideAWindowGoesOutAtTheWindowsEnd", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		nt.handler.config.DebounceWindow = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(
			time.Hour,
		)
		require.NoError(t, nt.attachIdleReader("idle", "sub"))
		require.NoError(t, nt.notify(1, false))
		require.NoError(t, nt.waitForReader("idle", 1))
		require.NoError(t, nt.notify(2, false))
		require.Zero(t, nt.wokeTo("idle"), "the window holds the notification")
		nt.runDebounce()
		require.Equal(t, int64(2), nt.wokeTo("idle"))
	})

	t.Run("ACloseCompletesAnIdleReader", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachIdleReader("idle", "sub"))
		require.NoError(t, nt.notify(3, true))
		require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, nt.callbacks()["idle"].GetStatus())
		require.NoError(t, nt.waitForReader("idle", 0), "a wait on a closed stream changes nothing")
	})

	t.Run("ARecheckWakesAnIdleReaderAndUsesUpItsWait", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachIdleReader("idle", "sub"))
		require.NoError(t, nt.waitForReader("idle", 0))
		nt.runRecheck()
		cb := nt.callbacks()["idle"]
		require.True(t, cb.GetPendingProgress().GetRecheck(), "a lost wait costs at most the gap")
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			require.Empty(t, n.GetWaits())
			require.Equal(t, 2*time.Minute, n.GetRecheckGap().AsDuration(), "the gap doubles")
		})
	})

	t.Run("ADetachDropsTheWait", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachIdleReader("idle", "sub"))
		require.NoError(t, nt.waitForReader("idle", 1))
		_, err := nt.handler.DetachStreamCallback(
			nt.ctx,
			&streamnotifierpb.DetachStreamCallbackRequest{
				NamespaceId: "namespace-id",
				BusinessId:  BusinessID(nt.ref),
				FrontendRequest: &workflowservice.DetachStreamCallbackRequest{
					Namespace: "ns", StreamRef: nt.ref, RequestId: "idle",
				},
			},
		)
		require.NoError(t, err)
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			require.Empty(t, n.GetWaits())
		})
		require.NoError(t, nt.waitForReader("idle", 1), "a detached reader's wait changes nothing")
		require.NoError(t, nt.notify(2, false))
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			require.Empty(t, n.GetWaits())
		})
	})

	t.Run("AnAttachAgainKeepsTheReaderIdle", func(t *testing.T) {
		nt := newNotifierTest(t, 10)
		require.NoError(t, nt.attachIdleReader("idle", "sub"))
		require.NoError(t, nt.attachIdleReader("idle", "sub"), "a new process attaches again")
		require.NoError(t, nt.notify(2, false))
		require.Zero(t, nt.wokeTo("idle"))
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			require.Equal(t, []string{"idle"}, n.GetIdleRequestIds())
		})
	})
}
