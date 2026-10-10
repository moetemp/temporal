package callback

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/resource"
	queuescommon "go.temporal.io/server/service/history/queues/common"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// progressRequest is what an outbound progress delivery put on the wire.
type progressRequest struct {
	state  string
	header http.Header
	body   map[string]any
}

type progressTest struct {
	t        *testing.T
	handler  *progressTaskHandler
	ctx      context.Context
	ref      chasm.ComponentRef
	requests []progressRequest
	// answer is the status the receiver answers the next delivery with.
	answer int
	// during runs while a delivery is on the wire.
	during func()
	// progressEnabled is the caller namespace's progress flag.
	progressEnabled bool
}

func newProgressTest(t *testing.T, cb *Callback, historyClient resource.HistoryClient) *progressTest {
	pt := &progressTest{t: t, answer: http.StatusOK, progressEnabled: true}
	ctrl := gomock.NewController(t)
	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(
		namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Name: "ns"}, nil, "active"), nil,
	).AnyTimes()
	pt.handler = newProgressTaskHandler(invocationTaskHandlerOptions{
		Config: &Config{
			RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
			RetryPolicy: func() backoff.RetryPolicy {
				return backoff.NewExponentialRetryPolicy(time.Second)
			},
			EnableProgress: func(string) bool { return pt.progressEnabled },
		},
		NamespaceRegistry: nsRegistry,
		MetricsHandler:    metrics.NoopMetricsHandler,
		Logger:            log.NewTestLogger(),
		HistoryClient:     historyClient,
		HTTPCallerProvider: func(queuescommon.NamespaceIDAndDestination) HTTPCaller {
			return func(r *http.Request) (*http.Response, error) {
				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				var decoded map[string]any
				decoder := json.NewDecoder(bytes.NewReader(body))
				decoder.UseNumber()
				require.NoError(t, decoder.Decode(&decoded))
				pt.requests = append(pt.requests, progressRequest{
					state:  r.Header.Get("Nexus-Operation-State"),
					header: r.Header.Clone(),
					body:   decoded,
				})
				if pt.during != nil {
					pt.during()
					pt.during = nil
				}
				return &http.Response{StatusCode: pt.answer, Body: http.NoBody}, nil
			}
		},
	})
	pt.ctx, pt.ref = newInvocationTaskTest(t, pt.handler.invocation, cb, nexusrpc.CompleteOperationOptions{})
	return pt
}

func (pt *progressTest) deliver(counter int64) {
	pt.t.Helper()
	_, _, err := chasm.UpdateComponent(pt.ctx, pt.ref, func(c *Callback, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
		return struct{}{}, c.DeliverProgress(ctx, &nexuspb.NexusOperationProgress{
			Position: "p",
			Counter:  counter,
			Metadata: map[string]string{"topic": "t"},
		})
	}, struct{}{})
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
	task := &callbackspb.ProgressTask{Counter: state.GetProgressInFlight(), Attempt: state.GetProgressAttempt()}
	var valid bool
	readCallbackState(pt.ctx, pt.t, pt.ref, func(ctx chasm.Context, c *Callback) {
		var err error
		valid, err = pt.handler.Validate(ctx, c, chasm.TaskInvocation{}, task)
		require.NoError(pt.t, err)
	})
	require.True(pt.t, valid, "the delivery in flight has a valid task")
	return pt.handler.Execute(pt.ctx, pt.ref, chasm.TaskAttributes{Destination: "http://localhost"}, task)
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

func TestProgressDeliveryOutbound(t *testing.T) {
	t.Run("OneInFlightAndABurstFoldsIntoTheNext", func(t *testing.T) {
		pt := newProgressTest(t, newNexusCallback("http://localhost/callback", map[string]string{"token": "abc"}), nil)
		pt.deliver(1)
		pt.deliver(3)
		pt.deliver(2) // stale behind 3
		state := pt.state()
		require.Equal(t, int64(1), state.GetProgressInFlight(), "one delivery in flight")
		require.Equal(t, int64(3), state.GetPendingProgress().GetCounter(), "later progress waits, folded")

		require.NoError(t, pt.runInFlight())
		require.Len(t, pt.requests, 1)
		require.Equal(t, "running", pt.requests[0].state)
		require.Equal(t, "abc", pt.requests[0].header.Get("token"), "the callback's headers ride along")
		require.Empty(t, pt.requests[0].header.Get("Nexus-Operation-Close-Time"))
		require.Equal(t, map[string]any{"position": "p", "counter": json.Number("3"), "metadata": map[string]any{"topic": "t"}}, pt.requests[0].body,
			"the delivery carries the newest pending progress")
		state = pt.state()
		require.Equal(t, int64(3), state.GetDeliveredProgressCounter())
		require.Zero(t, state.GetProgressInFlight())
		require.Nil(t, state.GetPendingProgress())

		pt.deliver(3)
		require.Zero(t, pt.state().GetProgressInFlight(), "a counter already delivered starts nothing")
	})

	t.Run("ProgressDuringADeliveryGetsAnotherOne", func(t *testing.T) {
		pt := newProgressTest(t, newNexusCallback("http://localhost/callback", nil), nil)
		pt.deliver(1)
		pt.during = func() { pt.deliver(5) }
		require.NoError(t, pt.runInFlight())
		state := pt.state()
		require.Equal(t, int64(1), state.GetDeliveredProgressCounter())
		require.Equal(t, int64(5), state.GetProgressInFlight(), "the newer progress is the next delivery")
		require.NoError(t, pt.runInFlight())
		require.Equal(t, json.Number("5"), pt.requests[1].body["counter"])
		require.Zero(t, pt.state().GetProgressInFlight())
	})

	for _, status := range []int{
		http.StatusBadRequest, http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusGone,
		http.StatusRequestEntityTooLarge, http.StatusUnsupportedMediaType, http.StatusUnprocessableEntity,
	} {
		t.Run(fmt.Sprintf("A%dTurnsProgressOff", status), func(t *testing.T) {
			pt := newProgressTest(t, newNexusCallback("http://localhost/callback", nil), nil)
			pt.deliver(1)
			pt.answer = status
			require.NoError(t, pt.runInFlight())
			state := pt.state()
			require.True(t, state.GetProgressDisabled())
			require.Zero(t, state.GetProgressInFlight())
			pt.deliver(2)
			require.Nil(t, pt.state().GetPendingProgress(), "progress stays off for the callback")
			require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, pt.state().GetStatus(), "the completion is still delivered")
			require.Equal(t, status == http.StatusNotFound, pt.state().GetCallerOperationClosed(),
				"only a 404 says the caller's operation is gone")
		})
	}

	for _, status := range []int{http.StatusRequestTimeout, http.StatusTooManyRequests} {
		t.Run(fmt.Sprintf("A%dRetries", status), func(t *testing.T) {
			pt := newProgressTest(t, newNexusCallback("http://localhost/callback", nil), nil)
			pt.deliver(1)
			pt.answer = status
			require.NoError(t, pt.runInFlight())
			state := pt.state()
			require.False(t, state.GetProgressDisabled())
			require.Equal(t, int64(1), state.GetProgressInFlight(), "the delivery is retried")
		})
	}

	t.Run("A5xxRetriesWithTheNewestProgress", func(t *testing.T) {
		pt := newProgressTest(t, newNexusCallback("http://localhost/callback", nil), nil)
		pt.deliver(1)
		pt.answer = http.StatusServiceUnavailable
		require.NoError(t, pt.runInFlight())
		state := pt.state()
		require.False(t, state.GetProgressDisabled())
		require.Equal(t, int64(1), state.GetProgressInFlight())
		require.Equal(t, int32(1), state.GetProgressAttempt())

		pt.deliver(2)
		pt.answer = http.StatusOK
		require.NoError(t, pt.runInFlight())
		require.Equal(t, json.Number("2"), pt.requests[1].body["counter"])
		require.Equal(t, int64(2), pt.state().GetDeliveredProgressCounter())
	})

	t.Run("TheCompletionNeverWaits", func(t *testing.T) {
		pt := newProgressTest(t, newNexusCallback("http://localhost/callback", nil), nil)
		pt.deliver(1)
		pt.during = func() {
			_, _, err := chasm.UpdateComponent(pt.ctx, pt.ref, func(c *Callback, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
				return struct{}{}, TransitionScheduled.Apply(c, ctx, EventScheduled{})
			}, struct{}{})
			require.NoError(t, err)
		}
		state := pt.state()
		task := &callbackspb.ProgressTask{Counter: state.GetProgressInFlight()}
		require.NoError(t, pt.handler.Execute(pt.ctx, pt.ref, chasm.TaskAttributes{Destination: "http://localhost"}, task))
		state = pt.state()
		require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, state.GetStatus())
		require.Zero(t, state.GetProgressInFlight(), "a result after the completion was scheduled is ignored")
		require.Nil(t, state.GetPendingProgress())
		pt.deliver(2)
		require.Nil(t, pt.state().GetPendingProgress(), "progress after the completion is dropped")
	})

	t.Run("AWorkerHandlerTakesNoProgressYet", func(t *testing.T) {
		cb := newNexusCallback("http://localhost/callback", nil)
		cb.Callback.Variant = &callbackspb.Callback_NexusHandler_{NexusHandler: &callbackspb.Callback_NexusHandler{TaskQueueName: "tq"}}
		pt := newProgressTest(t, cb, nil)
		pt.deliver(1)
		require.Nil(t, pt.state().GetPendingProgress())
	})
}

func TestProgressDeliveryInternal(t *testing.T) {
	ref := &persistencespb.ChasmComponentRef{NamespaceId: "namespace-id", BusinessId: "caller", RunId: "run", ArchetypeId: 1234}
	serialized, err := ref.Marshal()
	require.NoError(t, err)
	header := map[string]string{strings.ToLower(commonnexus.CallbackTokenHeader): base64.RawURLEncoding.EncodeToString(serialized)}

	for _, tc := range []struct {
		name         string
		err          error
		wantDisabled bool
		wantClosed   bool
		wantInFlight int64
	}{
		{name: "Delivered"},
		{name: "ClosedOperationTurnsProgressOff", err: serviceerror.NewNotFound("operation not found"), wantDisabled: true, wantClosed: true},
		{name: "UnavailableRetries", err: serviceerror.NewUnavailable("busy"), wantInFlight: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := historyservicemock.NewMockHistoryServiceClient(ctrl)
			client.EXPECT().CompleteNexusOperationChasm(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, req *historyservice.CompleteNexusOperationChasmRequest, _ ...grpc.CallOption) (*historyservice.CompleteNexusOperationChasmResponse, error) {
					require.Equal(t, serialized, req.GetCompletion().GetComponentRef())
					require.Equal(t, int64(1), req.GetProgress().GetCounter())
					require.Equal(t, "p", req.GetProgress().GetPosition())
					return &historyservice.CompleteNexusOperationChasmResponse{}, tc.err
				})
			pt := newProgressTest(t, newNexusCallback(chasm.NexusCompletionHandlerURL, header), client)
			pt.deliver(1)
			require.NoError(t, pt.runInFlight())
			state := pt.state()
			require.Equal(t, tc.wantDisabled, state.GetProgressDisabled())
			require.Equal(t, tc.wantClosed, state.GetCallerOperationClosed())
			require.Equal(t, tc.wantInFlight, state.GetProgressInFlight())
		})
	}
}

func TestProgressDeliveryInternalHonorsTheProgressFlag(t *testing.T) {
	ref := &persistencespb.ChasmComponentRef{NamespaceId: "namespace-id", BusinessId: "caller", RunId: "run", ArchetypeId: 1234}
	serialized, err := ref.Marshal()
	require.NoError(t, err)
	header := map[string]string{strings.ToLower(commonnexus.CallbackTokenHeader): base64.RawURLEncoding.EncodeToString(serialized)}
	// No call to History is expected: the caller's namespace does not take progress.
	client := historyservicemock.NewMockHistoryServiceClient(gomock.NewController(t))
	pt := newProgressTest(t, newNexusCallback(chasm.NexusCompletionHandlerURL, header), client)
	pt.progressEnabled = false
	pt.deliver(1)
	require.NoError(t, pt.runInFlight())
	require.True(t, pt.state().GetProgressDisabled())
}

func TestProgressBody(t *testing.T) {
	body, err := progressBody(&nexuspb.NexusOperationProgress{Counter: 7})
	require.NoError(t, err)
	require.JSONEq(t, `{"counter": 7}`, string(body), "optional members are left out")
}
