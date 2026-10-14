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
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/notificationservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
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
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/resource"
	queuescommon "go.temporal.io/server/service/history/queues/common"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// nexusProgressRequest is what an outbound progress delivery put on the wire.
type nexusProgressRequest struct {
	state  string
	header http.Header
	body   map[string]any
}

// nexusProgressTest runs the Nexus variant's real progress delivery against a fake receiver.
type nexusProgressTest struct {
	*progressTest
	requests []nexusProgressRequest
	// status is the HTTP status the receiver answers the next delivery with.
	status int
	// progressEnabled is the caller namespace's progress flag.
	progressEnabled bool
}

func newNexusProgressTest(
	t *testing.T,
	cb *Callback,
	historyClient resource.HistoryClient,
) *nexusProgressTest {
	npt := &nexusProgressTest{
		progressTest:    &progressTest{t: t},
		status:          http.StatusOK,
		progressEnabled: true,
	}
	ctrl := gomock.NewController(t)
	nsRegistry := namespace.NewMockRegistry(ctrl)
	ns := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "ns"}, nil, "active")
	nsRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil).AnyTimes()
	npt.handler = newProgressTaskHandler(invocationTaskHandlerOptions{
		Config: &Config{
			RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
			RetryPolicy: func() backoff.RetryPolicy {
				return backoff.NewExponentialRetryPolicy(time.Second)
			},
			EnableProgress:         func(string) bool { return npt.progressEnabled },
			ReceiverStateMaxSize:   func(string) int { return testReceiverStateMaxSize },
			ProgressBehindInterval: func(string) time.Duration { return time.Second },
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
				npt.requests = append(npt.requests, nexusProgressRequest{
					state:  r.Header.Get("Nexus-Operation-State"),
					header: r.Header.Clone(),
					body:   decoded,
				})
				if npt.during != nil {
					npt.during()
					npt.during = nil
				}
				return &http.Response{StatusCode: npt.status, Body: http.NoBody}, nil
			}
		},
	})
	npt.ctx, npt.ref = newInvocationTaskTest(
		t,
		npt.handler.invocation,
		cb,
		nexusrpc.CompleteOperationOptions{},
	)
	return npt
}

func TestNexusProgressDeliveryOutbound(t *testing.T) {
	t.Run("OneInFlightAndABurstFoldsIntoTheNext", func(t *testing.T) {
		npt := newNexusProgressTest(
			t,
			newNexusCallback("http://localhost/callback", map[string]string{"token": "abc"}),
			nil,
		)
		npt.report(1)
		npt.report(3)
		npt.report(2) // stale behind 3
		require.Equal(t, int64(3), npt.state().GetPendingProgress().GetCounter(),
			"later progress waits, folded")

		require.NoError(t, npt.runInFlight())
		require.Len(t, npt.requests, 1)
		require.Equal(t, "running", npt.requests[0].state)
		require.Equal(t, "abc", npt.requests[0].header.Get("token"),
			"the callback's headers ride along")
		require.Empty(t, npt.requests[0].header.Get("Nexus-Operation-Close-Time"))
		require.Equal(
			t,
			map[string]any{
				"position": "p",
				"counter":  "3",
				"metadata": map[string]any{"topic": "t"},
			},
			npt.requests[0].body,
			"the delivery carries the newest pending progress",
		)
		state := npt.state()
		require.Equal(t, int64(3), state.GetDeliveredProgressCounter())
		require.Zero(t, state.GetProgressInFlight())
		require.Nil(t, state.GetPendingProgress())
	})

	t.Run("ProgressDuringADeliveryGetsAnotherOne", func(t *testing.T) {
		npt := newNexusProgressTest(t, newNexusCallback("http://localhost/callback", nil), nil)
		npt.report(1)
		npt.during = func() { npt.report(5) }
		require.NoError(t, npt.runInFlight())
		require.Equal(t, int64(1), npt.state().GetDeliveredProgressCounter())
		require.NoError(t, npt.runInFlight())
		require.Equal(t, "5", npt.requests[1].body["counter"])
		require.Zero(t, npt.state().GetProgressInFlight())
	})

	for _, status := range []int{
		http.StatusBadRequest, http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusGone,
		http.StatusRequestEntityTooLarge, http.StatusUnsupportedMediaType,
		http.StatusUnprocessableEntity,
	} {
		t.Run(fmt.Sprintf("A%dTurnsProgressOff", status), func(t *testing.T) {
			npt := newNexusProgressTest(t, newNexusCallback("http://localhost/callback", nil), nil)
			npt.report(1)
			npt.status = status
			require.NoError(t, npt.runInFlight())
			state := npt.state()
			require.True(t, state.GetProgressDisabled())
			require.Zero(t, state.GetProgressInFlight())
			npt.report(2)
			require.Nil(t, npt.state().GetPendingProgress(), "progress stays off for the callback")
			require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, npt.state().GetStatus(),
				"the completion is still delivered")
			require.Equal(t, status == http.StatusNotFound, npt.state().GetReceiverClosed(),
				"only a 404 says the caller's operation is gone")
		})
	}

	for _, status := range []int{http.StatusRequestTimeout, http.StatusTooManyRequests} {
		t.Run(fmt.Sprintf("A%dRetries", status), func(t *testing.T) {
			npt := newNexusProgressTest(t, newNexusCallback("http://localhost/callback", nil), nil)
			npt.report(1)
			npt.status = status
			require.NoError(t, npt.runInFlight())
			state := npt.state()
			require.False(t, state.GetProgressDisabled())
			require.NotZero(t, state.GetProgressInFlight(), "the delivery is retried")
		})
	}

	t.Run("A5xxRetriesWithTheNewestProgress", func(t *testing.T) {
		npt := newNexusProgressTest(t, newNexusCallback("http://localhost/callback", nil), nil)
		npt.report(1)
		npt.status = http.StatusServiceUnavailable
		require.NoError(t, npt.runInFlight())
		state := npt.state()
		require.False(t, state.GetProgressDisabled())
		require.Equal(t, int32(1), state.GetProgressAttempt())

		npt.report(2)
		npt.status = http.StatusOK
		require.NoError(t, npt.runInFlight())
		require.Equal(t, "2", npt.requests[1].body["counter"])
		require.Equal(t, int64(2), npt.state().GetDeliveredProgressCounter())
	})
}

func TestNexusProgressDeliveryInternal(t *testing.T) {
	ref := &persistencespb.ChasmComponentRef{
		NamespaceId: "namespace-id",
		BusinessId:  "caller",
		RunId:       "run",
		ArchetypeId: 1234,
	}
	serialized, err := ref.Marshal()
	require.NoError(t, err)
	header := map[string]string{
		strings.ToLower(commonnexus.CallbackTokenHeader): base64.RawURLEncoding.EncodeToString(
			serialized,
		),
	}

	for _, tc := range []struct {
		name         string
		err          error
		wantDisabled bool
		wantClosed   bool
		wantInFlight bool
	}{
		{name: "Delivered"},
		{
			name:         "ClosedOperationTurnsProgressOff",
			err:          serviceerror.NewNotFound("operation not found"),
			wantDisabled: true,
			wantClosed:   true,
		},
		{name: "UnavailableRetries", err: serviceerror.NewUnavailable("busy"), wantInFlight: true},
		{
			name:         "NamespaceNotActiveRetries",
			err:          serviceerror.NewNamespaceNotActive("ns", "active", "standby"),
			wantInFlight: true,
		},
		{
			name:         "UnimplementedDuringAnUpgradeRetries",
			err:          serviceerror.NewUnimplemented("unhandled Nexus operation outcome"),
			wantInFlight: true,
		},
		{
			name:         "InvalidArgumentTurnsProgressOff",
			err:          serviceerror.NewInvalidArgument("odd"),
			wantDisabled: true,
		},
		{
			name:         "FailedPreconditionTurnsProgressOff",
			err:          serviceerror.NewFailedPrecondition("no"),
			wantDisabled: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := historyservicemock.NewMockHistoryServiceClient(ctrl)
			client.EXPECT().CompleteNexusOperationChasm(gomock.Any(), gomock.Any()).DoAndReturn(
				func(
					_ context.Context,
					req *historyservice.CompleteNexusOperationChasmRequest,
					_ ...grpc.CallOption,
				) (*historyservice.CompleteNexusOperationChasmResponse, error) {
					require.Equal(t, serialized, req.GetCompletion().GetComponentRef())
					require.Equal(t, int64(1), req.GetProgress().GetCounter())
					require.Equal(t, "p", req.GetProgress().GetPosition())
					return &historyservice.CompleteNexusOperationChasmResponse{}, tc.err
				})
			npt := newNexusProgressTest(
				t,
				newNexusCallback(chasm.NexusCompletionHandlerURL, header),
				client,
			)
			npt.report(1)
			require.NoError(t, npt.runInFlight())
			state := npt.state()
			require.Equal(t, tc.wantDisabled, state.GetProgressDisabled())
			require.Equal(t, tc.wantClosed, state.GetReceiverClosed())
			require.Equal(t, tc.wantInFlight, state.GetProgressInFlight() != 0)
		})
	}
}

func TestNexusProgressDeliveryInternalHonorsTheProgressFlag(t *testing.T) {
	ref := &persistencespb.ChasmComponentRef{
		NamespaceId: "namespace-id",
		BusinessId:  "caller",
		RunId:       "run",
		ArchetypeId: 1234,
	}
	serialized, err := ref.Marshal()
	require.NoError(t, err)
	header := map[string]string{
		strings.ToLower(commonnexus.CallbackTokenHeader): base64.RawURLEncoding.EncodeToString(
			serialized,
		),
	}
	// No call to History is expected, since the caller's namespace takes no progress.
	client := historyservicemock.NewMockHistoryServiceClient(gomock.NewController(t))
	npt := newNexusProgressTest(t, newNexusCallback(chasm.NexusCompletionHandlerURL, header), client)
	npt.progressEnabled = false
	npt.report(1)
	require.NoError(t, npt.runInFlight())
	require.True(t, npt.state().GetProgressDisabled())
}

func TestProgressBody(t *testing.T) {
	body, err := progressBody(&callbackspb.Progress{Counter: 7})
	require.NoError(t, err)
	require.JSONEq(t, `{"counter": "7"}`, string(body), "optional members are left out")

	body, err = progressBody(&callbackspb.Progress{Counter: 1<<62 + 1, Recheck: true})
	require.NoError(t, err)
	require.JSONEq(t, `{"counter": "4611686018427387905"}`, string(body),
		"a decimal string keeps counters above 2^53 exact for double-based parsers")
}

const testReceiverStateMaxSize = 16

func newNexusHandlerProgressCallback(progressOperation string) *Callback {
	return &Callback{
		CallbackState: &callbackspb.CallbackState{
			RequestId:        "request-id",
			RegistrationTime: timestamppb.New(time.Now()),
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_NexusHandler_{
					NexusHandler: &callbackspb.Callback_NexusHandler{
						TaskQueueName:     testNexusHandlerTaskQueue,
						Service:           "NotificationService",
						Operation:         "OnComplete",
						ProgressOperation: progressOperation,
						SourceContext:     &commonpb.Payload{Data: []byte("source-context")},
					},
				},
			},
			Status: callbackspb.CALLBACK_STATUS_STANDBY,
		},
	}
}

// nexusHandlerProgressTest records the OnProgress tasks a NexusHandler callback dispatches, and
// answers each one with the next of answers.
type nexusHandlerProgressTest struct {
	*nexusProgressTest
	dispatched []*matchingservice.DispatchNexusTaskRequest
	answers    []func() (*matchingservice.DispatchNexusTaskResponse, error)
}

func newNexusHandlerProgressTest(t *testing.T, cb *Callback) *nexusHandlerProgressTest {
	nt := &nexusHandlerProgressTest{nexusProgressTest: newNexusProgressTest(t, cb, nil)}
	matchingClient := matchingservicemock.NewMockMatchingServiceClient(gomock.NewController(t))
	matchingClient.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			req *matchingservice.DispatchNexusTaskRequest,
			_ ...grpc.CallOption,
		) (*matchingservice.DispatchNexusTaskResponse, error) {
			nt.dispatched = append(nt.dispatched, req)
			if nt.during != nil {
				nt.during()
				nt.during = nil
			}
			require.NotEmpty(t, nt.answers, "an answer for each dispatch")
			answer := nt.answers[0]
			nt.answers = nt.answers[1:]
			return answer()
		}).AnyTimes()
	nt.handler.invocation.matchingClient = matchingClient
	return nt
}

func (nt *nexusHandlerProgressTest) answer(
	resp *matchingservice.DispatchNexusTaskResponse,
	err error,
) {
	nt.answers = append(nt.answers, func() (*matchingservice.DispatchNexusTaskResponse, error) {
		return resp, err
	})
}

// onProgressRequest decodes the input of the i-th dispatched OnProgress task.
func (nt *nexusHandlerProgressTest) onProgressRequest(
	i int,
) *notificationservice.OnProgressRequest {
	nt.t.Helper()
	var req notificationservice.OnProgressRequest
	require.NoError(nt.t, payload.Decode(
		nt.dispatched[i].GetRequest().GetStartOperation().GetPayload(),
		&req,
	))
	return &req
}

// onProgressAnswer is a worker's sync answer to OnProgress, encoded by its data converter.
func onProgressAnswer(
	t *testing.T,
	resp *notificationservice.OnProgressResponse,
) *matchingservice.DispatchNexusTaskResponse {
	t.Helper()
	p, err := payload.Encode(resp)
	require.NoError(t, err)
	return startOperationResponse(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_SyncSuccess{
			SyncSuccess: &nexuspb.StartOperationResponse_Sync{Payload: p},
		},
	})
}

func TestNexusHandlerProgressDelivery(t *testing.T) {
	t.Run("DispatchesOnProgress", func(t *testing.T) {
		nt := newNexusHandlerProgressTest(t, newNexusHandlerProgressCallback("OnProgress"))
		nt.answer(syncSuccessResponse(), nil)
		nt.report(1)
		require.NoError(t, nt.runInFlight())

		require.Len(t, nt.dispatched, 1)
		dispatched := nt.dispatched[0]
		require.Equal(t, testNexusHandlerTaskQueue, dispatched.GetTaskQueue().GetName())
		require.Equal(t, enumspb.TASK_QUEUE_KIND_NORMAL, dispatched.GetTaskQueue().GetKind())
		start := dispatched.GetRequest().GetStartOperation()
		require.Equal(t, "NotificationService", start.GetService())
		require.Equal(t, "OnProgress", start.GetOperation())
		require.Equal(t, "request-id/progress/1", start.GetRequestId())
		require.Equal(
			t,
			[]byte("temporal.api.notificationservice.v1.OnProgressRequest"),
			start.GetPayload().GetMetadata()["messageType"],
		)
		require.Len(t, start.GetLinks(), 1)
		require.Equal(t, "temporal.api.common.v1.Link.Callback", start.GetLinks()[0].GetType())

		req := nt.onProgressRequest(0)
		require.Equal(t, int64(1), req.GetProgress().GetCounter())
		require.Equal(t, "p", req.GetProgress().GetPosition())
		require.Equal(t, map[string]string{"topic": "t"}, req.GetProgress().GetMetadata())
		require.Equal(t, []byte("source-context"), req.GetSourceContext().GetData())
		require.Empty(t, req.GetReceiverState(), "no state before the first accepted progress")

		state := nt.state()
		require.Equal(t, int64(1), state.GetDeliveredProgressCounter())
		require.Zero(t, state.GetProgressInFlight())
	})

	t.Run("NoProgressOperationSendsNothing", func(t *testing.T) {
		nt := newNexusHandlerProgressTest(t, newNexusHandlerProgressCallback(""))
		nt.report(1)
		require.Nil(t, nt.state().GetPendingProgress())
		require.Zero(t, nt.state().GetProgressInFlight())
		require.Empty(t, nt.dispatched)
	})

	undecodable := startOperationResponse(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_SyncSuccess{
			SyncSuccess: &nexuspb.StartOperationResponse_Sync{Payload: &commonpb.Payload{
				Metadata: map[string][]byte{"encoding": []byte("json/plain")},
				Data:     []byte("{not json"),
			}},
		},
	})
	for _, tc := range []struct {
		name         string
		response     func(*testing.T) *matchingservice.DispatchNexusTaskResponse
		err          error
		wantDisabled bool
		wantInFlight bool
	}{
		{
			name:     "SyncSuccessIsDelivered",
			response: func(*testing.T) *matchingservice.DispatchNexusTaskResponse { return syncSuccessResponse() },
		},
		{
			name: "AsyncSuccessIsDelivered",
			response: func(*testing.T) *matchingservice.DispatchNexusTaskResponse {
				return startOperationResponse(&nexuspb.StartOperationResponse{
					Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
						AsyncSuccess: &nexuspb.StartOperationResponse_Async{OperationToken: "t"},
					},
				})
			},
		},
		{
			name: "AnOperationFailureRefuses",
			response: func(*testing.T) *matchingservice.DispatchNexusTaskResponse {
				return startOperationResponse(&nexuspb.StartOperationResponse{
					Variant: &nexuspb.StartOperationResponse_Failure{
						Failure: &failurepb.Failure{
							Message: "no progress here",
							FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
								ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{},
							},
						},
					},
				})
			},
			wantDisabled: true,
		},
		{
			name: "ANotFoundHandlerErrorRefuses",
			response: func(*testing.T) *matchingservice.DispatchNexusTaskResponse {
				return handlerFailureResponse("NOT_FOUND")
			},
			wantDisabled: true,
		},
		{
			name: "ABadRequestHandlerErrorRefuses",
			response: func(*testing.T) *matchingservice.DispatchNexusTaskResponse {
				return handlerFailureResponse("BAD_REQUEST")
			},
			wantDisabled: true,
		},
		{
			name: "AnUnauthorizedHandlerErrorRefuses",
			response: func(*testing.T) *matchingservice.DispatchNexusTaskResponse {
				return handlerFailureResponse("UNAUTHORIZED")
			},
			wantDisabled: true,
		},
		{
			name: "ARetryableHandlerErrorRetries",
			response: func(*testing.T) *matchingservice.DispatchNexusTaskResponse {
				return handlerFailureResponse("INTERNAL")
			},
			wantInFlight: true,
		},
		{
			name: "AMatchingTimeoutRetries",
			response: func(*testing.T) *matchingservice.DispatchNexusTaskResponse {
				return &matchingservice.DispatchNexusTaskResponse{
					Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
						RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
					},
				}
			},
			wantInFlight: true,
		},
		{
			name:         "ARetryableRPCErrorRetries",
			err:          status.Error(codes.Unavailable, "matching unavailable"),
			wantInFlight: true,
		},
		{
			name:         "ANonRetryableRPCErrorRefuses",
			err:          status.Error(codes.InvalidArgument, "odd"),
			wantDisabled: true,
		},
		{
			name: "AnUndecodableResponseRefuses",
			response: func(*testing.T) *matchingservice.DispatchNexusTaskResponse {
				return undecodable
			},
			wantDisabled: true,
		},
		{
			name: "AStateOverTheCapRefuses",
			response: func(t *testing.T) *matchingservice.DispatchNexusTaskResponse {
				return onProgressAnswer(t, &notificationservice.OnProgressResponse{
					ReceiverState: bytes.Repeat([]byte("x"), testReceiverStateMaxSize+1),
				})
			},
			wantDisabled: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nt := newNexusHandlerProgressTest(t, newNexusHandlerProgressCallback("OnProgress"))
			var resp *matchingservice.DispatchNexusTaskResponse
			if tc.response != nil {
				resp = tc.response(t)
			}
			nt.answer(resp, tc.err)
			nt.report(1)
			// A failed delivery never counts against the task queue's circuit breaker, so a
			// follower that is down holds back no completion.
			require.NoError(t, nt.runInFlight())
			require.Len(t, nt.dispatched, 1)
			state := nt.state()
			require.Equal(t, tc.wantDisabled, state.GetProgressDisabled())
			require.Equal(t, tc.wantInFlight, state.GetProgressInFlight() != 0)
			require.False(t, state.GetReceiverClosed())
			require.Empty(t, state.GetReceiverState())
			if !tc.wantDisabled && !tc.wantInFlight {
				require.Equal(t, int64(1), state.GetDeliveredProgressCounter())
			}
		})
	}

	t.Run("TheSavedStateReachesTheNextRequest", func(t *testing.T) {
		nt := newNexusHandlerProgressTest(t, newNexusHandlerProgressCallback("OnProgress"))
		nt.answer(onProgressAnswer(t, &notificationservice.OnProgressResponse{
			ReceiverState: []byte("s1"),
		}), nil)
		nt.report(1)
		require.NoError(t, nt.runInFlight())
		require.Equal(t, []byte("s1"), nt.state().GetReceiverState())

		// An async answer is not read, so the saved state stays.
		nt.answer(startOperationResponse(&nexuspb.StartOperationResponse{
			Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
				AsyncSuccess: &nexuspb.StartOperationResponse_Async{OperationToken: "t"},
			},
		}), nil)
		nt.report(2)
		require.NoError(t, nt.runInFlight())
		require.Equal(t, []byte("s1"), nt.onProgressRequest(1).GetReceiverState())
		require.Equal(t, []byte("s1"), nt.state().GetReceiverState())

		nt.answer(onProgressAnswer(t, &notificationservice.OnProgressResponse{
			ReceiverState: []byte("s3"),
		}), nil)
		nt.report(3)
		require.NoError(t, nt.runInFlight())
		require.Equal(t, []byte("s1"), nt.onProgressRequest(2).GetReceiverState())
		require.Equal(t, []byte("s3"), nt.state().GetReceiverState())
	})

	t.Run("BehindRedeliversTheSameProgress", func(t *testing.T) {
		nt := newNexusHandlerProgressTest(t, newNexusHandlerProgressCallback("OnProgress"))
		nt.answer(onProgressAnswer(t, &notificationservice.OnProgressResponse{
			ReceiverState: []byte("page-1"),
			Behind:        true,
		}), nil)
		nt.answer(onProgressAnswer(t, &notificationservice.OnProgressResponse{
			ReceiverState: []byte("page-2"),
		}), nil)
		nt.report(1)
		require.NoError(t, nt.runInFlight())
		state := nt.state()
		require.Equal(t, int64(1), state.GetProgressInFlightCounter(), "the same progress goes again")
		require.Equal(t, []byte("page-1"), state.GetReceiverState())

		require.NoError(t, nt.runInFlight())
		require.Len(t, nt.dispatched, 2)
		req := nt.onProgressRequest(1)
		require.Equal(t, int64(1), req.GetProgress().GetCounter())
		require.Equal(t, []byte("page-1"), req.GetReceiverState())
		state = nt.state()
		require.Zero(t, state.GetProgressInFlight())
		require.Nil(t, state.GetPendingProgress())
		require.Equal(t, []byte("page-2"), state.GetReceiverState())
	})

	t.Run("BehindWaitsTheGap", func(t *testing.T) {
		cb := newNexusHandlerProgressCallback("OnProgress")
		cb.ProgressDeliveries = 1
		cb.ProgressInFlight = 1
		cb.PendingProgress = &callbackspb.Progress{Counter: 1}
		now := time.Unix(1700000000, 0)
		mctx := &chasm.MockMutableContext{
			MockContext: chasm.MockContext{HandleNow: func(chasm.Component) time.Time { return now }},
		}
		_, err := cb.saveProgressResult(mctx, saveProgressInput{
			inFlight:  1,
			sent:      &callbackspb.Progress{Counter: 1},
			result:    progressDelivered,
			reply:     nexusHandlerProgressReply{behind: true},
			behindGap: time.Second,
		})
		require.NoError(t, err)
		require.Len(t, mctx.Tasks, 1)
		require.Equal(t, &callbackspb.ProgressBackoffTask{Delivery: 2}, mctx.Tasks[0].Payload)
		require.Equal(t, now.Add(time.Second), mctx.Tasks[0].Attributes.ScheduledTime)
		require.Equal(t, int64(2), cb.ProgressInFlight)
	})

	t.Run("BehindGivesWayToNewerProgress", func(t *testing.T) {
		nt := newNexusHandlerProgressTest(t, newNexusHandlerProgressCallback("OnProgress"))
		nt.answer(onProgressAnswer(t, &notificationservice.OnProgressResponse{
			ReceiverState: []byte("page-1"),
			Behind:        true,
		}), nil)
		nt.answer(syncSuccessResponse(), nil)
		nt.report(1)
		nt.during = func() { nt.report(4) }
		require.NoError(t, nt.runInFlight())
		require.Equal(t, int64(4), nt.state().GetProgressInFlightCounter())
		require.NoError(t, nt.runInFlight())
		require.Equal(t, int64(4), nt.onProgressRequest(1).GetProgress().GetCounter())
		require.Equal(
			t,
			"request-id/progress/4",
			nt.dispatched[1].GetRequest().GetStartOperation().GetRequestId(),
		)
	})
}
