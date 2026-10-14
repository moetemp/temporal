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
			EnableProgress: func(string) bool { return npt.progressEnabled },
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
