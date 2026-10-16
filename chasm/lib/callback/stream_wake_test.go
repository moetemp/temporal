package callback

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/log"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func streamWakeTarget() *callbackspb.Callback_StreamWake {
	return &callbackspb.Callback_StreamWake{
		WorkflowId:     "reader",
		RunId:          "run",
		SubscriptionId: "sub",
	}
}

// historyAnswering is a History client that records each stream wake and answers it with err.
func historyAnswering(
	t *testing.T,
	err error,
	got *[]*historyservice.RecordStreamWakeRequest,
) *historyservicemock.MockHistoryServiceClient {
	client := historyservicemock.NewMockHistoryServiceClient(gomock.NewController(t))
	client.EXPECT().RecordStreamWake(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			req *historyservice.RecordStreamWakeRequest,
			_ ...grpc.CallOption,
		) (*historyservice.RecordStreamWakeResponse, error) {
			*got = append(*got, req)
			return &historyservice.RecordStreamWakeResponse{}, err
		}).AnyTimes()
	return client
}

func TestStreamWakeProgressDelivery(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		result progressResult
	}{
		{name: "Delivered", result: progressDelivered},
		{
			name:   "AClosedRunMarksTheReceiverClosed",
			err:    serviceerror.NewNotFound("workflow execution already completed"),
			result: progressReceiverClosed,
		},
		{
			name:   "TheReaderLimitTurnsProgressOff",
			err:    serviceerror.NewFailedPrecondition("too many readers"),
			result: progressRefused,
		},
		{
			name:   "UnavailableRetries",
			err:    serviceerror.NewUnavailable("busy"),
			result: progressRetry,
		},
		{
			name:   "AFailoverRetries",
			err:    serviceerror.NewNamespaceNotActive("ns", "standby", "active"),
			result: progressRetry,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []*historyservice.RecordStreamWakeRequest
			h := &progressTaskHandler{invocation: &invocationTaskHandler{
				historyClient: historyAnswering(t, tc.err, &got),
				logger:        log.NewTestLogger(),
			}}
			result, err := deliverStreamWakeProgress(
				context.Background(),
				h,
				chasm.ComponentRef{ExecutionKey: chasm.ExecutionKey{NamespaceID: "ns-id"}},
				progressInvocation{
					callback: &callbackspb.Callback{
						Variant: &callbackspb.Callback_StreamWake_{StreamWake: streamWakeTarget()},
					},
					progress: &callbackspb.Progress{
						Position: "p",
						Counter:  9,
						Metadata: map[string]string{"k": "v"},
						Recheck:  true,
					},
				},
			)
			require.Equal(t, tc.result, result)
			require.Equal(t, tc.err == nil, err == nil)
			require.Len(t, got, 1)
			require.Equal(t, "ns-id", got[0].GetNamespaceId(), "the reader is in the namespace")
			require.Equal(t, "reader", got[0].GetExecution().GetWorkflowId())
			require.Equal(t, "run", got[0].GetExecution().GetRunId())
			require.True(t, got[0].GetOnlyIfReserved(), "a wake takes no place it lost")
			require.True(t, proto.Equal(&streampb.StreamWake{
				SubscriptionId: "sub",
				Position:       "p",
				Counter:        9,
				Metadata:       map[string]string{"k": "v"},
				Recheck:        true,
			}, got[0].GetWake()))
		})
	}
}

func TestStreamWakeCompletion(t *testing.T) {
	closeWake := &streampb.StreamWake{
		Counter:     4,
		Closed:      true,
		CloseResult: &commonpb.Payload{Data: []byte("done")},
	}
	for _, tc := range []struct {
		name   string
		wake   *streampb.StreamWake
		err    error
		result invocationResult
		calls  int
	}{
		{name: "TheCloseReachesTheReader", wake: closeWake, result: invocationResultOK{}, calls: 1},
		{name: "ADetachedReaderFreesItsPlace", result: invocationResultOK{}, calls: 1},
		{
			name:   "ADetachedReaderOfAClosedRunFreesNothing",
			err:    serviceerror.NewNotFound("workflow execution already completed"),
			result: invocationResultOK{},
			calls:  1,
		},
		{
			name:   "AClosedRunNeedsNone",
			wake:   closeWake,
			err:    serviceerror.NewNotFound("workflow execution already completed"),
			result: invocationResultOK{},
			calls:  1,
		},
		{
			name:   "UnavailableRetries",
			wake:   closeWake,
			err:    serviceerror.NewUnavailable("busy"),
			result: invocationResultRetry{},
			calls:  1,
		},
		{
			name:   "InvalidArgumentFails",
			wake:   closeWake,
			err:    serviceerror.NewInvalidArgument("bad"),
			result: invocationResultFail{},
			calls:  1,
		},
		{
			name:   "FailedPreconditionFails",
			wake:   closeWake,
			err:    serviceerror.NewFailedPrecondition("no CHASM Workflow"),
			result: invocationResultFail{},
			calls:  1,
		},
		{
			name:   "AFailoverRetries",
			wake:   closeWake,
			err:    serviceerror.NewNamespaceNotActive("ns", "standby", "active"),
			result: invocationResultRetry{},
			calls:  1,
		},
		{
			name:   "ADetachDuringAFailoverRetries",
			err:    serviceerror.NewNamespaceNotActive("ns", "standby", "active"),
			result: invocationResultRetry{},
			calls:  1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []*historyservice.RecordStreamWakeRequest
			h := &invocationTaskHandler{historyClient: historyAnswering(t, tc.err, &got)}
			result := invocableStreamWake{
				namespaceID: "ns-id",
				target:      streamWakeTarget(),
				wake:        tc.wake,
			}.Invoke(context.Background(), nil, h, nil, chasm.TaskAttributes{})
			require.IsType(t, tc.result, result)
			require.Len(t, got, tc.calls)
			if tc.calls > 0 {
				require.Equal(t, "sub", got[0].GetWake().GetSubscriptionId())
				require.Equal(t, tc.wake == nil, got[0].GetRelease())
				require.True(t, got[0].GetOnlyIfReserved(), "a wake takes no place it lost")
			}
			if tc.calls > 0 && tc.wake != nil {
				require.True(t, got[0].GetWake().GetClosed())
				require.Equal(t, []byte("done"), got[0].GetWake().GetCloseResult().GetData())
			}
		})
	}
	retry := invocationResultRetry{err: errors.New("x")}
	require.NoError(t, invocableStreamWake{}.WrapError(retry, nil),
		"a retry does not count against a destination")
}
