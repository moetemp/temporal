package streamnotifier

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	streamnotifierpb "go.temporal.io/server/chasm/lib/streamnotifier/gen/streamnotifierpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

type fakeNotifierClient struct {
	streamnotifierpb.StreamNotifierServiceClient
	notified int
}

func (c *fakeNotifierClient) NotifyStream(
	context.Context,
	*streamnotifierpb.NotifyStreamRequest,
	...grpc.CallOption,
) (*streamnotifierpb.NotifyStreamResponse, error) {
	c.notified++
	return &streamnotifierpb.NotifyStreamResponse{FrontendResponse: &workflowservice.NotifyStreamResponse{}}, nil
}

func TestNotifyStreamCloseResultTakesTheBlobLimit(t *testing.T) {
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().GetNamespaceID(namespace.Name("ns")).Return(namespace.ID("namespace-id"), nil).AnyTimes()
	client := &fakeNotifierClient{}
	h := NewFrontendHandler(client, &Config{
		Enabled:            dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		ChasmEnabled:       dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		BlobSizeLimitError: dynamicconfig.GetIntPropertyFnFilteredByNamespace(100),
		BlobSizeLimitWarn:  dynamicconfig.GetIntPropertyFnFilteredByNamespace(50),
		MaxIDLength:        dynamicconfig.GetIntPropertyFn(1000),
	}, registry, nil, metrics.NoopMetricsHandler, log.NewTestLogger())
	closeWith := func(size int) error {
		_, err := h.NotifyStream(context.Background(), &workflowservice.NotifyStreamRequest{
			Namespace: "ns",
			StreamRef: &streampb.StreamReference{
				OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
				WorkflowId: "wf",
				RunId:      "first-run",
				Topic:      "tokens",
			},
			Counter:     1,
			Close:       true,
			CloseResult: &commonpb.Payload{Data: make([]byte, size)},
		})
		return err
	}

	var invalid *serviceerror.InvalidArgument
	require.ErrorAs(t, closeWith(200), &invalid, "a close result over the error limit is refused")
	require.Zero(t, client.notified, "a refused notification never reaches History")
	require.NoError(t, closeWith(70), "a close result over only the warning limit is accepted")
	require.Equal(t, 1, client.notified)
}

func TestNotifierRequestsAreBounded(t *testing.T) {
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().GetNamespaceID(namespace.Name("ns")).Return(namespace.ID("namespace-id"), nil).AnyTimes()
	client := &fakeNotifierClient{}
	h := NewFrontendHandler(client, &Config{
		Enabled:            dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		ChasmEnabled:       dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		BlobSizeLimitError: dynamicconfig.GetIntPropertyFnFilteredByNamespace(1 << 20),
		BlobSizeLimitWarn:  dynamicconfig.GetIntPropertyFnFilteredByNamespace(1 << 20),
		MaxIDLength:        dynamicconfig.GetIntPropertyFn(10),
	}, registry, nil, metrics.NoopMetricsHandler, log.NewTestLogger())
	ref := func(workflowID, topic string) *streampb.StreamReference {
		return &streampb.StreamReference{OwnerKind: enumspb.STREAM_OWNER_KIND_WORKFLOW, WorkflowId: workflowID, RunId: "first-run", Topic: topic}
	}
	notify := func(stream *streampb.StreamReference, position string) error {
		_, err := h.NotifyStream(context.Background(), &workflowservice.NotifyStreamRequest{
			Namespace: "ns", StreamRef: stream, Counter: 1, Position: position,
		})
		return err
	}
	var invalid *serviceerror.InvalidArgument
	require.ErrorAs(t, notify(ref("wf", "t"), strings.Repeat("x", maxPositionBytes+1)), &invalid, "a position over 1 KiB is refused")
	require.NoError(t, notify(ref("wf", "t"), strings.Repeat("x", maxPositionBytes)))
	require.ErrorAs(t, notify(ref(strings.Repeat("w", 11), "t"), ""), &invalid, "a workflow ID over the limit is refused")
	require.ErrorAs(t, notify(ref("wf", strings.Repeat("t", 11)), ""), &invalid, "a topic over the limit is refused")
	_, err := h.DetachStreamCallback(context.Background(), &workflowservice.DetachStreamCallbackRequest{
		Namespace: "ns", StreamRef: ref("wf", "t"), RequestId: strings.Repeat("r", 11),
	})
	require.ErrorAs(t, err, &invalid, "a request ID over the limit is refused")
	noRun := ref("wf", "t")
	noRun.RunId = ""
	require.ErrorAs(t, notify(noRun, ""), &invalid, "a reference without the chain's first run is refused")
	_, err = h.AttachStreamCallback(context.Background(), &workflowservice.AttachStreamCallbackRequest{
		Namespace: "ns", StreamRef: noRun, RequestId: "r", Callback: &commonpb.Callback_Nexus{Url: "http://caller"},
	})
	require.ErrorAs(t, err, &invalid)
	require.ErrorContains(t, err, "run_id must name the run chain's first run")
	_, err = h.DetachStreamCallback(context.Background(), &workflowservice.DetachStreamCallbackRequest{
		Namespace: "ns", StreamRef: noRun, RequestId: "r",
	})
	require.ErrorAs(t, err, &invalid, "a detach without the chain's first run is refused")
	require.Equal(t, 1, client.notified, "only the notification within bounds reached History")
}
