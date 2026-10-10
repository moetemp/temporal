package streamnotifier

import (
	"context"
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
	}, registry, nil, metrics.NoopMetricsHandler, log.NewTestLogger())
	closeWith := func(size int) error {
		_, err := h.NotifyStream(context.Background(), &workflowservice.NotifyStreamRequest{
			Namespace: "ns",
			StreamRef: &streampb.StreamReference{
				OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
				WorkflowId: "wf",
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
