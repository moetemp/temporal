package streamnotifier

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
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
	"google.golang.org/grpc/codes"
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
	return &streamnotifierpb.NotifyStreamResponse{
		FrontendResponse: &workflowservice.NotifyStreamResponse{},
	}, nil
}

func TestNotifyStreamCloseResultTakesTheBlobLimit(t *testing.T) {
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().
		GetNamespaceID(namespace.Name("ns")).
		Return(namespace.ID("namespace-id"), nil).
		AnyTimes()
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

func TestNotifyStreamCloseFailureIsChecked(t *testing.T) {
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().
		GetNamespaceID(namespace.Name("ns")).
		Return(namespace.ID("namespace-id"), nil).
		AnyTimes()
	client := &fakeNotifierClient{}
	h := NewFrontendHandler(client, &Config{
		Enabled:            dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		ChasmEnabled:       dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		BlobSizeLimitError: dynamicconfig.GetIntPropertyFnFilteredByNamespace(100),
		BlobSizeLimitWarn:  dynamicconfig.GetIntPropertyFnFilteredByNamespace(50),
		MaxIDLength:        dynamicconfig.GetIntPropertyFn(1000),
	}, registry, nil, metrics.NoopMetricsHandler, log.NewTestLogger())
	notify := func(closeStream bool, result *commonpb.Payload, failure *failurepb.Failure) error {
		_, err := h.NotifyStream(context.Background(), &workflowservice.NotifyStreamRequest{
			Namespace: "ns",
			StreamRef: &streampb.StreamReference{
				OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
				WorkflowId: "wf",
				RunId:      "first-run",
				Topic:      "tokens",
			},
			Counter:      1,
			Close:        closeStream,
			CloseResult:  result,
			CloseFailure: failure,
		})
		return err
	}
	failure := &failurepb.Failure{Message: "boom"}

	var invalid *serviceerror.InvalidArgument
	require.ErrorAs(t, notify(false, nil, failure), &invalid, "a failure needs close")
	require.ErrorAs(
		t,
		notify(true, &commonpb.Payload{Data: []byte("x")}, failure),
		&invalid,
		"a close is a result or a failure, never both",
	)
	require.ErrorAs(
		t,
		notify(true, nil, &failurepb.Failure{Message: string(make([]byte, 200))}),
		&invalid,
		"a close failure over the error limit is refused",
	)
	require.Zero(t, client.notified, "a refused notification never reaches History")
	require.NoError(t, notify(true, nil, failure))
	require.Equal(t, 1, client.notified)
}

func TestNotifierRequestsAreBounded(t *testing.T) {
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().
		GetNamespaceID(namespace.Name("ns")).
		Return(namespace.ID("namespace-id"), nil).
		AnyTimes()
	client := &fakeNotifierClient{}
	h := NewFrontendHandler(client, &Config{
		Enabled:            dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		ChasmEnabled:       dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		BlobSizeLimitError: dynamicconfig.GetIntPropertyFnFilteredByNamespace(1 << 20),
		BlobSizeLimitWarn:  dynamicconfig.GetIntPropertyFnFilteredByNamespace(1 << 20),
		MaxIDLength:        dynamicconfig.GetIntPropertyFn(10),
	}, registry, nil, metrics.NoopMetricsHandler, log.NewTestLogger())
	ref := func(workflowID, topic string) *streampb.StreamReference {
		return &streampb.StreamReference{
			OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
			WorkflowId: workflowID,
			RunId:      "first-run",
			Topic:      topic,
		}
	}
	notify := func(stream *streampb.StreamReference, position string) error {
		_, err := h.NotifyStream(context.Background(), &workflowservice.NotifyStreamRequest{
			Namespace: "ns", StreamRef: stream, Counter: 1, Position: position,
		})
		return err
	}
	var invalid *serviceerror.InvalidArgument
	require.ErrorAs(
		t,
		notify(ref("wf", "t"), strings.Repeat("x", maxPositionBytes+1)),
		&invalid,
		"a position over 1 KiB is refused",
	)
	require.NoError(t, notify(ref("wf", "t"), strings.Repeat("x", maxPositionBytes)))
	require.ErrorAs(
		t,
		notify(ref(strings.Repeat("w", 11), "t"), ""),
		&invalid,
		"a workflow ID over the limit is refused",
	)
	require.ErrorAs(
		t,
		notify(ref("wf", strings.Repeat("t", 11)), ""),
		&invalid,
		"a topic over the limit is refused",
	)
	_, err := h.DetachStreamCallback(
		context.Background(),
		&workflowservice.DetachStreamCallbackRequest{
			Namespace: "ns", StreamRef: ref("wf", "t"), RequestId: strings.Repeat("r", 11),
		},
	)
	require.ErrorAs(t, err, &invalid, "a request ID over the limit is refused")
	noRun := ref("wf", "t")
	noRun.RunId = ""
	require.ErrorAs(
		t,
		notify(noRun, ""),
		&invalid,
		"a reference without the chain's first run is refused",
	)
	_, err = h.AttachStreamCallback(
		context.Background(),
		&workflowservice.AttachStreamCallbackRequest{
			Namespace: "ns",
			StreamRef: noRun,
			RequestId: "r",
			Callback:  &commonpb.Callback_Nexus{Url: "http://caller"},
		},
	)
	require.ErrorAs(t, err, &invalid)
	require.ErrorContains(t, err, "run_id must name the run chain's first run")
	_, err = h.DetachStreamCallback(
		context.Background(),
		&workflowservice.DetachStreamCallbackRequest{
			Namespace: "ns", StreamRef: noRun, RequestId: "r",
		},
	)
	require.ErrorAs(t, err, &invalid, "a detach without the chain's first run is refused")
	require.Equal(t, 1, client.notified, "only the notification within bounds reached History")
}

type fakeAttachClient struct {
	streamnotifierpb.StreamNotifierServiceClient
	attached int
}

func (c *fakeAttachClient) AttachStreamCallback(
	context.Context,
	*streamnotifierpb.AttachStreamCallbackRequest,
	...grpc.CallOption,
) (*streamnotifierpb.AttachStreamCallbackResponse, error) {
	c.attached++
	return &streamnotifierpb.AttachStreamCallbackResponse{
		FrontendResponse: &workflowservice.AttachStreamCallbackResponse{},
	}, nil
}

func TestAttachTakesExactlyOneTarget(t *testing.T) {
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().
		GetNamespaceID(namespace.Name("ns")).
		Return(namespace.ID("namespace-id"), nil).
		AnyTimes()
	client := &fakeAttachClient{}
	h := NewFrontendHandler(client, &Config{
		Enabled:      dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		ChasmEnabled: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		MaxIDLength:  dynamicconfig.GetIntPropertyFn(1000),
	}, registry, nil, metrics.NoopMetricsHandler, log.NewTestLogger())
	attach := func(
		callback *commonpb.Callback_Nexus,
		target *streampb.WorkflowWakeTarget,
	) error {
		req := &workflowservice.AttachStreamCallbackRequest{
			Namespace: "ns",
			StreamRef: &streampb.StreamReference{
				OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
				WorkflowId: "wf",
				RunId:      "first-run",
				Topic:      "tokens",
			},
			RequestId:      "request",
			Callback:       callback,
			WorkflowTarget: target,
		}
		_, err := h.AttachStreamCallback(context.Background(), req)
		return err
	}
	target := func(subscription string) *streampb.WorkflowWakeTarget {
		return &streampb.WorkflowWakeTarget{
			WorkflowId:     "reader",
			RunId:          "run",
			SubscriptionId: subscription,
		}
	}

	var invalid *serviceerror.InvalidArgument
	require.ErrorAs(t, attach(nil, nil), &invalid, "an attach names a target")
	require.ErrorAs(t, attach(&commonpb.Callback_Nexus{Url: "http://caller"}, target("s")), &invalid,
		"an attach names one target")
	require.ErrorAs(t, attach(nil, target("")), &invalid, "a reader names its subscription")
	require.ErrorAs(t, attach(nil, target(strings.Repeat("s", 257))), &invalid,
		"a subscription ID is at most 256 bytes")
	require.ErrorAs(t, attach(nil, &streampb.WorkflowWakeTarget{SubscriptionId: "s"}), &invalid,
		"a reader names its run")
	require.Zero(t, client.attached)
	require.NoError(t, attach(nil, target(strings.Repeat("s", 256))))
	require.Equal(t, 1, client.attached)
}

type batchNotifierClient struct {
	streamnotifierpb.StreamNotifierServiceClient
	mu       sync.Mutex
	notified []*streamnotifierpb.NotifyStreamRequest
	err      error
}

func (c *batchNotifierClient) NotifyStream(
	_ context.Context,
	req *streamnotifierpb.NotifyStreamRequest,
	_ ...grpc.CallOption,
) (*streamnotifierpb.NotifyStreamResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.notified = append(c.notified, req)
	if c.err != nil {
		return nil, c.err
	}
	return &streamnotifierpb.NotifyStreamResponse{
		FrontendResponse: &workflowservice.NotifyStreamResponse{},
	}, nil
}

func TestNotifyStreamsAppliesEachEntry(t *testing.T) {
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().
		GetNamespaceID(namespace.Name("ns")).
		Return(namespace.ID("namespace-id"), nil).
		AnyTimes()
	client := &batchNotifierClient{}
	config := &Config{
		Enabled:        dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		ChasmEnabled:   dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		MaxIDLength:    dynamicconfig.GetIntPropertyFn(1000),
		MaxNotifyBatch: dynamicconfig.GetIntPropertyFnFilteredByNamespace(3),
	}
	h := NewFrontendHandler(client, config, registry, nil, metrics.NoopMetricsHandler,
		log.NewTestLogger())
	entry := func(topic string, counter int64) *streampb.StreamNotification {
		return &streampb.StreamNotification{
			StreamRef: &streampb.StreamReference{
				OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
				WorkflowId: "wf",
				RunId:      "first-run",
				Topic:      topic,
			},
			Position: "cursor",
			Counter:  counter,
			Metadata: map[string]string{"k": "v"},
		}
	}
	notify := func(entries ...*streampb.StreamNotification) ([]codes.Code, error) {
		resp, err := h.NotifyStreams(context.Background(), &workflowservice.NotifyStreamsRequest{
			Namespace:     "ns",
			Notifications: entries,
			Identity:      "driver",
		})
		var got []codes.Code
		for _, result := range resp.GetResults() {
			got = append(got, codes.Code(result.GetCode()))
		}
		return got, err
	}

	var invalid *serviceerror.InvalidArgument
	_, err := notify()
	require.ErrorAs(t, err, &invalid, "a batch has entries")
	_, err = notify(entry("a", 1), entry("b", 1), entry("c", 1), entry("d", 1))
	require.ErrorAs(t, err, &invalid, "a batch has at most streamnotifier.maxNotifyBatch entries")
	require.Empty(t, client.notified)

	got, err := notify(entry("a", 1), entry("a", 2), entry("b", 0))
	require.NoError(t, err)
	require.Equal(t, []codes.Code{codes.OK, codes.InvalidArgument, codes.InvalidArgument}, got,
		"a batch has one entry per stream, and each entry takes the NotifyStream rules")
	require.Len(t, client.notified, 1, "a bad entry holds up no other entry")
	got, err = notify(entry("", 1), entry("c", 1))
	require.NoError(t, err)
	require.Equal(t, []codes.Code{codes.InvalidArgument, codes.OK}, got, "each entry names a stream")
	client.notified = nil

	got, err = notify(entry("a", 5), entry("b", 7))
	require.NoError(t, err)
	require.Equal(t, []codes.Code{codes.OK, codes.OK}, got)
	require.Len(t, client.notified, 2)
	byTopic := map[string]*streamnotifierpb.NotifyStreamRequest{}
	for _, req := range client.notified {
		require.Equal(t, "namespace-id", req.GetNamespaceId())
		require.Equal(t, BusinessID(req.GetFrontendRequest().GetStreamRef()), req.GetBusinessId())
		require.False(t, req.GetFrontendRequest().GetClose())
		require.Equal(t, "driver", req.GetFrontendRequest().GetIdentity())
		require.Equal(t, "cursor", req.GetFrontendRequest().GetPosition())
		byTopic[req.GetFrontendRequest().GetStreamRef().GetTopic()] = req
	}
	require.EqualValues(t, 5, byTopic["a"].GetFrontendRequest().GetCounter())
	require.EqualValues(t, 7, byTopic["b"].GetFrontendRequest().GetCounter())

	config.Enabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
	_, err = notify(entry("a", 6))
	var unimplemented *serviceerror.Unimplemented
	require.ErrorAs(t, err, &unimplemented, "a namespace with the notifier off fails the call")
	config.Enabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)

	client.err = serviceerror.NewUnavailable("shard moved")
	got, err = notify(entry("a", 6))
	require.NoError(t, err)
	require.Equal(t, []codes.Code{codes.Unavailable}, got, "a failed entry fails only itself")
}
