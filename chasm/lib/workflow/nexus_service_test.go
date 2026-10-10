package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/streamnotifier"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	sdkconverter "go.temporal.io/server/common/sdk"
)

func streamNotifierProcessorContext() chasm.NexusOperationProcessorContext {
	return chasm.NexusOperationProcessorContext{
		Namespace: namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: "ns-id", Name: "ns"},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			"active",
		),
		RequestID: "request-id",
	}
}

func TestNotifyStreamProcessorRoutesToTheStreamsNotifier(t *testing.T) {
	sp := NewWorkflowServiceNexusServiceProcessor(Config{}, nil, nil)
	ref := &streampb.StreamReference{
		OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
		WorkflowId: "owner",
		Topic:      "tokens",
	}
	ctx := streamNotifierProcessorContext()
	ctx.ReserializeInputPayload = true
	result, err := sp.ProcessInput(
		ctx,
		workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.NotifyStream.Name(),
		payloads.MustEncodeSingle(&workflowservice.NotifyStreamRequest{
			StreamRef: ref,
			Counter:   1,
			Identity:  "set by the Workflow",
		}),
	)
	require.NoError(t, err)
	require.Equal(t, chasm.NexusOperationRoutingKeyExecution{
		NamespaceID: "ns-id",
		BusinessID:  streamnotifier.BusinessID(ref),
	}, result.RoutingKey)

	var forwarded workflowservice.NotifyStreamRequest
	require.NoError(t, sdkconverter.PreferProtoDataConverter.FromPayloads(
		&commonpb.Payloads{Payloads: []*commonpb.Payload{result.ReserializedInputPayload}},
		&forwarded,
	))
	require.Equal(t, "ns", forwarded.GetNamespace(), "the namespace comes from the calling Workflow")
	require.Empty(t, forwarded.GetIdentity(), "the identity is not taken from the Workflow")
}

func TestStreamNotifierProcessorsRejectAnotherNamespaceAndABadReference(t *testing.T) {
	sp := NewWorkflowServiceNexusServiceProcessor(Config{}, nil, nil)
	ops := workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService
	good := &streampb.StreamReference{
		OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
		WorkflowId: "owner",
		Topic:      "tokens",
	}
	for name, input := range map[string]struct {
		operation string
		request   any
	}{
		"another namespace": {ops.AttachStreamCallback.Name(), &workflowservice.AttachStreamCallbackRequest{Namespace: "other", StreamRef: good}},
		"no topic":          {ops.DetachStreamCallback.Name(), &workflowservice.DetachStreamCallbackRequest{StreamRef: &streampb.StreamReference{OwnerKind: enumspb.STREAM_OWNER_KIND_WORKFLOW, WorkflowId: "owner"}}},
		"no owner kind":     {ops.NotifyStream.Name(), &workflowservice.NotifyStreamRequest{StreamRef: &streampb.StreamReference{WorkflowId: "owner", Topic: "tokens"}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := sp.ProcessInput(streamNotifierProcessorContext(), input.operation, payloads.MustEncodeSingle(input.request))
			require.Error(t, err)
		})
	}
}

func TestStreamNotifierProcessorRefusesANilRequest(t *testing.T) {
	processor := streamNotifierOperationProcessor[*workflowservice.NotifyStreamRequest]{
		setNamespace:  func(r *workflowservice.NotifyStreamRequest, ns string) { r.Namespace = ns },
		clearIdentity: func(r *workflowservice.NotifyStreamRequest) { r.Identity = "" },
	}
	_, err := processor.ProcessInput(streamNotifierProcessorContext(), nil)
	var invalid *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalid)
}
