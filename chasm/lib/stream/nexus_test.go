package stream

import (
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/streamservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
)

func TestNexusErrorFailsTheOperationWithTheReason(t *testing.T) {
	err := nexusError(NewError(ReasonClosed, "The Stream is closed"))
	opErr, ok := err.(*nexus.OperationError)
	require.True(t, ok)
	require.Equal(t, nexus.OperationStateFailed, opErr.State)

	full := serviceerror.NewResourceExhausted(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "full")
	_, ok = nexusError(full).(*nexus.HandlerError)
	require.True(t, ok)

	unavailable := serviceerror.NewUnavailable("down")
	require.Equal(t, unavailable, nexusError(unavailable))
}

func TestProcessorRoutesByStreamID(t *testing.T) {
	ns := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id:   "ns-id",
			Name: "ns",
		},
		&persistencespb.NamespaceConfig{},
		"active",
	)
	collection := dynamicconfig.NewMemoryClient()
	collection.OverrideSetting(Enabled, true)
	sp := newNexusServiceProcessor(
		ConfigProvider(dynamicconfig.NewCollection(collection, log.NewNoopLogger())),
	)
	ctx := chasm.NexusOperationProcessorContext{Namespace: ns, ReserializeInputPayload: true}

	input := payloads.MustEncodeSingle(
		&streamservice.ReadRecordsRequest{Stream: &streamservice.StreamRef{Id: "chat-1"}},
	)
	result, err := sp.ProcessInput(ctx, "ReadRecords", input)
	require.NoError(t, err)
	require.Equal(
		t,
		chasm.NexusOperationRoutingKeyExecution{NamespaceID: "ns-id", BusinessID: "chat-1"},
		result.RoutingKey,
	)
	var routed streamservice.ReadRecordsRequest
	require.NoError(
		t,
		payloads.Decode(
			&commonpb.Payloads{Payloads: []*commonpb.Payload{result.ReserializedInputPayload}},
			&routed,
		),
	)
	require.Equal(t, "ns", routed.GetNamespace())

	// A Workflow Stream is on its Workflow's shard.
	result, err = sp.ProcessInput(
		ctx,
		"CloseStream",
		payloads.MustEncodeSingle(&streamservice.CloseStreamRequest{
			Stream: &streamservice.StreamRef{
				Id:                  "chat-1",
				WorkflowId:          "wf",
				FirstExecutionRunId: "first",
			},
		}),
	)
	require.NoError(t, err)
	require.Equal(
		t,
		chasm.NexusOperationRoutingKeyExecution{NamespaceID: "ns-id", BusinessID: "wf"},
		result.RoutingKey,
	)

	_, err = sp.ProcessInput(
		ctx,
		"ReadRecords",
		payloads.MustEncodeSingle(&streamservice.ReadRecordsRequest{}),
	)
	require.ErrorContains(t, err, ReasonInvalid)
	_, err = sp.ProcessInput(
		ctx,
		"ReadRecords",
		payloads.MustEncodeSingle(&streamservice.ReadRecordsRequest{
			Namespace: "other",
			Stream:    &streamservice.StreamRef{Id: "chat-1"},
		}),
	)
	require.ErrorContains(t, err, "does not match")
}
