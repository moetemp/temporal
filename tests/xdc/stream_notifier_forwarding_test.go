package xdc

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/streamnotifier"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
)

// StreamNotifierForwardingSuite runs the stream notifier across two clusters with the policy that
// forwards only the selected APIs.
type StreamNotifierForwardingSuite struct {
	xdcBaseSuite
}

func TestStreamNotifierForwardingSuite(t *testing.T) {
	t.Parallel()
	s := &StreamNotifierForwardingSuite{}
	s.enableTransitionHistory = true
	suite.Run(t, s)
}

func (s *StreamNotifierForwardingSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS.Key(): 1000,
		dynamicconfig.EnableChasm.Key(): true,
		streamnotifier.Enabled.Key():    true,
		callback.AllowedAddresses.Key(): []any{
			map[string]any{"Pattern": "*", "AllowInsecure": true},
		},
	}
	s.setupSuite(
		testcore.WithDCRedirectionPolicy(
			config.DCRedirectionPolicy{Policy: "selected-apis-forwarding"},
		),
	)
}

func (s *StreamNotifierForwardingSuite) SetupTest() {
	s.setupTest()
}

func (s *StreamNotifierForwardingSuite) TearDownSuite() {
	s.tearDownSuite()
}

// TestWritesForwardedFromStandbyToActive sends the notifier's writes to the standby cluster, which
// forwards them to the active one where the notifier lives.
func (s *StreamNotifierForwardingSuite) TestWritesForwardedFromStandbyToActive() {
	ctx := testcore.NewContext()
	ns := s.createGlobalNamespace()
	stream := &streampb.StreamReference{
		OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
		WorkflowId: "producer-" + uuid.NewString(),
		RunId:      "first-run",
		Topic:      "tokens",
	}
	standby := s.clusters[1].FrontendClient()
	active := s.clusters[0].FrontendClient()

	// The standby learns the namespace through replication before it can forward for it.
	await.RequireTrue(s.T(), func() bool {
		_, err := standby.AttachStreamCallback(ctx, &workflowservice.AttachStreamCallbackRequest{
			Namespace: ns,
			StreamRef: stream,
			RequestId: "a",
			Callback:  &commonpb.Callback_Nexus{Url: "http://caller.invalid/callback"},
		})
		return err == nil
	}, 10*time.Second, 200*time.Millisecond)
	_, err := standby.NotifyStream(ctx, &workflowservice.NotifyStreamRequest{
		Namespace: ns,
		StreamRef: stream,
		Position:  "p1",
		Counter:   1,
	})
	s.NoError(err)

	desc, err := active.DescribeStreamNotifier(ctx, &workflowservice.DescribeStreamNotifierRequest{
		Namespace: ns,
		StreamRef: stream,
	})
	s.NoError(err)
	s.Equal(int64(1), desc.GetCounter())
	s.Equal("p1", desc.GetPosition())
	s.Len(desc.GetCallbacks(), 1)
	s.Equal("a", desc.GetCallbacks()[0].GetRequestId())

	_, err = standby.DetachStreamCallback(ctx, &workflowservice.DetachStreamCallbackRequest{
		Namespace: ns,
		StreamRef: stream,
		RequestId: "a",
	})
	s.NoError(err)
	desc, err = active.DescribeStreamNotifier(ctx, &workflowservice.DescribeStreamNotifierRequest{
		Namespace: ns,
		StreamRef: stream,
	})
	s.NoError(err)
	// A detach completes the caller as canceled, so the callback is scheduled, not waiting.
	s.Len(desc.GetCallbacks(), 1)
	s.NotEqual(enumspb.CALLBACK_STATE_STANDBY, desc.GetCallbacks()[0].GetState())
}

// TestAttachedCallbackSurvivesFailover attaches a callback on the active cluster, fails the
// namespace over, and closes the stream on the new active cluster. The notifier replicates as
// CHASM state, so the callback is there and gets the close.
func (s *StreamNotifierForwardingSuite) TestAttachedCallbackSurvivesFailover() {
	ctx := testcore.NewContext()
	ns := s.createGlobalNamespace()
	stream := &streampb.StreamReference{
		OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
		WorkflowId: "producer-" + uuid.NewString(),
		RunId:      "first-run",
		Topic:      "tokens",
	}
	first := s.clusters[0].FrontendClient()
	second := s.clusters[1].FrontendClient()
	describe := func(client workflowservice.WorkflowServiceClient) (
		*workflowservice.DescribeStreamNotifierResponse, error,
	) {
		return client.DescribeStreamNotifier(ctx, &workflowservice.DescribeStreamNotifierRequest{
			Namespace: ns,
			StreamRef: stream,
		})
	}

	_, err := first.AttachStreamCallback(ctx, &workflowservice.AttachStreamCallbackRequest{
		Namespace: ns,
		StreamRef: stream,
		RequestId: "a",
		Callback:  &commonpb.Callback_Nexus{Url: "http://caller.invalid/callback"},
	})
	s.NoError(err)
	_, err = first.NotifyStream(ctx, &workflowservice.NotifyStreamRequest{
		Namespace: ns,
		StreamRef: stream,
		Position:  "p1",
		Counter:   1,
	})
	s.NoError(err)
	// Describe stays local, so the second cluster answers from its replicated copy.
	await.RequireTrue(s.T(), func() bool {
		desc, err := describe(second)
		return err == nil && desc.GetCounter() == 1 && len(desc.GetCallbacks()) == 1
	}, 10*time.Second, 200*time.Millisecond)

	s.failover(ns, 0, s.clusters[1].ClusterName(), 2)

	_, err = second.NotifyStream(ctx, &workflowservice.NotifyStreamRequest{
		Namespace: ns,
		StreamRef: stream,
		Position:  "p2",
		Counter:   2,
		Close:     true,
	})
	s.NoError(err)
	desc, err := describe(second)
	s.NoError(err)
	s.True(desc.GetClosed())
	s.Equal(int64(2), desc.GetCounter())
	s.Len(desc.GetCallbacks(), 1)
	s.Equal("a", desc.GetCallbacks()[0].GetRequestId())
	s.NotEqual(enumspb.CALLBACK_STATE_STANDBY, desc.GetCallbacks()[0].GetState(),
		"the callback attached before the failover gets the close")
}
