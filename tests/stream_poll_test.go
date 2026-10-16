package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/chasm/lib/streamnotifier"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

// StreamPollSuite covers PollStreamNotifier, the long poll a client reader waits on after a read
// that found nothing and armed the store.
type StreamPollSuite struct {
	parallelsuite.Suite[*StreamPollSuite]
}

func TestStreamPollSuite(t *testing.T) {
	parallelsuite.Run(t, &StreamPollSuite{})
}

type streamPollEnv struct {
	*testcore.TestEnv
	stream *streampb.StreamReference
}

func (s *StreamPollSuite) newEnv() *streamPollEnv {
	env := testcore.NewEnv(
		s.T(),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(streamnotifier.Enabled, true),
		testcore.WithDynamicConfig(streamnotifier.PollMaxWait, 2*time.Second),
	)
	return &streamPollEnv{
		TestEnv: env,
		stream: &streampb.StreamReference{
			OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
			WorkflowId: "producer-" + uuid.NewString(),
			RunId:      "first-run",
			Topic:      "tokens",
		},
	}
}

func (s *StreamPollSuite) notify(e *streamPollEnv, counter int64, closeResult *commonpb.Payload) {
	_, err := e.FrontendClient().NotifyStream(s.Context(), &workflowservice.NotifyStreamRequest{
		Namespace:   e.Namespace().String(),
		StreamRef:   e.stream,
		Position:    "cursor",
		Counter:     counter,
		Metadata:    map[string]string{"topic": "tokens"},
		Close:       closeResult != nil,
		CloseResult: closeResult,
	})
	s.NoError(err)
}

func (s *StreamPollSuite) poll(
	e *streamPollEnv,
	after int64,
) *workflowservice.PollStreamNotifierResponse {
	resp, err := e.FrontendClient().PollStreamNotifier(
		s.Context(),
		&workflowservice.PollStreamNotifierRequest{
			Namespace:    e.Namespace().String(),
			StreamRef:    e.stream,
			AfterCounter: after,
		},
	)
	s.NoError(err)
	return resp
}

func (s *StreamPollSuite) TestANotificationBeforeThePollIsNotLost() {
	env := s.newEnv()
	// The producer notifies before the reader polls, and no notifier existed yet.
	s.notify(env, 42, nil)
	started := time.Now()
	resp := s.poll(env, 41)
	s.Equal(int64(42), resp.GetCounter())
	s.Equal("cursor", resp.GetPosition())
	s.Equal("tokens", resp.GetMetadata()["topic"])
	s.Less(time.Since(started), time.Second, "the poll returns at once")
}

func (s *StreamPollSuite) TestANotificationWakesAWaitingPoll() {
	env := s.newEnv()
	s.notify(env, 1, nil)
	done := make(chan *workflowservice.PollStreamNotifierResponse, 1)
	go func() { done <- s.poll(env, 1) }()
	// Either order works: a poll that starts after the notification returns at once.
	s.notify(env, 2, nil)
	resp := <-done
	s.Equal(int64(2), resp.GetCounter())
}

func (s *StreamPollSuite) TestTheWaitLimitReturnsNoChange() {
	env := s.newEnv()
	started := time.Now()
	resp := s.poll(env, 7)
	s.LessOrEqual(resp.GetCounter(), int64(7), "the client reads the stream again")
	s.False(resp.GetClosed())
	s.GreaterOrEqual(time.Since(started), time.Second, "the poll waited")
	s.Less(time.Since(started), 10*time.Second, "streamnotifier.pollMaxWait bounds the wait")

	// The poll started the notifier, so a later notification has somewhere to land.
	desc, err := env.FrontendClient().DescribeStreamNotifier(
		s.Context(),
		&workflowservice.DescribeStreamNotifierRequest{
			Namespace: env.Namespace().String(),
			StreamRef: env.stream,
		},
	)
	s.NoError(err)
	s.Zero(desc.GetCounter())
}

func (s *StreamPollSuite) TestTheCloseEndsThePoll() {
	env := s.newEnv()
	s.notify(env, 1, nil)
	done := make(chan *workflowservice.PollStreamNotifierResponse, 1)
	go func() { done <- s.poll(env, 100) }()
	s.notify(env, 2, testcore.MustToPayload(s.T(), "summary"))
	resp := <-done
	s.True(resp.GetClosed())
	var summary string
	s.NoError(converter.GetDefaultDataConverter().FromPayload(resp.GetCloseResult(), &summary))
	s.Equal("summary", summary)
}
