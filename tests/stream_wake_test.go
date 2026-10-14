package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm/lib/streamnotifier"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

// StreamWakeSuite covers stream wakes: each rides a Workflow Task scheduled event, folded per
// reader, with no event of its own.
type StreamWakeSuite struct {
	parallelsuite.Suite[*StreamWakeSuite]
}

func TestStreamWakeSuite(t *testing.T) {
	parallelsuite.Run(t, &StreamWakeSuite{})
}

// wakeReader drives a reader Workflow with raw Workflow Task polls, so a test controls when each
// task starts and how it ends.
type wakeReader struct {
	s         *StreamWakeSuite
	env       *testcore.TestEnv
	taskQueue string
	run       client.WorkflowRun
}

func (s *StreamWakeSuite) newWakeReader(opts ...testcore.TestOption) *wakeReader {
	opts = append(
		opts,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
	)
	r := &wakeReader{
		s:         s,
		env:       testcore.NewEnv(s.T(), opts...),
		taskQueue: testcore.RandomizeStr(s.T().Name()),
	}
	run, err := r.env.SdkClient().ExecuteWorkflow(
		s.Context(), client.StartWorkflowOptions{TaskQueue: r.taskQueue}, "reader")
	s.NoError(err)
	r.run = run
	r.respond(r.poll())
	return r
}

func (r *wakeReader) execution() *commonpb.WorkflowExecution {
	return &commonpb.WorkflowExecution{WorkflowId: r.run.GetID(), RunId: r.run.GetRunID()}
}

func (r *wakeReader) wake(wake *streampb.StreamWake) error {
	_, err := r.env.GetTestCluster().HistoryClient().RecordStreamWake(
		r.s.Context(),
		&historyservice.RecordStreamWakeRequest{
			NamespaceId: r.env.NamespaceID().String(),
			Execution:   r.execution(),
			Wake:        wake,
		},
	)
	return err
}

func (r *wakeReader) wakeAt(subscription string, counter int64) {
	r.s.NoError(r.wake(&streampb.StreamWake{
		SubscriptionId: subscription,
		Position:       "cursor",
		Counter:        counter,
	}))
}

func (r *wakeReader) poll() *workflowservice.PollWorkflowTaskQueueResponse {
	resp, err := r.env.FrontendClient().PollWorkflowTaskQueue(
		r.s.Context(),
		&workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: r.env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: r.taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		},
	)
	r.s.NoError(err)
	r.s.NotEmpty(resp.GetTaskToken())
	return resp
}

func (r *wakeReader) respondWith(
	task *workflowservice.PollWorkflowTaskQueueResponse,
	req *workflowservice.RespondWorkflowTaskCompletedRequest,
) {
	req.Namespace = r.env.Namespace().String()
	req.Identity = "test"
	req.TaskToken = task.GetTaskToken()
	_, err := r.env.FrontendClient().RespondWorkflowTaskCompleted(r.s.Context(), req)
	r.s.NoError(err)
}

func (r *wakeReader) respond(task *workflowservice.PollWorkflowTaskQueueResponse) {
	r.respondWith(task, &workflowservice.RespondWorkflowTaskCompletedRequest{})
}

func (r *wakeReader) history() []*historypb.HistoryEvent {
	return r.env.GetHistory(r.env.Namespace().String(), r.execution())
}

// lastScheduledWakes are the wakes on the scheduled event of the task a poll answered.
func lastScheduledWakes(
	task *workflowservice.PollWorkflowTaskQueueResponse,
) []*streampb.StreamWake {
	events := task.GetHistory().GetEvents()
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
			return events[i].GetWorkflowTaskScheduledEventAttributes().GetStreamWakes()
		}
	}
	return nil
}

func carriedStreamWakes(hist []*historypb.HistoryEvent) []*streampb.StreamWake {
	var carried []*streampb.StreamWake
	for _, event := range hist {
		carried = append(
			carried,
			event.GetWorkflowTaskScheduledEventAttributes().GetStreamWakes()...)
	}
	return carried
}

func (s *StreamWakeSuite) TestAWakeRidesTheNextScheduledEvent() {
	r := s.newWakeReader()
	before := r.history()
	r.wakeAt("reader-1", 42)
	task := r.poll()
	wakes := lastScheduledWakes(task)
	s.Len(wakes, 1)
	s.Equal("reader-1", wakes[0].GetSubscriptionId())
	s.Equal(int64(42), wakes[0].GetCounter())
	s.Equal("cursor", wakes[0].GetPosition())
	r.respond(task)
	for _, event := range r.history()[len(before):] {
		s.Contains([]enumspb.EventType{
			enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
			enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		}, event.GetEventType(), "a wake adds no event of its own")
	}
}

func (s *StreamWakeSuite) TestABurstCostsAtMostOneFollowUpTask() {
	r := s.newWakeReader()
	before := r.history()
	// The first wake schedules a task. With no poll yet, later wakes fold into it.
	r.wakeAt("reader-1", 1)
	r.wakeAt("reader-1", 2)
	r.wakeAt("reader-2", 1)
	r.wakeAt("reader-1", 3)
	first := r.poll()
	s.Len(lastScheduledWakes(first), 1, "the scheduled event carries what it had")
	s.Equal(int64(1), lastScheduledWakes(first)[0].GetCounter())
	r.respond(first)

	followUp := r.poll()
	wakes := lastScheduledWakes(followUp)
	s.Len(wakes, 2, "one follow-up task carries what folded, one wake per reader")
	s.Equal("reader-1", wakes[0].GetSubscriptionId())
	s.Equal(int64(3), wakes[0].GetCounter(), "the reader sees its latest wake")
	s.Equal("reader-2", wakes[1].GetSubscriptionId())
	r.respond(followUp)

	var scheduled int
	for _, event := range r.history()[len(before):] {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
			scheduled++
		}
	}
	s.Equal(2, scheduled)
}

func (s *StreamWakeSuite) TestAWakeOnAPendingTaskWaitsForItsFollowUp() {
	r := s.newWakeReader()
	s.NoError(r.env.SdkClient().SignalWorkflow(
		s.Context(), r.run.GetID(), r.run.GetRunID(), "go", nil))
	r.wakeAt("reader-1", 7)
	signaled := r.poll()
	s.Empty(lastScheduledWakes(signaled),
		"the task the Signal scheduled has its scheduled event written already")
	r.respond(signaled)
	followUp := r.poll()
	s.Len(lastScheduledWakes(followUp), 1, "one follow-up task carries the wake")
	r.respond(followUp)
}

func (s *StreamWakeSuite) TestWakesWaitTheMinInterval() {
	const interval = 2 * time.Second
	r := s.newWakeReader(testcore.WithDynamicConfig(dynamicconfig.StreamWakeMinInterval, interval))
	r.wakeAt("reader-1", 1)
	first := r.poll()
	r.respond(first)

	r.wakeAt("reader-1", 2)
	r.wakeAt("reader-1", 3)
	second := r.poll()
	wakes := lastScheduledWakes(second)
	s.Len(wakes, 1)
	s.Equal(int64(3), wakes[0].GetCounter(), "wakes that waited fold into one task")
	scheduledAt := func(task *workflowservice.PollWorkflowTaskQueueResponse) time.Time {
		events := task.GetHistory().GetEvents()
		for i := len(events) - 1; i >= 0; i-- {
			if events[i].GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
				return events[i].GetEventTime().AsTime()
			}
		}
		return time.Time{}
	}
	s.GreaterOrEqual(scheduledAt(second).Sub(scheduledAt(first)), interval)
	r.respond(second)
}

func (s *StreamWakeSuite) TestAHeartbeatTaskCarriesNoWake() {
	r := s.newWakeReader()
	s.NoError(r.env.SdkClient().SignalWorkflow(
		s.Context(), r.run.GetID(), r.run.GetRunID(), "go", nil))
	running := r.poll()
	// The task is started, so the wake waits for the task after it.
	r.wakeAt("reader-1", 1)
	// The worker heartbeats a long local activity.
	r.respondWith(running, &workflowservice.RespondWorkflowTaskCompletedRequest{
		ForceCreateNewWorkflowTask: true,
	})
	heartbeat := r.poll()
	s.Empty(lastScheduledWakes(heartbeat), "a heartbeat task holds the wake")
	r.respond(heartbeat)

	next := r.poll()
	s.Len(lastScheduledWakes(next), 1, "the first normal task carries it")
	r.respond(next)
}

func (s *StreamWakeSuite) TestAClosedRunAnswersNotFound() {
	r := s.newWakeReader()
	r.wakeAt("reader-1", 1)
	task := r.poll()
	r.respondWith(task, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Commands: []*commandpb.Command{completeWorkflowCmd()},
	})
	s.NoError(r.run.Get(s.Context(), nil))
	var notFound *serviceerror.NotFound
	s.ErrorAs(r.wake(&streampb.StreamWake{SubscriptionId: "reader-1", Counter: 2}), &notFound,
		"the callback stops waking a closed run")
	s.Len(carriedStreamWakes(r.history()), 1)
}

func (s *StreamWakeSuite) TestTheReadersPerRunAreBounded() {
	r := s.newWakeReader(
		testcore.WithDynamicConfig(dynamicconfig.MaximumStreamWakeSources, 2),
	)
	r.wakeAt("reader-1", 1)
	r.wakeAt("reader-2", 1)
	r.wakeAt("reader-1", 2)
	var failed *serviceerror.FailedPrecondition
	s.ErrorAs(r.wake(&streampb.StreamWake{SubscriptionId: "reader-3", Counter: 1}), &failed)
}

// notifierReader is a wake reader with the stream notifier on, and the stream it reads.
type notifierReader struct {
	*wakeReader
	stream *streampb.StreamReference
}

func (s *StreamWakeSuite) newNotifierReader(opts ...testcore.TestOption) *notifierReader {
	opts = append(opts, testcore.WithDynamicConfig(streamnotifier.Enabled, true))
	return &notifierReader{
		wakeReader: s.newWakeReader(opts...),
		stream: &streampb.StreamReference{
			OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
			WorkflowId: "producer-" + uuid.NewString(),
			RunId:      "first-run",
			Topic:      "tokens",
		},
	}
}

func (r *notifierReader) attach(requestID, subscription string) error {
	_, err := r.env.FrontendClient().AttachStreamCallback(
		r.s.Context(),
		&workflowservice.AttachStreamCallbackRequest{
			Namespace: r.env.Namespace().String(),
			StreamRef: r.stream,
			RequestId: requestID,
			WorkflowTarget: &streampb.WorkflowWakeTarget{
				WorkflowId:     r.run.GetID(),
				RunId:          r.run.GetRunID(),
				SubscriptionId: subscription,
			},
		},
	)
	return err
}

func (r *notifierReader) notify(counter int64, closeResult *commonpb.Payload) {
	_, err := r.env.FrontendClient().NotifyStream(
		r.s.Context(),
		&workflowservice.NotifyStreamRequest{
			Namespace:   r.env.Namespace().String(),
			StreamRef:   r.stream,
			Position:    "cursor",
			Counter:     counter,
			Close:       closeResult != nil,
			CloseResult: closeResult,
		},
	)
	r.s.NoError(err)
}

func (r *notifierReader) describe() *workflowservice.DescribeStreamNotifierResponse {
	resp, err := r.env.FrontendClient().DescribeStreamNotifier(
		r.s.Context(),
		&workflowservice.DescribeStreamNotifierRequest{
			Namespace: r.env.Namespace().String(),
			StreamRef: r.stream,
		},
	)
	r.s.NoError(err)
	return resp
}

func (s *StreamWakeSuite) TestTheNotifierWakesAWorkflowReader() {
	r := s.newNotifierReader()
	s.NoError(r.attach("jetty/run/1", "sub-1"))
	r.notify(1, nil)
	task := r.poll()
	wakes := lastScheduledWakes(task)
	s.Len(wakes, 1)
	s.Equal("sub-1", wakes[0].GetSubscriptionId())
	s.Equal(int64(1), wakes[0].GetCounter())
	s.Equal("cursor", wakes[0].GetPosition())
	r.respond(task)

	s.Await(func(s *StreamWakeSuite) {
		callbacks := r.describe().GetCallbacks()
		s.Len(callbacks, 1)
		s.Equal("sub-1", callbacks[0].GetWorkflowTarget().GetSubscriptionId())
		s.Equal(int64(1), callbacks[0].GetDeliveredCounter())
	}, 10*time.Second, 50*time.Millisecond)
}

func (s *StreamWakeSuite) TestAnAttachAfterNotificationsWakesAtOnce() {
	r := s.newNotifierReader()
	r.notify(5, nil)
	s.NoError(r.attach("jetty/run/1", "sub-1"))
	task := r.poll()
	s.Equal(int64(5), lastScheduledWakes(task)[0].GetCounter(),
		"a notification before the attach is not lost")
	r.respond(task)
}

func (s *StreamWakeSuite) TestTheCloseWakesTheReader() {
	r := s.newNotifierReader()
	s.NoError(r.attach("jetty/run/1", "sub-1"))
	r.notify(3, testcore.MustToPayload(s.T(), "summary"))
	var wakes []*streampb.StreamWake
	s.Await(func(s *StreamWakeSuite) {
		task := r.poll()
		r.respond(task)
		wakes = append(wakes, lastScheduledWakes(task)...)
		s.NotEmpty(wakes)
		s.True(wakes[len(wakes)-1].GetClosed())
	}, 10*time.Second, 50*time.Millisecond)
	closed := wakes[len(wakes)-1]
	var summary string
	s.NoError(converter.GetDefaultDataConverter().FromPayload(closed.GetCloseResult(), &summary))
	s.Equal("summary", summary)
	s.Equal(int64(3), closed.GetCounter())
}

func (s *StreamWakeSuite) TestARecheckWakesAnIdleReader() {
	r := s.newNotifierReader(
		testcore.WithDynamicConfig(streamnotifier.RecheckInitial, time.Second),
	)
	s.NoError(r.attach("jetty/run/1", "sub-1"))
	// No producer notifies. The notifier asks the reader to read again after the quiet period.
	task := r.poll()
	wakes := lastScheduledWakes(task)
	s.Len(wakes, 1)
	s.True(wakes[0].GetRecheck())
	s.Zero(wakes[0].GetCounter())
	r.respond(task)
}

func (s *StreamWakeSuite) TestAReaderOfAClosedRunIsDropped() {
	r := s.newNotifierReader()
	s.NoError(r.attach("jetty/run/1", "sub-1"))
	s.NoError(r.env.SdkClient().TerminateWorkflow(
		s.Context(), r.run.GetID(), r.run.GetRunID(), "the reader is done"))
	r.notify(1, nil)
	// The delivery finds the run closed, and the next notification drops the callback.
	s.Await(func(s *StreamWakeSuite) {
		r.notify(2, nil)
		s.Empty(r.describe().GetCallbacks())
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *StreamWakeSuite) TestTheReaderLimitFailsTheAttach() {
	r := s.newNotifierReader(
		testcore.WithDynamicConfig(dynamicconfig.MaximumStreamWakeSources, 1),
	)
	s.NoError(r.attach("jetty/run/1", "sub-1"))
	s.NoError(r.attach("jetty/run/1", "sub-1"), "attaching again changes nothing")
	var failed *serviceerror.FailedPrecondition
	s.ErrorAs(r.attach("jetty/run/2", "sub-2"), &failed)
}

func (s *StreamWakeSuite) TestADetachedReaderFreesItsPlace() {
	r := s.newNotifierReader(
		testcore.WithDynamicConfig(dynamicconfig.MaximumStreamWakeSources, 1),
	)
	s.NoError(r.attach("jetty/run/1", "sub-1"))
	_, err := r.env.FrontendClient().DetachStreamCallback(
		s.Context(),
		&workflowservice.DetachStreamCallbackRequest{
			Namespace: r.env.Namespace().String(),
			StreamRef: r.stream,
			RequestId: "jetty/run/1",
		},
	)
	s.NoError(err)
	// The detach frees the place when the callback's completion runs.
	s.Await(func(s *StreamWakeSuite) {
		s.NoError(r.attach("jetty/run/2", "sub-2"))
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *StreamWakeSuite) TestATakenCloseFreesTheReadersPlace() {
	r := s.newNotifierReader(
		testcore.WithDynamicConfig(dynamicconfig.MaximumStreamWakeSources, 1),
	)
	s.NoError(r.attach("jetty/run/1", "sub-1"))
	r.notify(1, testcore.MustToPayload(s.T(), "summary"))
	s.Await(func(s *StreamWakeSuite) {
		task := r.poll()
		r.respond(task)
		wakes := lastScheduledWakes(task)
		s.NotEmpty(wakes)
		s.True(wakes[len(wakes)-1].GetClosed())
	}, 10*time.Second, 50*time.Millisecond)
	r.stream.Topic = "another"
	s.NoError(r.attach("jetty/run/2", "sub-2"))
}
