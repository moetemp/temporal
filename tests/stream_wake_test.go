package tests

import (
	"testing"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/api/historyservice/v1"
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
	// The first wake schedules a task. Later wakes wait, folded, for the task after it.
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
	s.Len(wakes, 2, "one follow-up task carries what waited, one wake per reader")
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

func (s *StreamWakeSuite) TestAHeartbeatTaskCarriesTheWake() {
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
	s.Len(lastScheduledWakes(heartbeat), 1,
		"SDKs end a heartbeat chain at a task with attachments, so the server holds nothing")
	r.respond(heartbeat)
}

func (s *StreamWakeSuite) TestAWakeWaitsForTheStartDelay() {
	r := &wakeReader{
		s: s,
		env: testcore.NewEnv(
			s.T(),
			testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
			testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		),
		taskQueue: testcore.RandomizeStr(s.T().Name()),
	}
	run, err := r.env.SdkClient().ExecuteWorkflow(
		s.Context(),
		client.StartWorkflowOptions{TaskQueue: r.taskQueue, StartDelay: time.Hour},
		"reader",
	)
	s.NoError(err)
	r.run = run
	r.wakeAt("reader-1", 1)
	for _, event := range r.history() {
		s.NotEqual(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, event.GetEventType(),
			"the first task waits for the start delay")
	}
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
