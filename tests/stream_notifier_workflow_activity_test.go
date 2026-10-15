package tests

import (
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/chasm/lib/streamnotifier"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// workflowActivityOwner drives an owner Workflow whose Activities own streams, next to a Workflow
// reader of those streams. Both run on raw task polls, so a test controls each step.
type workflowActivityOwner struct {
	*activityOwnerReader
	ownerQueue string
	owner      client.WorkflowRun
}

func (s *StreamWakeSuite) newWorkflowActivityOwner(
	opts ...testcore.TestOption,
) *workflowActivityOwner {
	// Options of the test come last, so they win over these.
	opts = append([]testcore.TestOption{
		testcore.WithDynamicConfig(streamnotifier.OwnerStartGrace, time.Second),
		testcore.WithDynamicConfig(streamnotifier.OwnerCloseGrace, time.Second),
	}, opts...)
	r := s.newActivityOwnerReader(opts...)
	o := &workflowActivityOwner{activityOwnerReader: r, ownerQueue: r.taskQueue + "-owner"}
	run, err := r.env.SdkClient().ExecuteWorkflow(
		s.Context(), client.StartWorkflowOptions{TaskQueue: o.ownerQueue}, "agent")
	s.NoError(err)
	o.owner = run
	return o
}

// stream is the reference to the stream that the owner's Activity activityID owns.
func (o *workflowActivityOwner) stream(activityID string) *streampb.StreamReference {
	return &streampb.StreamReference{
		OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY,
		WorkflowId: o.owner.GetID(),
		RunId:      o.owner.GetRunID(),
		ActivityId: activityID,
		Topic:      "tokens",
	}
}

func (o *workflowActivityOwner) pollOwner() *workflowservice.PollWorkflowTaskQueueResponse {
	resp, err := o.env.FrontendClient().PollWorkflowTaskQueue(
		o.s.Context(),
		&workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: o.env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: o.ownerQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		},
	)
	o.s.NoError(err)
	o.s.NotEmpty(resp.GetTaskToken())
	return resp
}

// ownerRuns ends the owner's next Workflow Task with commands.
func (o *workflowActivityOwner) ownerRuns(commands ...*commandpb.Command) {
	task := o.pollOwner()
	_, err := o.env.FrontendClient().RespondWorkflowTaskCompleted(
		o.s.Context(),
		&workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: o.env.Namespace().String(),
			TaskToken: task.GetTaskToken(),
			Commands:  commands,
			Identity:  "test",
		},
	)
	o.s.NoError(err)
}

// schedule is the command that schedules the Activity activityID on the Activities' queue.
func (o *workflowActivityOwner) schedule(
	activityID string,
	retry *commonpb.RetryPolicy,
) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
			ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:          activityID,
				ActivityType:        &commonpb.ActivityType{Name: "tool"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: o.activityQueue},
				StartToCloseTimeout: durationpb.New(time.Minute),
				RetryPolicy:         retry,
			},
		},
	}
}

func completeWorkflow() *commandpb.Command {
	attributes := &commandpb.CompleteWorkflowExecutionCommandAttributes{}
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: attributes,
		},
	}
}

func (s *StreamWakeSuite) TestWorkflowActivityStreamRefsAreChecked() {
	o := s.newWorkflowActivityOwner()
	var invalid *serviceerror.InvalidArgument
	for name, change := range map[string]func(*streampb.StreamReference){
		"no workflow_id": func(r *streampb.StreamReference) { r.WorkflowId = "" },
		"no run_id":      func(r *streampb.StreamReference) { r.RunId = "" },
		"no activity_id": func(r *streampb.StreamReference) { r.ActivityId = "" },
	} {
		ref := o.stream("tool-1")
		change(ref)
		s.ErrorAs(o.attachTo(ref, "sub-0"), &invalid, name)
	}

	// A Workflow and its Activity with the same IDs own different streams.
	stream := o.stream("tool-1")
	workflow := &streampb.StreamReference{
		OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
		WorkflowId: stream.GetWorkflowId(),
		RunId:      stream.GetRunId(),
		Topic:      "tokens",
	}
	s.NoError(o.attachTo(stream, "sub-activity"))
	s.NoError(o.attachTo(workflow, "sub-workflow"))
	s.Len(o.describeOf(stream).GetCallbacks(), 1)
	s.Len(o.describeOf(workflow).GetCallbacks(), 1)
	s.Equal(
		"sub-activity",
		o.describeOf(stream).GetCallbacks()[0].GetWorkflowTarget().GetSubscriptionId(),
	)
}

func (s *StreamWakeSuite) TestTheNotifierWakesAWorkflowReaderOfAWorkflowActivityStream() {
	o := s.newWorkflowActivityOwner()
	o.ownerRuns(o.schedule("tool-1", nil))
	stream := o.stream("tool-1")
	s.NoError(o.attachTo(stream, "sub-1"))
	s.NoError(o.notifyTo(stream, 1, nil))
	task := o.poll()
	wakes := lastScheduledWakes(task)
	s.Len(wakes, 1)
	s.Equal("sub-1", wakes[0].GetSubscriptionId())
	s.Equal(int64(1), wakes[0].GetCounter())
	o.respond(task)

	// The Workflow's Worker closes the stream with the Activity's outcome.
	s.NoError(o.notifyTo(stream, 2, testcore.MustToPayload(s.T(), "outcome")))
	var closeWake *streampb.StreamWake
	s.Await(func(s *StreamWakeSuite) {
		task := o.poll()
		o.respond(task)
		for _, wake := range lastScheduledWakes(task) {
			if wake.GetSubscriptionId() == "sub-1" && wake.GetClosed() {
				closeWake = wake
			}
		}
		s.NotNil(closeWake)
	}, 20*time.Second, 50*time.Millisecond)
	s.Nil(closeWake.GetCloseFailure())
	s.Nil(o.describeOf(stream).GetCloseFailure(), "the Worker's close keeps its outcome")
}

func (s *StreamWakeSuite) TestAWorkflowActivityWithNoCloseClosesAfterBothGraces() {
	o := s.newWorkflowActivityOwner()
	o.ownerRuns(o.schedule("tool-1", nil))
	stream := o.stream("tool-1")
	s.NoError(o.attachTo(stream, "sub-1"))
	// The start grace passes while the Activity is pending.
	time.Sleep(2 * time.Second) //nolint:forbidigo
	s.False(o.describeOf(stream).GetClosed(), "a pending Activity keeps the stream open")

	o.complete(o.pollActivity())
	s.False(o.describeOf(stream).GetClosed(), "the Workflow's Worker gets the close grace")
	failure := o.awaitClosed(stream)
	s.Equal("the owner Activity ended without closing the stream", failure.GetMessage())
}

func (s *StreamWakeSuite) TestAWorkflowActivitysCloseDuringTheGraceWins() {
	o := s.newWorkflowActivityOwner()
	o.ownerRuns(o.schedule("tool-1", nil))
	stream := o.stream("tool-1")
	s.NoError(o.attachTo(stream, "sub-1"))
	o.fail(o.pollActivity(), "Boom", true)
	// Two owner checks see the Activity gone before the Worker closes the stream.
	time.Sleep(1500 * time.Millisecond) //nolint:forbidigo
	s.NoError(o.notifyTo(stream, 3, testcore.MustToPayload(s.T(), "outcome")))
	time.Sleep(2 * time.Second) //nolint:forbidigo
	desc := o.describeOf(stream)
	s.True(desc.GetClosed())
	s.Nil(desc.GetCloseFailure(), "the owner check does not replace the Worker's close")
	s.Equal(int64(3), desc.GetCounter())
}

func (s *StreamWakeSuite) TestAWorkflowActivityScheduledAfterTheAttachKeepsItsStream() {
	o := s.newWorkflowActivityOwner(
		testcore.WithDynamicConfig(streamnotifier.OwnerStartGrace, 3*time.Second),
	)
	stream := o.stream("tool-1")
	s.NoError(o.attachTo(stream, "sub-1"), "the Workflow may link the stream before it schedules")
	time.Sleep(time.Second) //nolint:forbidigo
	s.False(o.describeOf(stream).GetClosed(), "inside the start grace")
	o.ownerRuns(o.schedule("tool-1", nil))
	time.Sleep(4 * time.Second) //nolint:forbidigo
	s.False(o.describeOf(stream).GetClosed(), "the scheduled Activity keeps the stream open")
}

func (s *StreamWakeSuite) TestARunThatClosesWithAPendingActivityClosesItsStreamAtOnce() {
	o := s.newWorkflowActivityOwner()
	o.ownerRuns(o.schedule("tool-1", nil))
	stream := o.stream("tool-1")
	s.NoError(o.attachTo(stream, "sub-1"))
	// An abandoned Activity may still run, but its run is gone.
	_ = o.pollActivity()
	s.NoError(o.env.SdkClient().SignalWorkflow(s.Context(), o.owner.GetID(), "", "done", nil))
	o.ownerRuns(completeWorkflow())
	failure := o.awaitClosed(stream)
	s.Equal("the owner Activity ended without closing the stream", failure.GetMessage())
}

func (s *StreamWakeSuite) TestRetriesAndResetsKeepAWorkflowActivityStreamOpen() {
	o := s.newWorkflowActivityOwner()
	o.ownerRuns(o.schedule("tool-1", &commonpb.RetryPolicy{
		InitialInterval: durationpb.New(time.Second),
		MaximumAttempts: 5,
	}))
	stream := o.stream("tool-1")
	s.NoError(o.attachTo(stream, "sub-1"))
	o.fail(o.pollActivity(), "Retry", false)
	task := o.pollActivity()
	s.Equal(int32(2), task.GetAttempt())
	time.Sleep(3 * time.Second) //nolint:forbidigo
	s.False(o.describeOf(stream).GetClosed(), "a retry keeps the Activity pending")

	// The token of a running attempt goes stale on a reset, so the reset comes in the backoff.
	o.fail(task, "Retry", false)
	_, err := o.env.FrontendClient().ResetActivity(
		s.Context(),
		&workflowservice.ResetActivityRequest{
			Namespace: o.env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: o.owner.GetID()},
			Activity:  &workflowservice.ResetActivityRequest_Id{Id: "tool-1"},
			Identity:  "test",
		},
	)
	s.NoError(err)
	task = o.pollActivity()
	s.Equal(int32(1), task.GetAttempt(), "the reset set the attempt back")
	time.Sleep(3 * time.Second) //nolint:forbidigo
	s.False(o.describeOf(stream).GetClosed(), "a reset keeps the Activity pending")
}

func (s *StreamWakeSuite) TestAMissingRunClosesItsWorkflowActivityStreamAfterTheGrace() {
	o := s.newWorkflowActivityOwner(
		testcore.WithDynamicConfig(streamnotifier.OwnerStartGrace, 3*time.Second),
	)
	stream := o.stream("tool-1")
	stream.RunId = uuid.NewString()
	s.NoError(o.attachTo(stream, "sub-1"), "any run of the Workflow ID is taken")
	time.Sleep(time.Second) //nolint:forbidigo
	s.False(o.describeOf(stream).GetClosed(), "inside the start grace")
	failure := o.awaitClosed(stream)
	s.Equal("the owner Activity ended without closing the stream", failure.GetMessage())
}
