package tests

import (
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	streampb "go.temporal.io/api/stream/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/streamnotifier"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// workflowActivityOwnerEndedFailure is the backstop failure of a stream whose Workflow Activity
// ended with no close.
const workflowActivityOwnerEndedFailure = "the owner Activity ended without closing the stream"

func (s *NexusWorkflowTestSuite) newWorkflowActivityStreamEnv() *activityStreamEnv {
	return s.newActivityStreamEnv(
		testcore.WithDynamicConfig(streamnotifier.OwnerStartGrace, time.Second),
		testcore.WithDynamicConfig(streamnotifier.OwnerCloseGrace, time.Second),
	)
}

// startWorkflowActivity starts an owner Workflow, has it schedule one Activity on the env's
// Activity queue, and answers the reference to the stream that the Activity owns.
func (env *activityStreamEnv) startWorkflowActivity() *streampb.StreamReference {
	ctx := env.s.Context()
	frontend := env.FrontendClient()
	queue := &taskqueuepb.TaskQueue{
		Name: testcore.RandomizeStr(env.s.T().Name() + "-agent"),
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	workflowID := "agent-" + uuid.NewString()
	started, err := frontend.StartWorkflowExecution(
		ctx,
		&workflowservice.StartWorkflowExecutionRequest{
			Namespace:    env.Namespace().String(),
			WorkflowId:   workflowID,
			WorkflowType: &commonpb.WorkflowType{Name: "agent"},
			TaskQueue:    queue,
			RequestId:    uuid.NewString(),
		},
	)
	env.s.NoError(err)
	task, err := frontend.PollWorkflowTaskQueue(
		ctx,
		&workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: queue,
			Identity:  "agent",
		},
	)
	env.s.NoError(err)
	activityID := "tool-" + uuid.NewString()
	attrs := &commandpb.ScheduleActivityTaskCommandAttributes{
		ActivityId:          activityID,
		ActivityType:        &commonpb.ActivityType{Name: "run_tool"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: env.queue},
		StartToCloseTimeout: durationpb.New(time.Minute),
	}
	_, err = frontend.RespondWorkflowTaskCompleted(
		ctx,
		&workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			Identity:  "agent",
			TaskToken: task.GetTaskToken(),
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: attrs,
				},
			}},
		},
	)
	env.s.NoError(err)
	return &streampb.StreamReference{
		OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY,
		WorkflowId: workflowID,
		RunId:      started.GetRunId(),
		ActivityId: activityID,
		Topic:      "tokens",
	}
}

func (s *NexusWorkflowTestSuite) TestWorkflowActivityStreamIdleNexusCaller(chasmEnabled bool) {
	if !chasmEnabled {
		// The notifier env runs on CHASM in both suites, so one run covers it.
		return
	}
	env := s.newWorkflowActivityStreamEnv()
	stream := env.startWorkflowActivity()
	endpointName := env.createRandomExternalNexusServer(
		s.Context(), s.T(), env.attachingHandler(stream, true))
	run, wfExec := env.runCaller(stream, endpointName)
	delivered := func() int64 {
		return env.describe(stream).GetCallbacks()[0].GetDeliveredCounter()
	}

	for counter := int64(1); counter <= 5; counter++ {
		env.notify(stream, counter)
	}
	time.Sleep(2 * time.Second) //nolint:forbidigo // checks that nothing is delivered
	s.Zero(delivered(), "an idle caller gets no progress")

	_, err := env.FrontendClient().WaitStreamCallback(
		s.Context(),
		&workflowservice.WaitStreamCallbackRequest{
			Namespace:    env.Namespace().String(),
			StreamRef:    stream,
			RequestId:    env.describe(stream).GetCallbacks()[0].GetRequestId(),
			AfterCounter: 5,
		},
	)
	s.NoError(err)
	env.notify(stream, 6)
	s.Await(func(s *NexusWorkflowTestSuite) {
		s.Equal(int64(6), delivered())
	}, 10*time.Second, 50*time.Millisecond)
	env.notify(stream, 7)
	time.Sleep(time.Second) //nolint:forbidigo // checks that nothing is delivered
	s.Equal(int64(6), delivered(), "one wait gives one wake")

	// The Activity completes and no close comes. The owner check completes the caller with the
	// backstop failure, but only after both graces.
	env.complete(env.pollActivity())
	s.False(env.describe(stream).GetClosed(), "the Workflow's Worker gets the close grace")
	s.Error(run.Get(s.Context(), nil))
	failed := s.RequireHistoryEvent(
		env.GetHistory(env.Namespace().String(), wfExec),
		enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED,
	)
	cause := failed.GetNexusOperationFailedEventAttributes().GetFailure().GetCause()
	s.Equal(workflowActivityOwnerEndedFailure, cause.GetMessage())
}

func (s *NexusWorkflowTestSuite) TestWorkflowActivityStreamCloseReachesTheNexusCaller(
	chasmEnabled bool,
) {
	if !chasmEnabled {
		return
	}
	env := s.newWorkflowActivityStreamEnv()
	stream := env.startWorkflowActivity()
	endpointName := env.createRandomExternalNexusServer(
		s.Context(), s.T(), env.attachingHandler(stream, false))
	run, _ := env.runCaller(stream, endpointName)

	// The agent's Worker closes the stream when it resolves the Activity, inside the grace.
	env.complete(env.pollActivity())
	_, err := env.FrontendClient().NotifyStream(
		s.Context(),
		&workflowservice.NotifyStreamRequest{
			Namespace: env.Namespace().String(),
			StreamRef: stream,
			Position:  "cursor-1",
			Counter:   1,
			Close:     true,
		},
	)
	s.NoError(err)
	s.NoError(run.Get(s.Context(), nil), "the Worker's close completes the caller")
	s.Nil(env.describe(stream).GetCloseFailure())
}
