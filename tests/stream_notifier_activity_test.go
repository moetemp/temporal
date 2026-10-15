package tests

import (
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/streamnotifier"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// activityOwnerReader is a Workflow reader of streams that standalone Activities own.
type activityOwnerReader struct {
	*wakeReader
	activityQueue string
}

func (s *StreamWakeSuite) newActivityOwnerReader(opts ...testcore.TestOption) *activityOwnerReader {
	opts = append(
		opts,
		testcore.WithDynamicConfig(streamnotifier.Enabled, true),
		testcore.WithDynamicConfig(streamnotifier.OwnerCheckInterval, time.Second),
		testcore.WithDynamicConfig(activity.EnableStandaloneActivityOperatorCommands, true),
	)
	r := s.newWakeReader(opts...)
	return &activityOwnerReader{wakeReader: r, activityQueue: r.taskQueue + "-activities"}
}

// start starts a standalone Activity and answers the reference to its stream.
func (r *activityOwnerReader) start(
	activityID string,
	options func(*workflowservice.StartActivityExecutionRequest),
) *streampb.StreamReference {
	req := &workflowservice.StartActivityExecutionRequest{
		Namespace:           r.env.Namespace().String(),
		ActivityId:          activityID,
		ActivityType:        &commonpb.ActivityType{Name: "produce"},
		Identity:            "test",
		TaskQueue:           &taskqueuepb.TaskQueue{Name: r.activityQueue},
		StartToCloseTimeout: durationpb.New(time.Minute),
		RequestId:           uuid.NewString(),
	}
	if options != nil {
		options(req)
	}
	resp, err := r.env.FrontendClient().StartActivityExecution(r.s.Context(), req)
	r.s.NoError(err)
	return activityStream(activityID, resp.GetRunId())
}

func activityStream(activityID, runID string) *streampb.StreamReference {
	return &streampb.StreamReference{
		OwnerKind:  enumspb.STREAM_OWNER_KIND_ACTIVITY,
		ActivityId: activityID,
		RunId:      runID,
		Topic:      "tokens",
	}
}

func (r *activityOwnerReader) attachTo(stream *streampb.StreamReference, subscription string) error {
	_, err := r.env.FrontendClient().AttachStreamCallback(
		r.s.Context(),
		&workflowservice.AttachStreamCallbackRequest{
			Namespace: r.env.Namespace().String(),
			StreamRef: stream,
			RequestId: "jetty/" + subscription,
			WorkflowTarget: &streampb.WorkflowWakeTarget{
				WorkflowId:     r.run.GetID(),
				RunId:          r.run.GetRunID(),
				SubscriptionId: subscription,
			},
		},
	)
	return err
}

func (r *activityOwnerReader) notifyTo(
	stream *streampb.StreamReference,
	counter int64,
	closeResult *commonpb.Payload,
) error {
	_, err := r.env.FrontendClient().NotifyStream(
		r.s.Context(),
		&workflowservice.NotifyStreamRequest{
			Namespace:   r.env.Namespace().String(),
			StreamRef:   stream,
			Position:    "cursor",
			Counter:     counter,
			Close:       closeResult != nil,
			CloseResult: closeResult,
		},
	)
	return err
}

func (r *activityOwnerReader) describeOf(
	stream *streampb.StreamReference,
) *workflowservice.DescribeStreamNotifierResponse {
	resp, err := r.env.FrontendClient().DescribeStreamNotifier(
		r.s.Context(),
		&workflowservice.DescribeStreamNotifierRequest{
			Namespace: r.env.Namespace().String(),
			StreamRef: stream,
		},
	)
	r.s.NoError(err)
	return resp
}

// awaitClosed waits for the owner check to close the stream, and answers its close failure.
func (r *activityOwnerReader) awaitClosed(stream *streampb.StreamReference) *failurepb.Failure {
	var desc *workflowservice.DescribeStreamNotifierResponse
	r.s.Await(func(s *StreamWakeSuite) {
		desc = r.describeOf(stream)
		s.True(desc.GetClosed())
	}, 20*time.Second, 100*time.Millisecond)
	return desc.GetCloseFailure()
}

// pollActivity takes the next task of the Activities' queue.
func (r *activityOwnerReader) pollActivity() *workflowservice.PollActivityTaskQueueResponse {
	resp, err := r.env.FrontendClient().PollActivityTaskQueue(
		r.s.Context(),
		&workflowservice.PollActivityTaskQueueRequest{
			Namespace: r.env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: r.activityQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		},
	)
	r.s.NoError(err)
	r.s.NotEmpty(resp.GetTaskToken())
	return resp
}

func (r *activityOwnerReader) complete(task *workflowservice.PollActivityTaskQueueResponse) {
	_, err := r.env.FrontendClient().RespondActivityTaskCompleted(
		r.s.Context(),
		&workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: r.env.Namespace().String(),
			TaskToken: task.GetTaskToken(),
			Result: &commonpb.Payloads{
				Payloads: []*commonpb.Payload{
					testcore.MustToPayload(r.s.T(), "the activity's own result"),
				},
			},
			Identity: "test",
		},
	)
	r.s.NoError(err)
}

func (r *activityOwnerReader) fail(
	task *workflowservice.PollActivityTaskQueueResponse,
	failureType string,
	nonRetryable bool,
) {
	_, err := r.env.FrontendClient().RespondActivityTaskFailed(
		r.s.Context(),
		&workflowservice.RespondActivityTaskFailedRequest{
			Namespace: r.env.Namespace().String(),
			TaskToken: task.GetTaskToken(),
			Failure: &failurepb.Failure{
				Message: "boom",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						Type:         failureType,
						NonRetryable: nonRetryable,
					},
				},
			},
			Identity: "test",
		},
	)
	r.s.NoError(err)
}

func (s *StreamWakeSuite) TestActivityStreamRefsAreChecked() {
	r := s.newActivityOwnerReader()
	id := "act-" + uuid.NewString()
	stream := r.start(id, nil)
	var invalid *serviceerror.InvalidArgument

	withWorkflow := activityStream(id, stream.GetRunId())
	withWorkflow.WorkflowId = id
	s.ErrorAs(r.attachTo(withWorkflow, "sub-0"), &invalid, "an Activity ref names no Workflow")
	noRun := activityStream(id, "")
	s.ErrorAs(r.attachTo(noRun, "sub-0"), &invalid, "an Activity ref names its run")
	s.ErrorAs(
		r.attachTo(activityStream(id, "no-such-run"), "sub-0"),
		&invalid,
		"a run the Activity ID never had is refused",
	)

	// A Workflow and an Activity with the same ID own different streams.
	workflow := &streampb.StreamReference{
		OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
		WorkflowId: id,
		RunId:      stream.GetRunId(),
		Topic:      "tokens",
	}
	s.NoError(r.attachTo(stream, "sub-activity"))
	s.NoError(r.attachTo(workflow, "sub-workflow"))
	s.Len(r.describeOf(stream).GetCallbacks(), 1)
	s.Len(r.describeOf(workflow).GetCallbacks(), 1)
	s.Equal("sub-activity", r.describeOf(stream).GetCallbacks()[0].GetWorkflowTarget().GetSubscriptionId())
}

func (s *StreamWakeSuite) TestActivityStreamsNeedStandaloneActivities() {
	r := s.newActivityOwnerReader(testcore.WithDynamicConfig(activity.Enabled, false))
	var precondition *serviceerror.FailedPrecondition
	err := r.notifyTo(activityStream("act", "run"), 1, nil)
	s.ErrorAs(err, &precondition, "never Unimplemented, which turns off every stream in Core")
}

func (s *StreamWakeSuite) TestTheNotifierWakesAWorkflowReaderOfAnActivityStream() {
	r := s.newActivityOwnerReader()
	stream := r.start("act-"+uuid.NewString(), nil)
	s.NoError(r.attachTo(stream, "sub-1"))
	s.NoError(r.notifyTo(stream, 1, nil))
	task := r.poll()
	wakes := lastScheduledWakes(task)
	s.Len(wakes, 1)
	s.Equal("sub-1", wakes[0].GetSubscriptionId())
	s.Equal(int64(1), wakes[0].GetCounter())
	r.respond(task)

	callbacks := r.describeOf(stream).GetCallbacks()
	s.Len(callbacks, 1)
	s.Equal("sub-1", callbacks[0].GetWorkflowTarget().GetSubscriptionId())
}

func (s *StreamWakeSuite) TestAnActivityThatEndsWithNoCloseClosesItsStream() {
	r := s.newActivityOwnerReader()

	// Completed gives a null result, not the Activity's own result.
	completed := r.start("act-completed-"+uuid.NewString(), nil)
	s.NoError(r.attachTo(completed, "sub-completed"))
	r.complete(r.pollActivity())
	s.Nil(r.awaitClosed(completed))
	var closeWake *streampb.StreamWake
	s.Await(func(s *StreamWakeSuite) {
		task := r.poll()
		r.respond(task)
		for _, wake := range lastScheduledWakes(task) {
			if wake.GetSubscriptionId() == "sub-completed" && wake.GetClosed() {
				closeWake = wake
			}
		}
		s.NotNil(closeWake)
	}, 20*time.Second, 50*time.Millisecond)
	s.Nil(closeWake.GetCloseResult())
	s.Nil(closeWake.GetCloseFailure())

	failed := r.start("act-failed-"+uuid.NewString(), nil)
	s.NoError(r.attachTo(failed, "sub-failed"))
	r.fail(r.pollActivity(), "Boom", true)
	failure := r.awaitClosed(failed)
	s.Equal("boom", failure.GetMessage())
	s.Equal("Boom", failure.GetApplicationFailureInfo().GetType())

	terminated := r.start("act-terminated-"+uuid.NewString(), nil)
	s.NoError(r.attachTo(terminated, "sub-terminated"))
	_, err := r.env.FrontendClient().TerminateActivityExecution(
		s.Context(),
		&workflowservice.TerminateActivityExecutionRequest{
			Namespace:  r.env.Namespace().String(),
			ActivityId: terminated.GetActivityId(),
			RunId:      terminated.GetRunId(),
			Reason:     "the operator stopped it",
			Identity:   "test",
			RequestId:  uuid.NewString(),
		},
	)
	s.NoError(err)
	s.NotNil(r.awaitClosed(terminated).GetTerminatedFailureInfo())

	canceled := r.start("act-canceled-"+uuid.NewString(), nil)
	s.NoError(r.attachTo(canceled, "sub-canceled"))
	_, err = r.env.FrontendClient().RequestCancelActivityExecution(
		s.Context(),
		&workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  r.env.Namespace().String(),
			ActivityId: canceled.GetActivityId(),
			RunId:      canceled.GetRunId(),
			Reason:     "the client gave up",
			Identity:   "test",
			RequestId:  uuid.NewString(),
		},
	)
	s.NoError(err)
	s.NotNil(r.awaitClosed(canceled).GetCanceledFailureInfo())

	timedOut := r.start(
		"act-timeout-"+uuid.NewString(),
		func(req *workflowservice.StartActivityExecutionRequest) {
			req.ScheduleToCloseTimeout = durationpb.New(time.Second)
			req.StartToCloseTimeout = durationpb.New(time.Second)
		},
	)
	s.NoError(r.attachTo(timedOut, "sub-timeout"))
	// No Worker takes the task, so the Activity's own failure names the schedule-to-start limit.
	s.Equal(
		enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		r.awaitClosed(timedOut).GetTimeoutFailureInfo().GetTimeoutType(),
	)
}

func (s *StreamWakeSuite) TestAnActivitysOwnCloseWins() {
	r := s.newActivityOwnerReader()
	stream := r.start("act-"+uuid.NewString(), nil)
	s.NoError(r.attachTo(stream, "sub-1"))
	task := r.pollActivity()
	s.NoError(r.notifyTo(stream, 3, testcore.MustToPayload(s.T(), "summary")))
	r.fail(task, "Boom", true)
	// Two owner checks would have run by now if the stream were open.
	time.Sleep(2 * time.Second) //nolint:forbidigo
	desc := r.describeOf(stream)
	s.True(desc.GetClosed())
	s.Nil(desc.GetCloseFailure(), "the owner's failure does not replace its own close")
	s.Equal(int64(3), desc.GetCounter())
}

func (s *StreamWakeSuite) TestRetriesAndResetsKeepAnActivityStreamOpen() {
	r := s.newActivityOwnerReader()
	stream := r.start(
		"act-"+uuid.NewString(),
		func(req *workflowservice.StartActivityExecutionRequest) {
			req.RetryPolicy = &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(time.Second),
				MaximumAttempts: 3,
			}
		},
	)
	s.NoError(r.attachTo(stream, "sub-1"))
	r.fail(r.pollActivity(), "Retry", false)
	task := r.pollActivity()
	s.Equal(int32(2), task.GetAttempt())
	time.Sleep(2 * time.Second) //nolint:forbidigo
	s.False(r.describeOf(stream).GetClosed(), "a retry keeps the run")

	_, err := r.env.FrontendClient().ResetActivityExecution(
		s.Context(),
		&workflowservice.ResetActivityExecutionRequest{
			Namespace:  r.env.Namespace().String(),
			ActivityId: stream.GetActivityId(),
			RunId:      stream.GetRunId(),
			Identity:   "test",
		},
	)
	s.NoError(err)
	r.fail(task, "Retry", false)
	task = r.pollActivity()
	s.Equal(int32(1), task.GetAttempt(), "the reset set the attempt back")
	time.Sleep(2 * time.Second) //nolint:forbidigo
	s.False(r.describeOf(stream).GetClosed(), "a reset keeps the run")

	r.complete(task)
	s.Nil(r.awaitClosed(stream))
}

func (s *StreamWakeSuite) TestAnActivityThatNeverStartsClosesAfterTheGrace() {
	r := s.newActivityOwnerReader(
		testcore.WithDynamicConfig(streamnotifier.OwnerStartGrace, 3*time.Second),
	)
	stream := activityStream("act-"+uuid.NewString(), "a-run-nobody-started")
	s.NoError(r.attachTo(stream, "sub-1"), "the owner may start after the reader subscribes")
	time.Sleep(time.Second) //nolint:forbidigo
	s.False(r.describeOf(stream).GetClosed(), "inside the grace")
	failure := r.awaitClosed(stream)
	s.Equal("the owner Activity ended without closing the stream", failure.GetMessage())
}

func (s *StreamWakeSuite) TestAReusedActivityIDOwnsNewStreams() {
	r := s.newActivityOwnerReader()
	id := "act-" + uuid.NewString()
	first := r.start(id, nil)
	r.complete(r.pollActivity())
	second := r.start(id, func(req *workflowservice.StartActivityExecutionRequest) {
		req.IdReusePolicy = enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE
	})
	s.NotEqual(first.GetRunId(), second.GetRunId())

	s.NoError(r.attachTo(first, "sub-first"), "an earlier run keys its own notifier")
	s.NoError(r.attachTo(second, "sub-second"))
	s.Equal(
		"the owner Activity ended without closing the stream",
		r.awaitClosed(first).GetMessage(),
	)
	s.False(r.describeOf(second).GetClosed(), "the new run's stream stays open")
	s.Len(r.describeOf(second).GetCallbacks(), 1)
}
