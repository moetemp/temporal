package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/notificationservice/v1"
	streampb "go.temporal.io/api/stream/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/callback"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/chasm/lib/streamnotifier"
	"go.temporal.io/server/common/callbacks"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// activityStreamEnv starts standalone Activities that own streams, and drives their attempts.
type activityStreamEnv struct {
	*NexusTestEnv
	s     *NexusWorkflowTestSuite
	queue string
}

func (s *NexusWorkflowTestSuite) newActivityStreamEnv(
	opts ...testcore.TestOption,
) *activityStreamEnv {
	opts = append(
		opts,
		testcore.WithDynamicConfig(chasmnexus.EnableProgress, true),
		testcore.WithDynamicConfig(streamnotifier.Enabled, true),
		testcore.WithDynamicConfig(streamnotifier.OwnerCheckInterval, time.Second),
		testcore.WithDynamicConfig(
			callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
		),
		testcore.WithDynamicConfig(callback.RetryPolicyInitialInterval, 10*time.Millisecond),
		testcore.WithDynamicConfig(callback.RetryPolicyMaximumInterval, 50*time.Millisecond),
		testcore.WithDynamicConfig(
			chasmnexus.EnabledCallbackKinds,
			[]callbacks.Kind{callbacks.KindNexus, callbacks.KindNexusHandler},
		),
	)
	return &activityStreamEnv{
		NexusTestEnv: s.newTestEnv(true, opts...),
		s:            s,
		queue:        testcore.RandomizeStr("activities-" + s.T().Name()),
	}
}

// start starts a standalone Activity and answers the reference to its stream.
func (env *activityStreamEnv) start() *streampb.StreamReference {
	activityID := "tool-" + uuid.NewString()
	resp, err := env.FrontendClient().StartActivityExecution(
		env.s.Context(),
		&workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "run_tool"},
			Identity:            "test",
			TaskQueue:           &taskqueuepb.TaskQueue{Name: env.queue},
			StartToCloseTimeout: durationpb.New(time.Minute),
			RequestId:           uuid.NewString(),
		},
	)
	env.s.NoError(err)
	return activityStream(activityID, resp.GetRunId())
}

func (env *activityStreamEnv) pollActivity() *workflowservice.PollActivityTaskQueueResponse {
	resp, err := env.FrontendClient().PollActivityTaskQueue(
		env.s.Context(),
		&workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: env.queue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		},
	)
	env.s.NoError(err)
	env.s.NotEmpty(resp.GetTaskToken())
	return resp
}

func (env *activityStreamEnv) complete(task *workflowservice.PollActivityTaskQueueResponse) {
	_, err := env.FrontendClient().RespondActivityTaskCompleted(
		env.s.Context(),
		&workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: task.GetTaskToken(),
			Identity:  "test",
		},
	)
	env.s.NoError(err)
}

func (env *activityStreamEnv) failBoom(task *workflowservice.PollActivityTaskQueueResponse) {
	_, err := env.FrontendClient().RespondActivityTaskFailed(
		env.s.Context(),
		&workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: task.GetTaskToken(),
			Failure: &failurepb.Failure{
				Message: "boom",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						Type:         "Boom",
						NonRetryable: true,
					},
				},
			},
			Identity: "test",
		},
	)
	env.s.NoError(err)
}

func (env *activityStreamEnv) notify(stream *streampb.StreamReference, counter int64) {
	_, err := env.FrontendClient().NotifyStream(
		env.s.Context(),
		&workflowservice.NotifyStreamRequest{
			Namespace: env.Namespace().String(),
			StreamRef: stream,
			Position:  fmt.Sprintf("cursor-%d", counter),
			Counter:   counter,
		},
	)
	env.s.NoError(err)
}

func (env *activityStreamEnv) describe(
	stream *streampb.StreamReference,
) *workflowservice.DescribeStreamNotifierResponse {
	resp, err := env.FrontendClient().DescribeStreamNotifier(
		env.s.Context(),
		&workflowservice.DescribeStreamNotifierRequest{
			Namespace: env.Namespace().String(),
			StreamRef: stream,
		},
	)
	env.s.NoError(err)
	return resp
}

// attachingHandler is a stream operation's handler. It attaches the caller's callback to stream.
func (env *activityStreamEnv) attachingHandler(
	stream *streampb.StreamReference,
	idle bool,
) nexustest.Handler {
	return nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			_, _ string,
			_ *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			_, err := env.FrontendClient().AttachStreamCallback(
				ctx,
				&workflowservice.AttachStreamCallbackRequest{
					Namespace: env.Namespace().String(),
					StreamRef: stream,
					RequestId: options.RequestID,
					Callback: &commonpb.Callback_Nexus{
						Url:    options.CallbackURL,
						Header: options.CallbackHeader,
					},
					Idle: idle,
				},
			)
			if err != nil {
				return nil, err
			}
			return &nexus.HandlerStartOperationResultAsync{
				OperationToken: stream.GetActivityId(),
			}, nil
		},
	}
}

// runCaller starts a Workflow that calls the stream operation and waits until the operation
// started and its callback is attached.
func (env *activityStreamEnv) runCaller(
	stream *streampb.StreamReference,
	endpointName string,
) (client.WorkflowRun, *commonpb.WorkflowExecution) {
	ctx := env.s.Context()
	taskQueue := testcore.RandomizeStr(env.s.T().Name())
	callerWF := func(ctx workflow.Context) error {
		c := workflow.NewNexusClient(endpointName, "service")
		return c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{}).
			Get(ctx, nil)
	}
	w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(callerWF)
	env.s.NoError(w.Start())
	env.s.T().Cleanup(w.Stop)
	run, err := env.SdkClient().
		ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, callerWF)
	env.s.NoError(err)
	wfExec := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}
	env.s.Await(func(s *NexusWorkflowTestSuite) {
		s.RequireHistoryEvent(
			env.GetHistory(env.Namespace().String(), wfExec),
			enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
		)
		s.Len(env.describe(stream).GetCallbacks(), 1)
	}, 10*time.Second, 50*time.Millisecond)
	return run, wfExec
}

func (s *NexusWorkflowTestSuite) TestActivityStreamIdleNexusCaller(chasmEnabled bool) {
	if !chasmEnabled {
		// A standalone Activity runs on CHASM only.
		return
	}
	env := s.newActivityStreamEnv()
	stream := env.start()
	endpointName := env.createRandomExternalNexusServer(
		s.Context(), s.T(), env.attachingHandler(stream, true))
	run, _ := env.runCaller(stream, endpointName)
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
	time.Sleep(time.Second) //nolint:forbidigo // checks that nothing is delivered
	s.Zero(delivered(), "the counter has not passed the wait")
	env.notify(stream, 6)
	s.Await(func(s *NexusWorkflowTestSuite) {
		s.Equal(int64(6), delivered())
	}, 10*time.Second, 50*time.Millisecond)

	// The Activity completes with no close, so the owner check completes the caller.
	env.complete(env.pollActivity())
	s.NoError(run.Get(s.Context(), nil), "a completed owner gives a null result")
	s.Nil(env.describe(stream).GetCloseFailure())
}

func (s *NexusWorkflowTestSuite) TestActivityStreamOwnerFailureFailsTheNexusCaller(
	chasmEnabled bool,
) {
	if !chasmEnabled {
		return
	}
	env := s.newActivityStreamEnv()
	stream := env.start()
	endpointName := env.createRandomExternalNexusServer(
		s.Context(), s.T(), env.attachingHandler(stream, false))
	run, wfExec := env.runCaller(stream, endpointName)

	env.failBoom(env.pollActivity())
	s.Error(run.Get(s.Context(), nil), "the owner's failure fails the caller's operation")
	failed := s.RequireHistoryEvent(
		env.GetHistory(env.Namespace().String(), wfExec),
		enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED,
	)
	cause := failed.GetNexusOperationFailedEventAttributes().GetFailure().GetCause()
	s.Equal("Boom", cause.GetApplicationFailureInfo().GetType())
	s.Equal("boom", cause.GetMessage())
}

// systemNexusCall runs one System Nexus operation from a Workflow driven by hand, since the SDK
// refuses the reserved endpoint name. The input may name the calling Workflow. It answers the
// operation's failure, or nil.
func (env *activityStreamEnv) systemNexusCall(
	operation string,
	input func(caller *commonpb.WorkflowExecution) proto.Message,
) *failurepb.Failure {
	ctx := env.s.Context()
	frontend := env.FrontendClient()
	queue := &taskqueuepb.TaskQueue{
		Name: testcore.RandomizeStr(env.s.T().Name() + "-" + operation),
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	workflowID := "system-nexus-" + uuid.NewString()
	started, err := frontend.StartWorkflowExecution(
		ctx,
		&workflowservice.StartWorkflowExecutionRequest{
			Namespace:    env.Namespace().String(),
			WorkflowId:   workflowID,
			WorkflowType: &commonpb.WorkflowType{Name: "reader"},
			TaskQueue:    queue,
			RequestId:    uuid.NewString(),
		},
	)
	env.s.NoError(err)
	poll := func() *workflowservice.PollWorkflowTaskQueueResponse {
		resp, err := frontend.PollWorkflowTaskQueue(
			ctx,
			&workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: env.Namespace().String(),
				TaskQueue: queue,
				Identity:  "reader",
			},
		)
		env.s.NoError(err)
		return resp
	}
	attrs := &commandpb.ScheduleNexusOperationCommandAttributes{
		Endpoint:  commonnexus.SystemEndpoint,
		Service:   workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.ServiceName,
		Operation: operation,
		Input: payloads.MustEncodeSingle(input(&commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      started.GetRunId(),
		})),
	}
	task := poll()
	_, err = frontend.RespondWorkflowTaskCompleted(
		ctx,
		&workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "reader",
			TaskToken: task.GetTaskToken(),
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: attrs,
				},
			}},
		},
	)
	env.s.NoError(err)
	task = poll()
	for _, event := range task.GetHistory().GetEvents() {
		if failed := event.GetNexusOperationFailedEventAttributes(); failed != nil {
			return failed.GetFailure()
		}
		if event.GetNexusOperationCompletedEventAttributes() != nil {
			return nil
		}
	}
	env.s.Fail("the System Nexus operation neither completed nor failed")
	return nil
}

func (s *NexusWorkflowTestSuite) TestActivityStreamSystemNexusAttachAndWait(chasmEnabled bool) {
	if !chasmEnabled {
		return
	}
	env := s.newActivityStreamEnv()
	stream := env.start()
	workflowService := workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService
	failure := env.systemNexusCall(
		workflowService.AttachStreamCallback.Name(),
		func(caller *commonpb.WorkflowExecution) proto.Message {
			return &workflowservice.AttachStreamCallbackRequest{
				StreamRef: stream,
				RequestId: "system-nexus-attach",
				WorkflowTarget: &streampb.WorkflowWakeTarget{
					WorkflowId:     caller.GetWorkflowId(),
					RunId:          caller.GetRunId(),
					SubscriptionId: "sub-1",
				},
			}
		},
	)
	s.Nil(failure, "%v", failure)
	callbacks := env.describe(stream).GetCallbacks()
	s.Len(callbacks, 1)
	s.Equal("sub-1", callbacks[0].GetWorkflowTarget().GetSubscriptionId())

	failure = env.systemNexusCall(
		workflowService.WaitStreamCallback.Name(),
		func(*commonpb.WorkflowExecution) proto.Message {
			return &workflowservice.WaitStreamCallbackRequest{
				StreamRef: stream,
				RequestId: "system-nexus-attach",
			}
		},
	)
	s.Nil(failure, "%v", failure)
}

func (s *NexusWorkflowTestSuite) TestActivityStreamSystemNexusNeedsStandaloneActivities(
	chasmEnabled bool,
) {
	if !chasmEnabled {
		return
	}
	env := s.newActivityStreamEnv(testcore.WithDynamicConfig(activity.Enabled, false))
	workflowService := workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService
	stream := activityStream("tool-"+uuid.NewString(), "run")
	for _, call := range []struct {
		operation string
		input     proto.Message
	}{
		{
			operation: workflowService.AttachStreamCallback.Name(),
			input: &workflowservice.AttachStreamCallbackRequest{
				StreamRef: stream,
				RequestId: "attach",
				WorkflowTarget: &streampb.WorkflowWakeTarget{
					WorkflowId:     "reader",
					RunId:          "run",
					SubscriptionId: "sub",
				},
			},
		},
		{
			operation: workflowService.WaitStreamCallback.Name(),
			input: &workflowservice.WaitStreamCallbackRequest{
				StreamRef: stream,
				RequestId: "attach",
			},
		},
	} {
		failure := env.systemNexusCall(
			call.operation,
			func(*commonpb.WorkflowExecution) proto.Message { return call.input },
		)
		s.NotNil(failure, call.operation)
		s.Contains(
			failure.GetCause().GetMessage()+failure.GetMessage(),
			"standalone Activities",
			"%s answers FailedPrecondition, never Unimplemented: %v",
			call.operation,
			failure,
		)
	}
}

func (s *NexusWorkflowTestSuite) TestActivityStreamProgressReachesStandaloneCallerWorker(
	chasmEnabled bool,
) {
	if !chasmEnabled {
		return
	}
	env := s.newActivityStreamEnv()
	ctx := s.Context()
	frontend := env.FrontendClient()
	stream := env.start()
	endpointName := env.createRandomExternalNexusServer(
		ctx,
		s.T(),
		env.attachingHandler(stream, false),
	)
	workerQueue := testcore.RandomizeStr("stream-follower-" + s.T().Name())
	_, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId: "follower-" + uuid.NewString(),
		Endpoint:    endpointName,
		CompletionCallbacks: []*commonpb.Callback{{
			Variant: &commonpb.Callback_NexusHandler_{
				NexusHandler: &commonpb.Callback_NexusHandler{
					TaskQueueName:     workerQueue,
					Service:           "NotificationService",
					Operation:         "OnComplete",
					ProgressOperation: "OnProgress",
				},
			},
		}},
	})
	s.NoError(err)

	poll := func() *workflowservice.PollNexusTaskQueueResponse {
		for {
			res, err := frontend.PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
				Namespace: env.Namespace().String(),
				Identity:  uuid.NewString(),
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: workerQueue,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
			})
			s.NoError(err)
			if res.GetTaskToken() != nil {
				return res
			}
		}
	}
	answer := func(task *workflowservice.PollNexusTaskQueueResponse, response any) {
		result, err := payload.Encode(response)
		s.NoError(err)
		_, err = frontend.RespondNexusTaskCompleted(
			ctx,
			&workflowservice.RespondNexusTaskCompletedRequest{
				Namespace: env.Namespace().String(),
				Identity:  uuid.NewString(),
				TaskToken: task.GetTaskToken(),
				Response: &nexuspb.Response{Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_SyncSuccess{
							SyncSuccess: &nexuspb.StartOperationResponse_Sync{Payload: result},
						},
					},
				}},
			},
		)
		s.NoError(err)
	}

	s.Await(func(s *NexusWorkflowTestSuite) {
		desc, err := frontend.DescribeStreamNotifier(
			ctx,
			&workflowservice.DescribeStreamNotifierRequest{
				Namespace: env.Namespace().String(),
				StreamRef: stream,
			},
		)
		s.NoError(err)
		s.Len(desc.GetCallbacks(), 1)
	}, 10*time.Second, 50*time.Millisecond)
	env.notify(stream, 1)
	for {
		task := poll()
		start := task.GetRequest().GetStartOperation()
		s.Equal("OnProgress", start.GetOperation())
		var req notificationservice.OnProgressRequest
		s.NoError(payload.Decode(start.GetPayload(), &req))
		answer(task, &notificationservice.OnProgressResponse{ReceiverState: []byte("read-to-1")})
		if req.GetProgress().GetCounter() >= 1 {
			s.Equal("cursor-1", req.GetProgress().GetPosition())
			s.Equal(stream.GetActivityId(), req.GetOperationToken())
			break
		}
	}

	// The Activity fails with no close. The owner check completes the follower with its failure.
	env.failBoom(env.pollActivity())
	task := poll()
	start := task.GetRequest().GetStartOperation()
	s.Equal("OnComplete", start.GetOperation())
	var onComplete notificationservice.OnCompleteRequest
	s.NoError(payload.Decode(start.GetPayload(), &onComplete))
	s.Equal([]byte("read-to-1"), onComplete.GetReceiverState())
	s.Nil(onComplete.GetSuccess())
	failure := onComplete.GetFailure()
	s.NotNil(failure)
	s.Contains(failure.String(), "Boom", "the Activity's failure reaches the Worker")
	answer(task, &notificationservice.OnCompleteResponse{})
}

func (s *NexusSyncSuite) TestActivityStreamReadCarriesTheCallerNamespace() {
	env := s.newEnv()
	target := s.registerNamespace(env, "sync-target")
	taskQueue := testcore.RandomizeStr("sync-tq")
	endpoint := s.createEndpoint(env, target, taskQueue)

	seen := make(chan *workflowservice.PollNexusTaskQueueResponse, 1)
	done := s.answerOneTask(env, target, taskQueue, syncSuccessAnswer(`{"records":[]}`), seen)

	req := syncNexusRequest(env.Namespace(), endpoint)
	input := `{"stream":{"kind":"activity","workflow_id":"","activity_id":"tool-1",` +
		`"chain_run_id":"run-1","topic":"tokens"}}`
	req.Input = &commonpb.Payload{Data: []byte(input)}
	_, err := env.FrontendClient().ExecuteSyncNexusOperation(s.Context(), req)
	s.NoError(err)
	s.NoError(s.Rcv(done))

	task := s.Rcv(seen)
	s.JSONEq(input, string(task.GetRequest().GetStartOperation().GetPayload().GetData()),
		"the frontend passes an Activity ref on as it is")
	s.Equal(
		env.Namespace().String(),
		task.GetRequest().GetHeader()[commonnexus.CallerNamespaceHeaderName],
	)
}
