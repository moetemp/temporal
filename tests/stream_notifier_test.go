package tests

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/notificationservice/v1"
	streampb "go.temporal.io/api/stream/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/callback"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/chasm/lib/streamnotifier"
	"go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestStreamNotifierEndToEnd runs a stream-returning Nexus operation through the stream notifier:
// the handler attaches the caller's callback, the producer's notifications reach the caller as
// progress, and closing the stream completes the operation with the close result. An HSM caller
// refuses progress and still gets the completion.
func (s *NexusWorkflowTestSuite) TestStreamNotifierEndToEnd(chasmEnabled bool) {
	env := s.newTestEnv(
		chasmEnabled,
		testcore.WithDynamicConfig(chasmnexus.EnableProgress, true),
		testcore.WithDynamicConfig(streamnotifier.Enabled, true),
		testcore.WithDynamicConfig(
			callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
		),
		testcore.WithDynamicConfig(callback.RetryPolicyInitialInterval, 10*time.Millisecond),
		testcore.WithDynamicConfig(callback.RetryPolicyMaximumInterval, 50*time.Millisecond),
	)
	if !chasmEnabled {
		// The caller's operation and callbacks stay on HSM while the notifier runs on CHASM.
		env.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	}
	ctx := s.Context()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	stream := &streampb.StreamReference{
		OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
		WorkflowId: "producer-" + uuid.NewString(),
		RunId:      "first-run",
		Topic:      "tokens",
	}
	frontend := env.FrontendClient()
	notify := func(counter int64, closeStream bool) {
		req := &workflowservice.NotifyStreamRequest{
			Namespace: env.Namespace().String(),
			StreamRef: stream,
			Position:  "cursor",
			Counter:   counter,
			Metadata:  map[string]string{"topic": "tokens"},
			Close:     closeStream,
		}
		if closeStream {
			req.CloseResult = testcore.MustToPayload(s.T(), "summary")
		}
		_, err := frontend.NotifyStream(ctx, req)
		s.NoError(err)
	}
	describe := func() *workflowservice.DescribeStreamNotifierResponse {
		resp, err := frontend.DescribeStreamNotifier(
			ctx,
			&workflowservice.DescribeStreamNotifierRequest{
				Namespace: env.Namespace().String(),
				StreamRef: stream,
			},
		)
		s.NoError(err)
		return resp
	}

	h := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			// The handler hands the caller's callback to the stream's notifier and answers async.
			_, err := frontend.AttachStreamCallback(
				ctx,
				&workflowservice.AttachStreamCallbackRequest{
					Namespace: env.Namespace().String(),
					StreamRef: stream,
					RequestId: options.RequestID,
					Callback: &commonpb.Callback_Nexus{
						Url:    options.CallbackURL,
						Header: options.CallbackHeader,
					},
				},
			)
			if err != nil {
				return nil, err
			}
			return &nexus.HandlerStartOperationResultAsync{
				OperationToken: stream.GetWorkflowId(),
			}, nil
		},
	}
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	callerWF := func(ctx workflow.Context) (string, error) {
		c := workflow.NewNexusClient(endpointName, "service")
		var result string
		err := c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{}).
			Get(ctx, &result)
		return result, err
	}
	w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(callerWF)
	s.NoError(w.Start())
	defer w.Stop()
	run, err := env.SdkClient().
		ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, callerWF)
	s.NoError(err)
	wfExec := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}

	var started []*historypb.HistoryEvent
	s.Await(func(s *NexusWorkflowTestSuite) {
		started = env.GetHistory(env.Namespace().String(), wfExec)
		s.RequireHistoryEvent(started, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED)
		s.Len(describe().GetCallbacks(), 1)
	}, 10*time.Second, 50*time.Millisecond)

	notify(1, false)
	notify(2, false)
	notify(3, false)

	if chasmEnabled {
		// The notifier's callback delivers the latest progress, and the caller folds it onto a
		// Workflow Task with no event of its own.
		s.Await(func(s *NexusWorkflowTestSuite) {
			s.Equal(int64(3), describe().GetCallbacks()[0].GetDeliveredCounter())
			s.NotEmpty(
				carriedNexusProgress(
					env.GetHistory(env.Namespace().String(), wfExec)[len(started):],
				),
			)
		}, 10*time.Second, 50*time.Millisecond)
		hist := env.GetHistory(env.Namespace().String(), wfExec)
		carried := carriedNexusProgress(hist[len(started):])
		s.Equal(s.scheduledEventID(started), carried[0].GetScheduledEventId())
		s.Equal("cursor", carried[0].GetPosition())
		s.Equal("tokens", carried[0].GetMetadata()["topic"])
		for _, event := range hist[len(started):] {
			s.Contains([]enumspb.EventType{
				enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
			}, event.GetEventType(), "progress must add no event of its own")
		}
	} else {
		// The HSM caller answers 400, so progress turns off for its callback.
		s.Await(func(s *NexusWorkflowTestSuite) {
			s.True(describe().GetCallbacks()[0].GetProgressDisabled())
		}, 10*time.Second, 50*time.Millisecond)
	}

	notify(4, true)
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("summary", result, "closing the stream completes the operation with its result")
	s.Await(func(s *NexusWorkflowTestSuite) {
		desc := describe()
		s.True(desc.GetClosed())
		s.Equal(enumspb.CALLBACK_STATE_SUCCEEDED, desc.GetCallbacks()[0].GetState())
	}, 10*time.Second, 50*time.Millisecond)
}

// TestStreamNotifierFailedCloseFailsTheCaller closes a stream with a failure and checks that the
// caller's stream-returning operation fails with it as the cause, as a Workflow-run operation fails
// with its run's failure.
func (s *NexusWorkflowTestSuite) TestStreamNotifierFailedCloseFailsTheCaller(chasmEnabled bool) {
	env := s.newTestEnv(
		chasmEnabled,
		testcore.WithDynamicConfig(chasmnexus.EnableProgress, true),
		testcore.WithDynamicConfig(streamnotifier.Enabled, true),
		testcore.WithDynamicConfig(
			callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
		),
		testcore.WithDynamicConfig(callback.RetryPolicyInitialInterval, 10*time.Millisecond),
		testcore.WithDynamicConfig(callback.RetryPolicyMaximumInterval, 50*time.Millisecond),
	)
	if !chasmEnabled {
		env.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	}
	ctx := s.Context()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	stream := &streampb.StreamReference{
		OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
		WorkflowId: "producer-" + uuid.NewString(),
		RunId:      "first-run",
		Topic:      "tokens",
	}
	frontend := env.FrontendClient()
	describe := func() *workflowservice.DescribeStreamNotifierResponse {
		resp, err := frontend.DescribeStreamNotifier(
			ctx,
			&workflowservice.DescribeStreamNotifierRequest{
				Namespace: env.Namespace().String(),
				StreamRef: stream,
			},
		)
		s.NoError(err)
		return resp
	}
	h := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			_, err := frontend.AttachStreamCallback(
				ctx,
				&workflowservice.AttachStreamCallbackRequest{
					Namespace: env.Namespace().String(),
					StreamRef: stream,
					RequestId: options.RequestID,
					Callback: &commonpb.Callback_Nexus{
						Url:    options.CallbackURL,
						Header: options.CallbackHeader,
					},
				},
			)
			if err != nil {
				return nil, err
			}
			return &nexus.HandlerStartOperationResultAsync{
				OperationToken: stream.GetWorkflowId(),
			}, nil
		},
	}
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)
	callerWF := func(ctx workflow.Context) (string, error) {
		c := workflow.NewNexusClient(endpointName, "service")
		var result string
		err := c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{}).
			Get(ctx, &result)
		return result, err
	}
	w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(callerWF)
	s.NoError(w.Start())
	defer w.Stop()
	run, err := env.SdkClient().
		ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, callerWF)
	s.NoError(err)
	wfExec := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}
	s.Await(func(s *NexusWorkflowTestSuite) {
		s.RequireHistoryEvent(
			env.GetHistory(env.Namespace().String(), wfExec),
			enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
		)
		s.Len(describe().GetCallbacks(), 1)
	}, 10*time.Second, 50*time.Millisecond)

	_, err = frontend.NotifyStream(ctx, &workflowservice.NotifyStreamRequest{
		Namespace: env.Namespace().String(),
		StreamRef: stream,
		Counter:   1,
		Close:     true,
		CloseFailure: &failurepb.Failure{
			Message: "the agent gave up",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					Type:         "AgentGaveUp",
					NonRetryable: true,
				},
			},
		},
	})
	s.NoError(err)

	s.Error(run.Get(ctx, nil), "a stream closed with a failure fails the caller's operation")
	hist := env.GetHistory(env.Namespace().String(), wfExec)
	failed := s.RequireHistoryEvent(hist, enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED)
	cause := failed.GetNexusOperationFailedEventAttributes().GetFailure().GetCause()
	s.Equal("AgentGaveUp", cause.GetApplicationFailureInfo().GetType())
	s.Equal("the agent gave up", cause.GetMessage())
	s.Equal(
		"AgentGaveUp",
		describe().GetCloseFailure().GetApplicationFailureInfo().GetType(),
		"a description says why the stream closed",
	)
}

// TestStreamNotifierOwnerCheckLeavesAJustCompletedOwnerToItsWorker completes the owner Workflow,
// lets several owner checks run, and only then closes the stream with a result, as the Worker's
// close after the final task does when it lands late. The caller must get that result, not the
// empty one an owner check would close with, since the notifier keeps the first close.
func (s *NexusWorkflowTestSuite) TestStreamNotifierOwnerCheckLeavesAJustCompletedOwnerToItsWorker(
	chasmEnabled bool,
) {
	env := s.newTestEnv(
		chasmEnabled,
		testcore.WithDynamicConfig(chasmnexus.EnableProgress, true),
		testcore.WithDynamicConfig(streamnotifier.Enabled, true),
		testcore.WithDynamicConfig(streamnotifier.OwnerCheckInterval, 100*time.Millisecond),
		testcore.WithDynamicConfig(
			callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
		),
		testcore.WithDynamicConfig(callback.RetryPolicyInitialInterval, 10*time.Millisecond),
		testcore.WithDynamicConfig(callback.RetryPolicyMaximumInterval, 50*time.Millisecond),
	)
	if !chasmEnabled {
		env.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	}
	ctx := s.Context()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	frontend := env.FrontendClient()

	ownerWF := func(ctx workflow.Context) error {
		workflow.GetSignalChannel(ctx, "end").Receive(ctx, nil)
		return nil
	}
	callerWF := func(ctx workflow.Context, endpoint string) (string, error) {
		c := workflow.NewNexusClient(endpoint, "service")
		var result string
		err := c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{}).
			Get(ctx, &result)
		return result, err
	}
	w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(ownerWF)
	w.RegisterWorkflow(callerWF)
	s.NoError(w.Start())
	defer w.Stop()

	owner, err := env.SdkClient().
		ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, ownerWF)
	s.NoError(err)
	stream := &streampb.StreamReference{
		OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
		WorkflowId: owner.GetID(),
		RunId:      owner.GetRunID(),
		Topic:      "tokens",
	}
	describe := func() *workflowservice.DescribeStreamNotifierResponse {
		resp, err := frontend.DescribeStreamNotifier(
			ctx,
			&workflowservice.DescribeStreamNotifierRequest{
				Namespace: env.Namespace().String(),
				StreamRef: stream,
			},
		)
		s.NoError(err)
		return resp
	}
	h := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			_, err := frontend.AttachStreamCallback(
				ctx,
				&workflowservice.AttachStreamCallbackRequest{
					Namespace: env.Namespace().String(),
					StreamRef: stream,
					RequestId: options.RequestID,
					Callback: &commonpb.Callback_Nexus{
						Url:    options.CallbackURL,
						Header: options.CallbackHeader,
					},
				},
			)
			if err != nil {
				return nil, err
			}
			return &nexus.HandlerStartOperationResultAsync{
				OperationToken: stream.GetWorkflowId(),
			}, nil
		},
	}
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)
	caller, err := env.SdkClient().ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{TaskQueue: taskQueue},
		callerWF,
		endpointName,
	)
	s.NoError(err)
	s.Await(func(s *NexusWorkflowTestSuite) {
		resp, err := frontend.DescribeStreamNotifier(
			ctx,
			&workflowservice.DescribeStreamNotifierRequest{
				Namespace: env.Namespace().String(),
				StreamRef: stream,
			},
		)
		s.NoError(err)
		s.Len(resp.GetCallbacks(), 1)
	}, 10*time.Second, 50*time.Millisecond)

	s.NoError(env.SdkClient().SignalWorkflow(ctx, owner.GetID(), owner.GetRunID(), "end", nil))
	s.NoError(owner.Get(ctx, nil))
	// Ten owner checks, all inside the grace that leaves the close to the Worker.
	time.Sleep(time.Second) //nolint:forbidigo // checks that nothing happens during the grace
	s.False(describe().GetClosed(), "an owner check closed a just-completed owner's stream")

	_, err = frontend.NotifyStream(ctx, &workflowservice.NotifyStreamRequest{
		Namespace:   env.Namespace().String(),
		StreamRef:   stream,
		Counter:     1,
		Close:       true,
		CloseResult: testcore.MustToPayload(s.T(), "the real result"),
	})
	s.NoError(err)
	var result string
	s.NoError(caller.Get(ctx, &result))
	s.Equal("the real result", result)
}

// TestStreamNotifierClosedFromWorkflow closes a stream from its owner Workflow through the
// __temporal_system endpoint, which is how a Workflow closes a stream it owns durably, and checks
// that the caller's stream-returning operation completes with the close result.
func (s *NexusWorkflowTestSuite) TestStreamNotifierClosedFromWorkflow(chasmEnabled bool) {
	if !chasmEnabled {
		// The owner Workflow closes through System Nexus, which only the CHASM caller path serves;
		// TestStreamNotifierEndToEnd covers an HSM caller.
		return
	}
	env := s.newTestEnv(
		chasmEnabled,
		testcore.WithDynamicConfig(chasmnexus.EnableProgress, true),
		testcore.WithDynamicConfig(streamnotifier.Enabled, true),
		testcore.WithDynamicConfig(
			callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
		),
		testcore.WithDynamicConfig(callback.RetryPolicyInitialInterval, 10*time.Millisecond),
		testcore.WithDynamicConfig(callback.RetryPolicyMaximumInterval, 50*time.Millisecond),
	)
	ctx := s.Context()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	stream := &streampb.StreamReference{
		OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
		WorkflowId: "owner-" + uuid.NewString(),
		Topic:      "tokens",
	}
	frontend := env.FrontendClient()
	// The owner starts first: its run chain's first run keys the stream's notifier.
	ownerTaskQueue := &taskqueuepb.TaskQueue{
		Name: testcore.RandomizeStr(s.T().Name() + "-owner"),
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	owner, err := frontend.StartWorkflowExecution(
		ctx,
		&workflowservice.StartWorkflowExecutionRequest{
			Namespace:    env.Namespace().String(),
			WorkflowId:   stream.GetWorkflowId(),
			WorkflowType: &commonpb.WorkflowType{Name: "owner-workflow"},
			TaskQueue:    ownerTaskQueue,
			RequestId:    uuid.NewString(),
		},
	)
	s.NoError(err)
	stream.RunId = owner.GetRunId()

	h := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			_, err := frontend.AttachStreamCallback(
				ctx,
				&workflowservice.AttachStreamCallbackRequest{
					Namespace: env.Namespace().String(),
					StreamRef: stream,
					RequestId: options.RequestID,
					Callback: &commonpb.Callback_Nexus{
						Url:    options.CallbackURL,
						Header: options.CallbackHeader,
					},
				},
			)
			if err != nil {
				return nil, err
			}
			return &nexus.HandlerStartOperationResultAsync{
				OperationToken: stream.GetWorkflowId(),
			}, nil
		},
	}
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	callerWF := func(ctx workflow.Context) (string, error) {
		c := workflow.NewNexusClient(endpointName, "service")
		var result string
		err := c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{}).
			Get(ctx, &result)
		return result, err
	}
	w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(callerWF)
	s.NoError(w.Start())
	defer w.Stop()

	run, err := env.SdkClient().
		ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, callerWF)
	s.NoError(err)
	wfExec := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}
	s.Await(func(s *NexusWorkflowTestSuite) {
		s.RequireHistoryEvent(
			env.GetHistory(env.Namespace().String(), wfExec),
			enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
		)
	}, 10*time.Second, 50*time.Millisecond)

	// The owner Workflow is driven by hand, since the SDK refuses the reserved endpoint name. It
	// sends no namespace and no identity: the operation takes the namespace from the Workflow.
	poll := func() *workflowservice.PollWorkflowTaskQueueResponse {
		resp, err := frontend.PollWorkflowTaskQueue(
			ctx,
			&workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: env.Namespace().String(),
				TaskQueue: ownerTaskQueue,
				Identity:  "owner",
			},
		)
		s.NoError(err)
		return resp
	}
	task := poll()
	workflowService := workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService
	notifyAttrs := &commandpb.ScheduleNexusOperationCommandAttributes{
		Endpoint:  commonnexus.SystemEndpoint,
		Service:   workflowService.ServiceName,
		Operation: workflowService.NotifyStream.Name(),
		Input: payloads.MustEncodeSingle(&workflowservice.NotifyStreamRequest{
			StreamRef:   stream,
			Position:    "end",
			Counter:     1,
			Close:       true,
			CloseResult: testcore.MustToPayload(s.T(), "summary"),
		}),
	}
	_, err = frontend.RespondWorkflowTaskCompleted(
		ctx,
		&workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "owner",
			TaskToken: task.TaskToken,
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: notifyAttrs,
				},
			}},
		},
	)
	s.NoError(err)
	task = poll()
	var completed bool
	for _, event := range task.GetHistory().GetEvents() {
		if failed := event.GetNexusOperationFailedEventAttributes(); failed != nil {
			s.Failf("the owner's NotifyStream operation failed", "%v", failed.GetFailure())
		}
		completed = completed || event.GetNexusOperationCompletedEventAttributes() != nil
	}
	s.True(completed, "the owner's NotifyStream operation must complete")

	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("summary", result, "closing the stream from its owner completes the caller's operation")
}

// TestStreamNotifierOwnerStartGrace attaches a caller to a stream whose owner Workflow does not
// exist. A reader may subscribe before its owner starts, so the owner check leaves the stream open
// for streamnotifier.ownerStartGrace from the notifier's start. An owner that never starts then
// closes the stream with a failure.
func (s *NexusWorkflowTestSuite) TestStreamNotifierOwnerStartGrace(chasmEnabled bool) {
	env := s.newTestEnv(
		chasmEnabled,
		testcore.WithDynamicConfig(streamnotifier.Enabled, true),
		testcore.WithDynamicConfig(streamnotifier.OwnerCheckInterval, 100*time.Millisecond),
		testcore.WithDynamicConfig(streamnotifier.OwnerStartGrace, 2*time.Second),
		testcore.WithDynamicConfig(
			callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
		),
		testcore.WithDynamicConfig(callback.RetryPolicyInitialInterval, 10*time.Millisecond),
		testcore.WithDynamicConfig(callback.RetryPolicyMaximumInterval, 50*time.Millisecond),
	)
	if !chasmEnabled {
		env.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	}
	ctx := s.Context()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	frontend := env.FrontendClient()
	stream := &streampb.StreamReference{
		OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
		WorkflowId: "never-started-" + uuid.NewString(),
		RunId:      uuid.NewString(),
		Topic:      "tokens",
	}
	describe := func(s *NexusWorkflowTestSuite) *workflowservice.DescribeStreamNotifierResponse {
		resp, err := frontend.DescribeStreamNotifier(
			ctx,
			&workflowservice.DescribeStreamNotifierRequest{
				Namespace: env.Namespace().String(),
				StreamRef: stream,
			},
		)
		s.NoError(err)
		return resp
	}
	h := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			_, err := frontend.AttachStreamCallback(
				ctx,
				&workflowservice.AttachStreamCallbackRequest{
					Namespace: env.Namespace().String(),
					StreamRef: stream,
					RequestId: options.RequestID,
					Callback: &commonpb.Callback_Nexus{
						Url:    options.CallbackURL,
						Header: options.CallbackHeader,
					},
				},
			)
			if err != nil {
				return nil, err
			}
			return &nexus.HandlerStartOperationResultAsync{
				OperationToken: stream.GetWorkflowId(),
			}, nil
		},
	}
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)
	callerWF := func(ctx workflow.Context) error {
		c := workflow.NewNexusClient(endpointName, "service")
		return c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{}).
			Get(ctx, nil)
	}
	w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(callerWF)
	s.NoError(w.Start())
	defer w.Stop()
	attached := time.Now()
	run, err := env.SdkClient().
		ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, callerWF)
	s.NoError(err)
	s.Await(func(s *NexusWorkflowTestSuite) {
		s.Len(describe(s).GetCallbacks(), 1)
	}, 10*time.Second, 50*time.Millisecond)

	// Several owner checks run inside the grace.
	time.Sleep(time.Second) //nolint:forbidigo // checks that nothing happens during the grace
	if time.Since(attached) < 2*time.Second {
		s.False(describe(s).GetClosed(), "an owner check closed the stream inside the grace")
	}

	s.Await(func(s *NexusWorkflowTestSuite) {
		s.True(describe(s).GetClosed())
	}, 10*time.Second, 50*time.Millisecond)
	s.Equal(
		"the owner Workflow ended without closing the stream",
		describe(s).GetCloseFailure().GetMessage(),
	)
	s.Error(run.Get(ctx, nil), "an owner that never started fails the caller")
}

// newStreamNotifierEnv is a CHASM caller environment with the stream notifier on.
func (s *NexusWorkflowTestSuite) newStreamNotifierEnv() *NexusTestEnv {
	return s.newTestEnv(
		true,
		testcore.WithDynamicConfig(chasmnexus.EnableProgress, true),
		testcore.WithDynamicConfig(streamnotifier.Enabled, true),
		testcore.WithDynamicConfig(
			callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
		),
		testcore.WithDynamicConfig(callback.RetryPolicyInitialInterval, 10*time.Millisecond),
		testcore.WithDynamicConfig(callback.RetryPolicyMaximumInterval, 50*time.Millisecond),
	)
}

// TestStreamNotifierCompletionBeforeTheStartCarriesTheToken attaches a caller to a stream that is
// already closed, so the notifier completes the operation before the handler answers the start. The
// completion carries the handler's operation token, so the caller still learns which stream it is.
func (s *NexusWorkflowTestSuite) TestStreamNotifierCompletionBeforeTheStartCarriesTheToken(
	chasmEnabled bool,
) {
	if !chasmEnabled {
		// Only a CHASM caller takes a completion before its start; TestStreamNotifierEndToEnd
		// covers HSM.
		return
	}
	env := s.newStreamNotifierEnv()
	ctx := s.Context()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	frontend := env.FrontendClient()
	stream := &streampb.StreamReference{
		OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
		WorkflowId: "producer-" + uuid.NewString(),
		RunId:      "first-run",
		Topic:      "tokens",
	}
	_, err := frontend.NotifyStream(ctx, &workflowservice.NotifyStreamRequest{
		Namespace:   env.Namespace().String(),
		StreamRef:   stream,
		Counter:     1,
		Close:       true,
		CloseResult: testcore.MustToPayload(s.T(), "summary"),
	})
	s.NoError(err)

	const token = "stream-token"
	h := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			_, err := frontend.AttachStreamCallback(
				ctx,
				&workflowservice.AttachStreamCallbackRequest{
					Namespace: env.Namespace().String(),
					StreamRef: stream,
					RequestId: options.RequestID,
					Callback: &commonpb.Callback_Nexus{
						Url:    options.CallbackURL,
						Header: options.CallbackHeader,
					},
					OperationToken: token,
					StartTime:      timestamppb.Now(),
				},
			)
			if err != nil {
				return nil, err
			}
			// Answer only once the notifier has delivered the completion, so it beats the start.
			// This runs on the handler's goroutine, so it polls rather than asserting.
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()
			for {
				desc, err := frontend.DescribeStreamNotifier(
					ctx,
					&workflowservice.DescribeStreamNotifierRequest{
						Namespace: env.Namespace().String(),
						StreamRef: stream,
					},
				)
				if err == nil && len(desc.GetCallbacks()) == 1 &&
					desc.GetCallbacks()[0].GetState() == enumspb.CALLBACK_STATE_SUCCEEDED {
					return &nexus.HandlerStartOperationResultAsync{OperationToken: token}, nil
				}
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-ticker.C:
				}
			}
		},
	}
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	callerWF := func(ctx workflow.Context) (string, error) {
		c := workflow.NewNexusClient(endpointName, "service")
		var result string
		err := c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{}).
			Get(ctx, &result)
		return result, err
	}
	w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(callerWF)
	s.NoError(w.Start())
	defer w.Stop()
	run, err := env.SdkClient().
		ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, callerWF)
	s.NoError(err)
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("summary", result)

	hist := env.GetHistory(
		env.Namespace().String(),
		&commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()},
	)
	started := s.RequireHistoryEvent(hist, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED)
	s.Equal(token, started.GetNexusOperationStartedEventAttributes().GetOperationToken(),
		"the started event the completion wrote names the stream")
}

// TestStreamNotifierCancelCompletesTheCaller cancels a caller's stream-returning operation. The
// handler's cancel detaches the caller, and the notifier completes the caller's operation as
// canceled, which a caller waiting for the cancellation needs.
func (s *NexusWorkflowTestSuite) TestStreamNotifierCancelCompletesTheCaller(chasmEnabled bool) {
	if !chasmEnabled {
		// Only a CHASM caller attaches through these tests; TestStreamNotifierEndToEnd covers HSM.
		return
	}
	env := s.newStreamNotifierEnv()
	ctx := s.Context()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	frontend := env.FrontendClient()
	stream := &streampb.StreamReference{
		OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
		WorkflowId: "producer-" + uuid.NewString(),
		RunId:      "first-run",
		Topic:      "tokens",
	}
	var attachRequestID string
	h := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			attachRequestID = options.RequestID
			_, err := frontend.AttachStreamCallback(
				ctx,
				&workflowservice.AttachStreamCallbackRequest{
					Namespace: env.Namespace().String(),
					StreamRef: stream,
					RequestId: options.RequestID,
					Callback: &commonpb.Callback_Nexus{
						Url:    options.CallbackURL,
						Header: options.CallbackHeader,
					},
					OperationToken: "stream-token",
				},
			)
			if err != nil {
				return nil, err
			}
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "stream-token"}, nil
		},
		OnCancelOperation: func(
			ctx context.Context,
			service,
			operation,
			token string,
			options nexus.CancelOperationOptions,
		) error {
			_, err := frontend.DetachStreamCallback(
				ctx,
				&workflowservice.DetachStreamCallbackRequest{
					Namespace: env.Namespace().String(),
					StreamRef: stream,
					RequestId: attachRequestID,
				},
			)
			return err
		},
	}
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	callerWF := func(ctx workflow.Context) (string, error) {
		opCtx, cancel := workflow.WithCancel(ctx)
		c := workflow.NewNexusClient(endpointName, "service")
		fut := c.ExecuteOperation(opCtx, "operation", "input", workflow.NexusOperationOptions{})
		if err := fut.GetNexusOperationExecution().Get(ctx, nil); err != nil {
			return "", err
		}
		cancel()
		err := fut.Get(ctx, nil)
		var canceled *temporal.CanceledError
		if errors.As(err, &canceled) {
			return "canceled", nil
		}
		return "", err
	}
	w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(callerWF)
	s.NoError(w.Start())
	defer w.Stop()
	run, err := env.SdkClient().
		ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, callerWF)
	s.NoError(err)
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("canceled", result, "the caller's operation ends canceled")
}

// TestStreamNotifierProgressReachesStandaloneCallerWorker follows a stream with a standalone
// operation whose only callback is a Worker's. The notifier's progress reaches the Worker as
// OnProgress, and the close reaches it as OnComplete with the state the Worker returned.
func (s *NexusWorkflowTestSuite) TestStreamNotifierProgressReachesStandaloneCallerWorker(
	chasmEnabled bool,
) {
	if !chasmEnabled {
		// A standalone operation runs on CHASM only.
		return
	}
	env := s.newTestEnv(
		true,
		testcore.WithDynamicConfig(chasmnexus.EnableProgress, true),
		testcore.WithDynamicConfig(streamnotifier.Enabled, true),
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
	ctx := s.Context()
	frontend := env.FrontendClient()
	stream := &streampb.StreamReference{
		OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
		WorkflowId: "producer-" + uuid.NewString(),
		RunId:      "first-run",
		Topic:      "tokens",
	}
	notify := func(counter int64, closeStream bool) {
		req := &workflowservice.NotifyStreamRequest{
			Namespace: env.Namespace().String(),
			StreamRef: stream,
			Position:  fmt.Sprintf("cursor-%d", counter),
			Counter:   counter,
			Close:     closeStream,
		}
		if closeStream {
			req.CloseResult = testcore.MustToPayload(s.T(), "summary")
		}
		_, err := frontend.NotifyStream(ctx, req)
		s.NoError(err)
	}
	h := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			_, _ string,
			_ *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			_, err := frontend.AttachStreamCallback(
				ctx,
				&workflowservice.AttachStreamCallbackRequest{
					Namespace: env.Namespace().String(),
					StreamRef: stream,
					RequestId: options.RequestID,
					Callback: &commonpb.Callback_Nexus{
						Url:    options.CallbackURL,
						Header: options.CallbackHeader,
					},
				},
			)
			if err != nil {
				return nil, err
			}
			return &nexus.HandlerStartOperationResultAsync{
				OperationToken: stream.GetWorkflowId(),
			}, nil
		},
	}
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)
	workerQueue := testcore.RandomizeStr("stream-follower-" + s.T().Name())

	operationID := "follower-" + uuid.NewString()
	_, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId: operationID,
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
	// awaitProgress answers OnProgress tasks with state until one carries the counter.
	awaitProgress := func(counter int64, state string) *notificationservice.OnProgressRequest {
		for {
			task := poll()
			start := task.GetRequest().GetStartOperation()
			s.Equal("OnProgress", start.GetOperation())
			var req notificationservice.OnProgressRequest
			s.NoError(payload.Decode(start.GetPayload(), &req))
			answer(task, &notificationservice.OnProgressResponse{ReceiverState: []byte(state)})
			if req.GetProgress().GetCounter() >= counter {
				return &req
			}
		}
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

	notify(1, false)
	first := awaitProgress(1, "read-to-1")
	s.Equal(int64(1), first.GetProgress().GetCounter())
	s.Equal("cursor-1", first.GetProgress().GetPosition())
	s.Equal(stream.GetWorkflowId(), first.GetOperationToken(), "the token names the stream")

	notify(2, false)
	// The second delivery carries the first one's state. Its answer keeps that state, so the
	// completion carries it whichever result the callback saves last.
	second := awaitProgress(2, "read-to-1")
	s.Equal("cursor-2", second.GetProgress().GetPosition())
	s.Equal([]byte("read-to-1"), second.GetReceiverState())

	notify(3, true)
	task := poll()
	start := task.GetRequest().GetStartOperation()
	s.Equal("OnComplete", start.GetOperation())
	var onComplete notificationservice.OnCompleteRequest
	s.NoError(payload.Decode(start.GetPayload(), &onComplete))
	s.Equal([]byte("read-to-1"), onComplete.GetReceiverState())
	var result string
	s.NoError(payload.Decode(onComplete.GetSuccess(), &result))
	s.Equal("summary", result, "the close result reaches the Worker")
	answer(task, &notificationservice.OnCompleteResponse{})
}
