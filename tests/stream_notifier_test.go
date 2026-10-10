package tests

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	streampb "go.temporal.io/api/stream/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/callback"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/chasm/lib/streamnotifier"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
)

// TestStreamNotifierEndToEnd runs a stream-returning Nexus operation through the stream notifier:
// the handler attaches the caller's callback, the producer's notifications reach the caller as
// progress, and closing the stream completes the operation with the close result. An HSM caller
// refuses progress and still gets the completion.
func (s *NexusWorkflowTestSuite) TestStreamNotifierEndToEnd(chasmEnabled bool) {
	env := s.newTestEnv(chasmEnabled,
		testcore.WithDynamicConfig(chasmnexus.EnableProgress, true),
		testcore.WithDynamicConfig(streamnotifier.Enabled, true),
		testcore.WithDynamicConfig(callback.AllowedAddresses, []any{map[string]any{"Pattern": "*", "AllowInsecure": true}}),
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
		resp, err := frontend.DescribeStreamNotifier(ctx, &workflowservice.DescribeStreamNotifierRequest{
			Namespace: env.Namespace().String(),
			StreamRef: stream,
		})
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
			_, err := frontend.AttachStreamCallback(ctx, &workflowservice.AttachStreamCallbackRequest{
				Namespace: env.Namespace().String(),
				StreamRef: stream,
				RequestId: options.RequestID,
				Callback: &commonpb.Callback_Nexus{
					Url:    options.CallbackURL,
					Header: options.CallbackHeader,
				},
			})
			if err != nil {
				return nil, err
			}
			return &nexus.HandlerStartOperationResultAsync{OperationToken: stream.GetWorkflowId()}, nil
		},
	}
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	callerWF := func(ctx workflow.Context) (string, error) {
		c := workflow.NewNexusClient(endpointName, "service")
		var result string
		err := c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{}).Get(ctx, &result)
		return result, err
	}
	w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(callerWF)
	s.NoError(w.Start())
	defer w.Stop()
	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, callerWF)
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
			s.NotEmpty(carriedNexusProgress(env.GetHistory(env.Namespace().String(), wfExec)[len(started):]))
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

// TestStreamNotifierClosedFromWorkflow closes a stream from its owner Workflow through the
// __temporal_system endpoint, which is how a Workflow closes a stream it owns durably, and checks
// that the caller's stream-returning operation completes with the close result.
func (s *NexusWorkflowTestSuite) TestStreamNotifierClosedFromWorkflow(chasmEnabled bool) {
	if !chasmEnabled {
		s.T().Skip("the stream notifier runs on CHASM")
	}
	env := s.newTestEnv(chasmEnabled,
		testcore.WithDynamicConfig(chasmnexus.EnableProgress, true),
		testcore.WithDynamicConfig(streamnotifier.Enabled, true),
		testcore.WithDynamicConfig(callback.AllowedAddresses, []any{map[string]any{"Pattern": "*", "AllowInsecure": true}}),
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

	h := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			_, err := frontend.AttachStreamCallback(ctx, &workflowservice.AttachStreamCallbackRequest{
				Namespace: env.Namespace().String(),
				StreamRef: stream,
				RequestId: options.RequestID,
				Callback: &commonpb.Callback_Nexus{
					Url:    options.CallbackURL,
					Header: options.CallbackHeader,
				},
			})
			if err != nil {
				return nil, err
			}
			return &nexus.HandlerStartOperationResultAsync{OperationToken: stream.GetWorkflowId()}, nil
		},
	}
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	callerWF := func(ctx workflow.Context) (string, error) {
		c := workflow.NewNexusClient(endpointName, "service")
		var result string
		err := c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{}).Get(ctx, &result)
		return result, err
	}
	w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(callerWF)
	s.NoError(w.Start())
	defer w.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, callerWF)
	s.NoError(err)
	wfExec := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}
	s.Await(func(s *NexusWorkflowTestSuite) {
		s.RequireHistoryEvent(env.GetHistory(env.Namespace().String(), wfExec), enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED)
	}, 10*time.Second, 50*time.Millisecond)

	// The owner Workflow is driven by hand, since the SDK refuses the reserved endpoint name. It
	// sends no namespace and no identity: the operation takes the namespace from the Workflow.
	ownerTaskQueue := &taskqueuepb.TaskQueue{Name: testcore.RandomizeStr(s.T().Name() + "-owner"), Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	_, err = frontend.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    env.Namespace().String(),
		WorkflowId:   stream.GetWorkflowId(),
		WorkflowType: &commonpb.WorkflowType{Name: "owner-workflow"},
		TaskQueue:    ownerTaskQueue,
		RequestId:    uuid.NewString(),
	})
	s.NoError(err)
	poll := func() *workflowservice.PollWorkflowTaskQueueResponse {
		resp, err := frontend.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: ownerTaskQueue,
			Identity:  "owner",
		})
		s.NoError(err)
		return resp
	}
	task := poll()
	_, err = frontend.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "owner",
		TaskToken: task.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  commonnexus.SystemEndpoint,
					Service:   workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.ServiceName,
					Operation: workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.NotifyStream.Name(),
					Input: payloads.MustEncodeSingle(&workflowservice.NotifyStreamRequest{
						StreamRef:   stream,
						Position:    "end",
						Counter:     1,
						Close:       true,
						CloseResult: testcore.MustToPayload(s.T(), "summary"),
					}),
				},
			},
		}},
	})
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
