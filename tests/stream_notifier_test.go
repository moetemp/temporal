package tests

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/callback"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/chasm/lib/streamnotifier"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// newStreamNotifierEnv is a CHASM caller environment with the stream notifier on.
func (s *NexusWorkflowTestSuite) newStreamNotifierEnv() *NexusTestEnv {
	return s.newTestEnv(true,
		testcore.WithDynamicConfig(chasmnexus.EnableProgress, true),
		testcore.WithDynamicConfig(streamnotifier.Enabled, true),
		testcore.WithDynamicConfig(callback.AllowedAddresses, []any{map[string]any{"Pattern": "*", "AllowInsecure": true}}),
		testcore.WithDynamicConfig(callback.RetryPolicyInitialInterval, 10*time.Millisecond),
		testcore.WithDynamicConfig(callback.RetryPolicyMaximumInterval, 50*time.Millisecond),
	)
}

// TestStreamNotifierCompletionBeforeTheStartCarriesTheToken attaches a caller to a stream that is
// already closed, so the notifier completes the operation before the handler answers the start. The
// completion carries the handler's operation token, so the caller still learns which stream it is.
func (s *NexusWorkflowTestSuite) TestStreamNotifierCompletionBeforeTheStartCarriesTheToken(chasmEnabled bool) {
	if !chasmEnabled {
		// Only a CHASM caller takes a completion before its start; TestStreamNotifierEndToEnd covers HSM.
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
			_, err := frontend.AttachStreamCallback(ctx, &workflowservice.AttachStreamCallbackRequest{
				Namespace:      env.Namespace().String(),
				StreamRef:      stream,
				RequestId:      options.RequestID,
				Callback:       &commonpb.Callback_Nexus{Url: options.CallbackURL, Header: options.CallbackHeader},
				OperationToken: token,
				StartTime:      timestamppb.Now(),
			})
			if err != nil {
				return nil, err
			}
			// Answer only once the notifier has delivered the completion, so it beats the start. This
			// runs on the handler's goroutine, so it polls rather than asserting.
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()
			for {
				desc, err := frontend.DescribeStreamNotifier(ctx, &workflowservice.DescribeStreamNotifierRequest{
					Namespace: env.Namespace().String(),
					StreamRef: stream,
				})
				if err == nil && len(desc.GetCallbacks()) == 1 && desc.GetCallbacks()[0].GetState() == enumspb.CALLBACK_STATE_SUCCEEDED {
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
		err := c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{}).Get(ctx, &result)
		return result, err
	}
	w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(callerWF)
	s.NoError(w.Start())
	defer w.Stop()
	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, callerWF)
	s.NoError(err)
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("summary", result)

	hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()})
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
			_, err := frontend.AttachStreamCallback(ctx, &workflowservice.AttachStreamCallbackRequest{
				Namespace:      env.Namespace().String(),
				StreamRef:      stream,
				RequestId:      options.RequestID,
				Callback:       &commonpb.Callback_Nexus{Url: options.CallbackURL, Header: options.CallbackHeader},
				OperationToken: "stream-token",
			})
			if err != nil {
				return nil, err
			}
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "stream-token"}, nil
		},
		OnCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
			_, err := frontend.DetachStreamCallback(ctx, &workflowservice.DetachStreamCallbackRequest{
				Namespace: env.Namespace().String(),
				StreamRef: stream,
				RequestId: attachRequestID,
			})
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
	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, callerWF)
	s.NoError(err)
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("canceled", result, "the caller's operation ends canceled")
}
