package tests

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/notificationservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/callbacks"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

// newProgressTestEnv is a standalone operation env that takes progress and NexusHandler callbacks.
func (s *NexusStandaloneCallbacksTestSuite) newProgressTestEnv() *NexusTestEnv {
	env := s.newTestEnv(true)
	env.OverrideDynamicConfig(
		nexusoperation.EnabledCallbackKinds,
		[]callbacks.Kind{callbacks.KindNexus, callbacks.KindNexusHandler},
	)
	env.OverrideDynamicConfig(nexusoperation.EnableProgress, true)
	return env
}

// pollWorkerCallback takes the next Nexus task from a NexusHandler callback's task queue, as a
// Worker would.
func (s *NexusStandaloneCallbacksTestSuite) pollWorkerCallback(
	env *NexusTestEnv,
	taskQueue string,
) *workflowservice.PollNexusTaskQueueResponse {
	s.T().Helper()
	for {
		res, err := env.FrontendClient().PollNexusTaskQueue(
			s.Context(),
			&workflowservice.PollNexusTaskQueueRequest{
				Namespace: env.Namespace().String(),
				Identity:  uuid.NewString(),
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueue,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
			},
		)
		s.NoError(err)
		if res.GetTaskToken() != nil {
			return res
		}
	}
}

// answerWorkerCallback completes a NexusHandler callback's task synchronously with the response.
func (s *NexusStandaloneCallbacksTestSuite) answerWorkerCallback(
	env *NexusTestEnv,
	task *workflowservice.PollNexusTaskQueueResponse,
	response any,
) {
	s.T().Helper()
	result, err := payload.Encode(response)
	s.NoError(err)
	_, err = env.FrontendClient().RespondNexusTaskCompleted(
		s.Context(),
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

// progressEndpoint registers a handler that leaves the operation running and hands out the
// callback it was given. With early set, it posts progress before its start response.
func (s *NexusStandaloneCallbacksTestSuite) progressEndpoint(
	env *NexusTestEnv,
	early string,
) (string, <-chan [2]string, <-chan int) {
	callbackCh := make(chan [2]string, 1)
	earlyStatus := make(chan int, 1)
	endpoint := env.createRandomExternalNexusServer(s.Context(), s.T(), nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			_, _ string,
			_ *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			token := options.CallbackHeader.Get(commonnexus.CallbackTokenHeader)
			if early != "" {
				status, err := postNexusProgress(ctx, options.CallbackURL, token, early)
				if err != nil {
					return nil, err
				}
				earlyStatus <- status
			}
			callbackCh <- [2]string{options.CallbackURL, token}
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "handler-token"}, nil
		},
	})
	return endpoint, callbackCh, earlyStatus
}

func (s *NexusStandaloneCallbacksTestSuite) TestStandaloneOperationRelaysProgressToWorker() {
	env := s.newProgressTestEnv()
	ctx := s.Context()
	endpoint, callbackCh, earlyStatus := s.progressEndpoint(
		env, `{"position": "cursor-1", "counter": "1"}`,
	)
	workerQueue := testcore.RandomizeStr("progress-follower-" + s.T().Name())

	operationID := testvars.New(s.T()).Any().String()
	_, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId: operationID,
		Endpoint:    endpoint,
		CompletionCallbacks: []*commonpb.Callback{{
			Variant: &commonpb.Callback_NexusHandler_{
				NexusHandler: &commonpb.Callback_NexusHandler{
					TaskQueueName:     workerQueue,
					Service:           "NotificationService",
					Operation:         "OnComplete",
					ProgressOperation: "OnProgress",
					SourceContext:     &commonpb.Payload{Data: []byte("follower")},
				},
			},
		}},
	})
	s.NoError(err)
	callback := s.Rcv(callbackCh)
	s.Equal(http.StatusOK, s.Rcv(earlyStatus), "progress before the start is kept, not refused")

	// The progress that came before the start reaches the Worker after it.
	task := s.pollWorkerCallback(env, workerQueue)
	start := task.GetRequest().GetStartOperation()
	s.Equal("NotificationService", start.GetService())
	s.Equal("OnProgress", start.GetOperation())
	var onProgress notificationservice.OnProgressRequest
	s.NoError(payload.Decode(start.GetPayload(), &onProgress))
	s.Equal(int64(1), onProgress.GetProgress().GetCounter())
	s.Equal("cursor-1", onProgress.GetProgress().GetPosition())
	s.Equal("handler-token", onProgress.GetOperationToken())
	s.Equal([]byte("follower"), onProgress.GetSourceContext().GetData())
	s.Empty(onProgress.GetReceiverState())
	s.answerWorkerCallback(env, task, &notificationservice.OnProgressResponse{
		ReceiverState: []byte("read-to-1"),
	})

	status, err := postNexusProgress(ctx, callback[0], callback[1], `{"counter": "2"}`)
	s.NoError(err)
	s.Equal(http.StatusOK, status)
	task = s.pollWorkerCallback(env, workerQueue)
	s.NoError(payload.Decode(task.GetRequest().GetStartOperation().GetPayload(), &onProgress))
	s.Equal(int64(2), onProgress.GetProgress().GetCounter())
	s.Equal([]byte("read-to-1"), onProgress.GetReceiverState())
	s.answerWorkerCallback(env, task, &notificationservice.OnProgressResponse{
		ReceiverState: []byte("read-to-2"),
	})

	// The next delivery carries the saved state, so the completion sent after it carries it too.
	status, err = postNexusProgress(ctx, callback[0], callback[1], `{"counter": "3"}`)
	s.NoError(err)
	s.Equal(http.StatusOK, status)
	task = s.pollWorkerCallback(env, workerQueue)
	s.NoError(payload.Decode(task.GetRequest().GetStartOperation().GetPayload(), &onProgress))
	s.Equal(int64(3), onProgress.GetProgress().GetCounter())
	s.Equal([]byte("read-to-2"), onProgress.GetReceiverState())
	s.answerWorkerCallback(env, task, &notificationservice.OnProgressResponse{
		ReceiverState: []byte("read-to-2"),
	})

	s.NoError(nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{
		Serializer: commonnexus.PayloadSerializer,
	}).CompleteOperation(ctx, callback[0], nexusrpc.CompleteOperationOptions{
		Result: testcore.MustToPayload(s.T(), "done"),
		Header: nexus.Header{commonnexus.CallbackTokenHeader: callback[1]},
	}))

	task = s.pollWorkerCallback(env, workerQueue)
	start = task.GetRequest().GetStartOperation()
	s.Equal("OnComplete", start.GetOperation())
	var onComplete notificationservice.OnCompleteRequest
	s.NoError(payload.Decode(start.GetPayload(), &onComplete))
	s.Equal([]byte("read-to-2"), onComplete.GetReceiverState())
	var result string
	s.NoError(payload.Decode(onComplete.GetSuccess(), &result))
	s.Equal("done", result)
	s.answerWorkerCallback(env, task, &notificationservice.OnCompleteResponse{})
	s.awaitCallbackInfo(env, operationID, enumspb.CALLBACK_STATE_SUCCEEDED)
}

func (s *NexusStandaloneCallbacksTestSuite) TestStandaloneOperationWithoutProgressCallbackRefusesProgress() {
	env := s.newProgressTestEnv()
	ctx := s.Context()

	for name, cbs := range map[string][]*commonpb.Callback{
		"NoCallback": nil,
		"ACompletionOnlyCallback": {{
			Variant: &commonpb.Callback_NexusHandler_{
				NexusHandler: &commonpb.Callback_NexusHandler{
					TaskQueueName: testcore.RandomizeStr("completion-only"),
					Service:       "NotificationService",
					Operation:     "OnComplete",
				},
			},
		}},
	} {
		s.Run(name, func(s *NexusStandaloneCallbacksTestSuite) {
			endpoint, callbackCh, earlyStatus := s.progressEndpoint(env, `{"counter": "1"}`)
			_, err := env.startNexusOperation(
				ctx,
				&workflowservice.StartNexusOperationExecutionRequest{
					OperationId:         testvars.New(s.T()).Any().String(),
					Endpoint:            endpoint,
					CompletionCallbacks: cbs,
				},
			)
			s.NoError(err)
			callback := s.Rcv(callbackCh)
			s.Equal(http.StatusBadRequest, s.Rcv(earlyStatus), "refused before the start")
			status, err := postNexusProgress(ctx, callback[0], callback[1], `{"counter": "2"}`)
			s.NoError(err)
			s.Equal(http.StatusBadRequest, status, "and after it")
		})
	}
}
