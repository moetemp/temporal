package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	cnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

const executeSyncNexusOperationAPIName = api.WorkflowServicePrefix + "ExecuteSyncNexusOperation"

// NexusSyncSuite covers ExecuteSyncNexusOperation, the frontend call that runs one sync Nexus
// operation for a caller namespace and persists nothing.
type NexusSyncSuite struct {
	parallelsuite.Suite[*NexusSyncSuite]
}

func TestNexusSyncSuite(t *testing.T) {
	parallelsuite.Run(t, &NexusSyncSuite{})
}

// syncNexusTaskAnswer is what a test worker sends back for one Nexus task. Set exactly one field.
type syncNexusTaskAnswer struct {
	response *nexuspb.Response
	failure  *failurepb.Failure
}

func (s *NexusSyncSuite) newEnv(opts ...testcore.TestOption) *testcore.TestEnv {
	opts = append([]testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.FrontendEnableExecuteSyncNexusOperation, true),
	}, opts...)
	return testcore.NewEnv(s.T(), opts...)
}

// registerNamespace registers another namespace, so the caller and the handler namespaces differ
// as they do for a stream reader.
func (s *NexusSyncSuite) registerNamespace(env *testcore.TestEnv, prefix string) namespace.Name {
	name := namespace.Name(testcore.RandomizeStr(prefix))
	_, err := env.RegisterNamespace(s.Context(), name, 1, enumspb.ARCHIVAL_STATE_DISABLED, "", "")
	s.NoError(err)
	return name
}

// registerRateLimitedNamespace registers a namespace with two requests per second and no burst
// above that. The limit is set first, since the frontend reads it when it first sees the
// namespace. Registration itself makes a few calls, so the limit can't be lower.
func (s *NexusSyncSuite) registerRateLimitedNamespace(env *testcore.TestEnv) namespace.Name {
	name := namespace.Name(testcore.RandomizeStr("sync-limited"))
	constraint := dynamicconfig.Constraints{Namespace: name.String()}
	s.T().Cleanup(env.OverrideDynamicConfig(dynamicconfig.FrontendMaxNamespaceRPSPerInstance,
		[]dynamicconfig.ConstrainedValue{{Constraints: constraint, Value: 2}}))
	s.T().Cleanup(env.OverrideDynamicConfig(dynamicconfig.FrontendMaxNamespaceBurstRatioPerInstance,
		[]dynamicconfig.ConstrainedValue{{Constraints: constraint, Value: 1.0}}))
	_, err := env.RegisterNamespace(s.Context(), name, 1, enumspb.ARCHIVAL_STATE_DISABLED, "", "")
	s.NoError(err)
	return name
}

func (s *NexusSyncSuite) createEndpoint(
	env *testcore.TestEnv,
	targetNamespace namespace.Name,
	taskQueue string,
) string {
	return s.createEndpointEntry(env, targetNamespace, taskQueue).GetSpec().GetName()
}

func (s *NexusSyncSuite) createEndpointEntry(
	env *testcore.TestEnv,
	targetNamespace namespace.Name,
	taskQueue string,
) *nexuspb.Endpoint {
	name := testcore.RandomizedNexusEndpoint(s.T().Name())
	resp, err := env.OperatorClient().
		CreateNexusEndpoint(s.Context(), &operatorservice.CreateNexusEndpointRequest{
			Spec: &nexuspb.EndpointSpec{
				Name: name,
				Target: &nexuspb.EndpointTarget{
					Variant: &nexuspb.EndpointTarget_Worker_{
						Worker: &nexuspb.EndpointTarget_Worker{
							Namespace: targetNamespace.String(),
							TaskQueue: taskQueue,
						},
					},
				},
			},
		})
	s.NoError(err)
	s.T().Cleanup(func() {
		_, _ = env.OperatorClient().
			DeleteNexusEndpoint(testcore.NewContext(), &operatorservice.DeleteNexusEndpointRequest{
				Id:      resp.Endpoint.Id,
				Version: resp.Endpoint.Version,
			})
	})
	return resp.GetEndpoint()
}

// answerOneTask polls one Nexus task in the target namespace and answers it. The task goes to
// seen before the answer, so the test can check what the handler got.
func (s *NexusSyncSuite) answerOneTask(
	env *testcore.TestEnv,
	targetNamespace namespace.Name,
	taskQueue string,
	answer syncNexusTaskAnswer,
	seen chan<- *workflowservice.PollNexusTaskQueueResponse,
) <-chan error {
	ctx := s.Context()
	errCh := make(chan error, 1)
	go func() {
		res, err := env.FrontendClient().
			PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
				Namespace: targetNamespace.String(),
				Identity:  uuid.NewString(),
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueue,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
			})
		if err != nil {
			errCh <- err
			return
		}
		if res.GetTaskToken() == nil {
			errCh <- errors.New("poll returned no task")
			return
		}
		seen <- res
		if answer.failure != nil {
			_, err = env.FrontendClient().
				RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
					Namespace: targetNamespace.String(),
					Identity:  uuid.NewString(),
					TaskToken: res.GetTaskToken(),
					Failure:   answer.failure,
				})
		} else {
			req := &workflowservice.RespondNexusTaskCompletedRequest{
				Namespace: targetNamespace.String(),
				Identity:  uuid.NewString(),
				TaskToken: res.GetTaskToken(),
				Response:  answer.response,
			}
			_, err = env.FrontendClient().RespondNexusTaskCompleted(ctx, req)
		}
		errCh <- err
	}()
	return errCh
}

func syncStartAnswer(start *nexuspb.StartOperationResponse) syncNexusTaskAnswer {
	return syncNexusTaskAnswer{response: &nexuspb.Response{
		Variant: &nexuspb.Response_StartOperation{StartOperation: start},
	}}
}

func syncSuccessAnswer(data string) syncNexusTaskAnswer {
	return syncStartAnswer(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_SyncSuccess{
			SyncSuccess: &nexuspb.StartOperationResponse_Sync{
				Payload: &commonpb.Payload{Data: []byte(data)},
			},
		},
	})
}

func syncNexusRequest(
	callerNamespace namespace.Name,
	endpoint string,
) *workflowservice.ExecuteSyncNexusOperationRequest {
	return &workflowservice.ExecuteSyncNexusOperationRequest{
		Namespace:   callerNamespace.String(),
		Identity:    "sync-test",
		RequestId:   uuid.NewString(),
		Endpoint:    endpoint,
		Service:     "temporal.sdk.streams.v1.TemporalStreams",
		Operation:   "read",
		Input:       &commonpb.Payload{Data: []byte(`{"stream":"s1"}`)},
		NexusHeader: map[string]string{"X-Trace-Id": "trace-1"},
	}
}

func (s *NexusSyncSuite) requireOneResourceExhausted(errs []error) {
	for _, err := range errs {
		if _, ok := errors.AsType[*serviceerror.ResourceExhausted](err); ok {
			return
		}
	}
	s.Failf("no call was rate limited", "errors: %v", errs)
}

func (s *NexusSyncSuite) TestSyncResult() {
	env := s.newEnv()
	target := s.registerNamespace(env, "sync-target")
	taskQueue := testcore.RandomizeStr("sync-tq")
	endpoint := s.createEndpoint(env, target, taskQueue)

	seen := make(chan *workflowservice.PollNexusTaskQueueResponse, 1)
	done := s.answerOneTask(env, target, taskQueue, syncSuccessAnswer(`{"records":[]}`), seen)

	req := syncNexusRequest(env.Namespace(), endpoint)
	resp, err := env.FrontendClient().ExecuteSyncNexusOperation(s.Context(), req)
	s.NoError(err)
	s.JSONEq(`{"records":[]}`, string(resp.GetResult().GetData()))
	s.NoError(s.Rcv(done))

	task := s.Rcv(seen)
	s.Equal(endpoint, task.GetRequest().GetEndpoint())
	s.Equal("trace-1", task.GetRequest().GetHeader()["x-trace-id"])
	start := task.GetRequest().GetStartOperation()
	s.Equal("temporal.sdk.streams.v1.TemporalStreams", start.GetService())
	s.Equal("read", start.GetOperation())
	s.Equal(req.GetRequestId(), start.GetRequestId())
	s.Empty(start.GetCallback())
	s.JSONEq(`{"stream":"s1"}`, string(start.GetPayload().GetData()))
	callerNamespace := task.GetRequest().GetHeader()[cnexus.CallerNamespaceHeaderName]
	s.Equal(env.Namespace().String(), callerNamespace)
}

func (s *NexusSyncSuite) TestHandlerError() {
	env := s.newEnv()
	taskQueue := testcore.RandomizeStr("sync-tq")
	endpoint := s.createEndpoint(env, env.Namespace(), taskQueue)

	seen := make(chan *workflowservice.PollNexusTaskQueueResponse, 1)
	done := s.answerOneTask(
		env,
		env.Namespace(),
		taskQueue,
		syncNexusTaskAnswer{failure: &failurepb.Failure{
			Message: "stream name is not valid",
			FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
				NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
					Type:          "BAD_REQUEST",
					RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE,
				},
			},
		}},
		seen,
	)

	resp, err := env.FrontendClient().
		ExecuteSyncNexusOperation(s.Context(), syncNexusRequest(env.Namespace(), endpoint))
	s.NoError(err)
	s.NoError(s.Rcv(done))
	s.Nil(resp.GetResult())
	s.Equal("stream name is not valid", resp.GetFailure().GetMessage())
	s.Equal("BAD_REQUEST", resp.GetFailure().GetNexusHandlerFailureInfo().GetType())
}

func (s *NexusSyncSuite) TestOperationFailure() {
	env := s.newEnv()
	taskQueue := testcore.RandomizeStr("sync-tq")
	endpoint := s.createEndpoint(env, env.Namespace(), taskQueue)

	seen := make(chan *workflowservice.PollNexusTaskQueueResponse, 1)
	done := s.answerOneTask(
		env,
		env.Namespace(),
		taskQueue,
		syncStartAnswer(&nexuspb.StartOperationResponse{
			Variant: &nexuspb.StartOperationResponse_Failure{
				Failure: &failurepb.Failure{
					Message: "replay range lost",
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
						ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
							Type:         "StreamReplayRangeLostError",
							NonRetryable: true,
						},
					},
				},
			},
		}),
		seen,
	)

	resp, err := env.FrontendClient().
		ExecuteSyncNexusOperation(s.Context(), syncNexusRequest(env.Namespace(), endpoint))
	s.NoError(err)
	s.NoError(s.Rcv(done))
	s.Equal("replay range lost", resp.GetFailure().GetMessage())
	s.Equal("StreamReplayRangeLostError", resp.GetFailure().GetApplicationFailureInfo().GetType())
}

func (s *NexusSyncSuite) TestAsyncAnswerRefused() {
	env := s.newEnv()
	taskQueue := testcore.RandomizeStr("sync-tq")
	endpoint := s.createEndpoint(env, env.Namespace(), taskQueue)

	seen := make(chan *workflowservice.PollNexusTaskQueueResponse, 1)
	done := s.answerOneTask(
		env,
		env.Namespace(),
		taskQueue,
		syncStartAnswer(&nexuspb.StartOperationResponse{
			Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
				AsyncSuccess: &nexuspb.StartOperationResponse_Async{OperationToken: "token"},
			},
		}),
		seen,
	)

	_, err := env.FrontendClient().
		ExecuteSyncNexusOperation(s.Context(), syncNexusRequest(env.Namespace(), endpoint))
	s.NoError(s.Rcv(done))
	var precondition *serviceerror.FailedPrecondition
	s.ErrorAs(err, &precondition)
}

func (s *NexusSyncSuite) TestCallerNamespaceHeaderFromClientRefused() {
	env := s.newEnv()
	endpoint := s.createEndpoint(env, env.Namespace(), testcore.RandomizeStr("sync-tq"))
	req := syncNexusRequest(env.Namespace(), endpoint)
	req.NexusHeader = map[string]string{"Temporal-Caller-Namespace": "someone-else"}

	_, err := env.FrontendClient().ExecuteSyncNexusOperation(s.Context(), req)
	var invalid *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalid)
}

func (s *NexusSyncSuite) TestHTTPRouteRemovesCallerNamespaceHeader() {
	env := s.newEnv()
	taskQueue := testcore.RandomizeStr("sync-tq")
	endpoint := s.createEndpointEntry(env, env.Namespace(), taskQueue)

	seen := make(chan *workflowservice.PollNexusTaskQueueResponse, 1)
	answer := syncStartAnswer(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_SyncSuccess{
			SyncSuccess: &nexuspb.StartOperationResponse_Sync{
				Payload: &commonpb.Payload{
					Metadata: map[string][]byte{"encoding": []byte("json/plain")},
					Data:     []byte(`"ok"`),
				},
			},
		},
	})
	done := s.answerOneTask(env, env.Namespace(), taskQueue, answer, seen)

	client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
		BaseURL: "http://" + env.HttpAPIAddress() + "/" +
			cnexus.RouteDispatchNexusTaskByEndpoint.Path(endpoint.GetId()),
		Service: "temporal.sdk.streams.v1.TemporalStreams",
	})
	s.NoError(err)
	_, err = client.StartOperation(s.Context(), "read", "input", nexus.StartOperationOptions{
		Header: nexus.Header{
			"x-trace-id":                     "trace-1",
			cnexus.CallerNamespaceHeaderName: "forged-namespace",
		},
	})
	s.NoError(err)
	s.NoError(s.Rcv(done))

	task := s.Rcv(seen)
	s.Equal("trace-1", task.GetRequest().GetHeader()["x-trace-id"])
	s.NotContains(task.GetRequest().GetHeader(), cnexus.CallerNamespaceHeaderName)
}

func (s *NexusSyncSuite) TestUnknownEndpoint() {
	env := s.newEnv()
	req := syncNexusRequest(env.Namespace(), "no-such-endpoint")
	_, err := env.FrontendClient().ExecuteSyncNexusOperation(s.Context(), req)
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound)
}

func (s *NexusSyncSuite) TestNoPollerDeadline() {
	env := s.newEnv()
	endpoint := s.createEndpoint(env, env.Namespace(), testcore.RandomizeStr("sync-tq"))

	ctx, cancel := context.WithTimeout(s.Context(), 2*time.Second)
	defer cancel()
	_, err := env.FrontendClient().
		ExecuteSyncNexusOperation(ctx, syncNexusRequest(env.Namespace(), endpoint))
	var deadline *serviceerror.DeadlineExceeded
	s.ErrorAs(err, &deadline)
}

func (s *NexusSyncSuite) TestDisabled() {
	env := testcore.NewEnv(s.T())
	endpoint := s.createEndpoint(env, env.Namespace(), testcore.RandomizeStr("sync-tq"))
	_, err := env.FrontendClient().
		ExecuteSyncNexusOperation(s.Context(), syncNexusRequest(env.Namespace(), endpoint))
	var unimplemented *serviceerror.Unimplemented
	s.ErrorAs(err, &unimplemented)
}

// TestAuthorizationAndRateLimits needs a dedicated cluster for the authorizer hook. The rate
// limit cases share it, since they set limits on namespaces other than the test namespace.
func (s *NexusSyncSuite) TestAuthorizationAndRateLimits() {
	env := s.newEnv(testcore.WithDedicatedCluster())
	target := s.registerNamespace(env, "sync-target")

	s.RunSequential("AuthorizesTheCallerNamespace", func(s *NexusSyncSuite) {
		taskQueue := testcore.RandomizeStr("sync-tq")
		endpoint := s.createEndpoint(env, target, taskQueue)

		targets := make(chan *authorization.CallTarget, 10)
		env.SetOnAuthorize(
			func(
				_ context.Context,
				_ *authorization.Claims,
				ct *authorization.CallTarget,
			) (authorization.Result, error) {
				if ct.APIName != executeSyncNexusOperationAPIName {
					return authorization.Result{Decision: authorization.DecisionAllow}, nil
				}
				targets <- ct
				// Deny the target namespace. The call must still pass, since the caller is checked.
				if ct.Namespace == target.String() {
					return authorization.Result{Decision: authorization.DecisionDeny}, nil
				}
				return authorization.Result{Decision: authorization.DecisionAllow}, nil
			},
		)
		defer env.SetOnAuthorize(nil)

		seen := make(chan *workflowservice.PollNexusTaskQueueResponse, 1)
		done := s.answerOneTask(env, target, taskQueue, syncSuccessAnswer(`{}`), seen)
		_, err := env.FrontendClient().
			ExecuteSyncNexusOperation(s.Context(), syncNexusRequest(env.Namespace(), endpoint))
		s.NoError(err)
		s.NoError(s.Rcv(done))
		s.Equal(env.Namespace().String(), s.Rcv(targets).Namespace)
	})

	s.RunSequential("DeniedCallerNamespace", func(s *NexusSyncSuite) {
		endpoint := s.createEndpoint(env, target, testcore.RandomizeStr("sync-tq"))
		env.SetOnAuthorize(
			func(
				_ context.Context,
				_ *authorization.Claims,
				ct *authorization.CallTarget,
			) (authorization.Result, error) {
				if ct.APIName == executeSyncNexusOperationAPIName &&
					ct.Namespace == env.Namespace().String() {
					return authorization.Result{Decision: authorization.DecisionDeny}, nil
				}
				return authorization.Result{Decision: authorization.DecisionAllow}, nil
			},
		)
		defer env.SetOnAuthorize(nil)

		_, err := env.FrontendClient().
			ExecuteSyncNexusOperation(s.Context(), syncNexusRequest(env.Namespace(), endpoint))
		var denied *serviceerror.PermissionDenied
		s.ErrorAs(err, &denied)
	})

	s.RunSequential("TargetNamespaceRateLimitCounts", func(s *NexusSyncSuite) {
		limited := s.registerRateLimitedNamespace(env)
		endpoint := s.createEndpoint(env, limited, testcore.RandomizeStr("sync-tq"))

		// Nobody polls, so each allowed call waits for its short deadline. The calls take tokens
		// faster than the bucket fills, so one of them finds it empty.
		var errs []error
		for range 5 {
			ctx, cancel := context.WithTimeout(s.Context(), 200*time.Millisecond)
			_, err := env.FrontendClient().
				ExecuteSyncNexusOperation(ctx, syncNexusRequest(env.Namespace(), endpoint))
			cancel()
			errs = append(errs, err)
		}
		s.requireOneResourceExhausted(errs)
	})

	s.RunSequential("CallerNamespaceRateLimitCounts", func(s *NexusSyncSuite) {
		limited := s.registerRateLimitedNamespace(env)

		// An unknown endpoint answers at once, so the calls come faster than two per second.
		var errs []error
		for range 5 {
			req := syncNexusRequest(limited, "no-such-endpoint")
			_, err := env.FrontendClient().ExecuteSyncNexusOperation(s.Context(), req)
			errs = append(errs, err)
		}
		s.requireOneResourceExhausted(errs)
	})
}
