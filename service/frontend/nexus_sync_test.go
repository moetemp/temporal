package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/service/frontend/configs"
	"go.uber.org/mock/gomock"
)

const (
	syncTestCallerNamespace = "caller-ns"
	syncTestCallerID        = "caller-ns-id"
	syncTestTargetNamespace = "target-ns"
	syncTestTargetID        = "target-ns-id"
	syncTestEndpoint        = "stream-endpoint"
	syncTestTaskQueue       = "stream-tq"
)

// fakeSyncRateLimiter records the target namespace checks and answers with err.
type fakeSyncRateLimiter struct {
	interceptor.NamespaceRateLimitInterceptor
	calls []string
	err   error
}

func (f *fakeSyncRateLimiter) Allow(
	_ context.Context,
	namespaceName namespace.Name,
	methodName string,
	_ headers.HeaderGetter,
) error {
	f.calls = append(f.calls, namespaceName.String()+" "+methodName)
	return f.err
}

// fakeSyncAuthorizer records each call target and answers with err.
type fakeSyncAuthorizer struct {
	targets []*authorization.CallTarget
	err     error
}

func (f *fakeSyncAuthorizer) Authorize(
	_ context.Context,
	_ *authorization.Claims,
	target *authorization.CallTarget,
) (*commonpb.Principal, error) {
	f.targets = append(f.targets, target)
	return nil, f.err
}

// fakeSyncCountLimiter records the target namespace checks and answers with err.
type fakeSyncCountLimiter struct {
	calls    []string
	released int
	err      error
}

func (f *fakeSyncCountLimiter) Allow(
	namespaceName namespace.Name,
	methodName string,
	_ metrics.Handler,
	_ any,
) (func(), error) {
	f.calls = append(f.calls, namespaceName.String()+" "+methodName)
	return func() { f.released++ }, f.err
}

type syncTestEnv struct {
	handler       *WorkflowHandler
	matching      *matchingservicemock.MockMatchingServiceClient
	rateLimiter   *fakeSyncRateLimiter
	authorizer    *fakeSyncAuthorizer
	countLimiter  *fakeSyncCountLimiter
	endpointCalls []namespace.ID
}

func newSyncTestEnv(
	t *testing.T,
	enabled bool,
	targetActive bool,
	targetOpts ...func(*persistencespb.NamespaceInfo),
) *syncTestEnv {
	ctrl := gomock.NewController(t)
	dc := dynamicconfig.NewNoopCollection()

	activeCluster := cluster.TestCurrentClusterName
	if !targetActive {
		activeCluster = cluster.TestAlternativeClusterName
	}
	callerNS := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: syncTestCallerID, Name: syncTestCallerNamespace},
		nil,
		cluster.TestCurrentClusterName,
	)
	targetInfo := &persistencespb.NamespaceInfo{
		Id:    syncTestTargetID,
		Name:  syncTestTargetNamespace,
		State: enumspb.NAMESPACE_STATE_REGISTERED,
	}
	for _, opt := range targetOpts {
		opt(targetInfo)
	}
	targetNS := namespace.NewGlobalNamespaceForTest(
		targetInfo,
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: activeCluster,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1,
	)
	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().
		GetNamespace(namespace.Name(syncTestCallerNamespace)).
		Return(callerNS, nil).
		AnyTimes()
	nsRegistry.EXPECT().
		GetNamespaceByID(namespace.ID(syncTestTargetID)).
		Return(targetNS, nil).
		AnyTimes()

	clusterMetadata := cluster.NewMockMetadata(ctrl)
	clusterMetadata.EXPECT().
		GetCurrentClusterName().
		Return(cluster.TestCurrentClusterName).
		AnyTimes()

	env := &syncTestEnv{
		matching:     matchingservicemock.NewMockMatchingServiceClient(ctrl),
		rateLimiter:  &fakeSyncRateLimiter{},
		authorizer:   &fakeSyncAuthorizer{},
		countLimiter: &fakeSyncCountLimiter{},
	}
	registry := nexustest.FakeEndpointRegistry{
		OnGetByName: func(
			_ context.Context,
			namespaceID namespace.ID,
			name string,
		) (*persistencespb.NexusEndpointEntry, error) {
			env.endpointCalls = append(env.endpointCalls, namespaceID)
			switch name {
			case syncTestEndpoint:
				return &persistencespb.NexusEndpointEntry{
					Id: "endpoint-id",
					Endpoint: &persistencespb.NexusEndpoint{
						Spec: &persistencespb.NexusEndpointSpec{
							Name: name,
							Target: &persistencespb.NexusEndpointTarget{
								Variant: &persistencespb.NexusEndpointTarget_Worker_{
									Worker: &persistencespb.NexusEndpointTarget_Worker{
										NamespaceId: syncTestTargetID,
										TaskQueue:   syncTestTaskQueue,
									},
								},
							},
						},
					},
				}, nil
			case "external-endpoint":
				return &persistencespb.NexusEndpointEntry{
					Id: "external-id",
					Endpoint: &persistencespb.NexusEndpoint{
						Spec: &persistencespb.NexusEndpointSpec{
							Name: name,
							Target: &persistencespb.NexusEndpointTarget{
								Variant: &persistencespb.NexusEndpointTarget_External_{
									External: &persistencespb.NexusEndpointTarget_External{
										Url: "http://localhost",
									},
								},
							},
						},
					},
				}, nil
			default:
				return nil, serviceerror.NewNotFoundf(
					"could not find Nexus endpoint by name: %v",
					name,
				)
			}
		},
	}

	env.handler = &WorkflowHandler{
		config: &Config{
			EnableExecuteSyncNexusOperation: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(
				enabled,
			),
		},
		logger:               log.NewNoopLogger(),
		namespaceRegistry:    nsRegistry,
		clusterMetadata:      clusterMetadata,
		matchingClient:       env.matching,
		endpointRegistry:     registry,
		namespaceRateLimiter: env.rateLimiter,
		syncNexusChecks: SyncNexusChecks{
			Authorizer: env.authorizer,
			StateChecker: interceptor.NewNamespaceValidatorInterceptor(
				nsRegistry,
				dynamicconfig.GetBoolPropertyFn(false),
				dynamicconfig.GetIntPropertyFn(1000),
				nil,
			),
			CountLimiter: env.countLimiter,
		},
		nexusOperationConfig: &chasmnexus.Config{
			RequestTimeout:             chasmnexus.RequestTimeout.Get(dc),
			MaxServiceNameLength:       chasmnexus.MaxServiceNameLength.Get(dc),
			MaxOperationNameLength:     chasmnexus.MaxOperationNameLength.Get(dc),
			MaxOperationHeaderSize:     chasmnexus.MaxOperationHeaderSize.Get(dc),
			DisallowedOperationHeaders: chasmnexus.DisallowedOperationHeaders.Get(dc),
			PayloadSizeLimit:           dynamicconfig.BlobSizeLimitError.Get(dc),
			PayloadSizeLimitWarn:       dynamicconfig.BlobSizeLimitWarn.Get(dc),
			MaxIDLengthLimit:           dynamicconfig.MaxIDLengthLimit.Get(dc),
		},
	}
	return env
}

func syncTestRequest() *workflowservice.ExecuteSyncNexusOperationRequest {
	return &workflowservice.ExecuteSyncNexusOperationRequest{
		Namespace:   syncTestCallerNamespace,
		RequestId:   "request-id",
		Endpoint:    syncTestEndpoint,
		Service:     "temporal.sdk.streams.v1.TemporalStreams",
		Operation:   "read",
		Input:       &commonpb.Payload{Data: []byte(`{"stream":"s"}`)},
		NexusHeader: map[string]string{"X-Trace": "abc"},
	}
}

func syncTestDispatchResponse(
	start *nexuspb.StartOperationResponse,
) *matchingservice.DispatchNexusTaskResponse {
	return &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{StartOperation: start},
			},
		},
	}
}

func TestExecuteSyncNexusOperation_SyncResult(t *testing.T) {
	env := newSyncTestEnv(t, true, true)
	result := &commonpb.Payload{Data: []byte(`{"records":[]}`)}

	env.matching.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			ctx context.Context,
			req *matchingservice.DispatchNexusTaskRequest,
			_ ...any,
		) (*matchingservice.DispatchNexusTaskResponse, error) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.LessOrEqual(t, time.Until(deadline), 10*time.Second)

			require.Equal(t, syncTestTargetID, req.GetNamespaceId())
			require.Equal(t, syncTestTaskQueue, req.GetTaskQueue().GetName())
			require.Equal(t, syncTestEndpoint, req.GetRequest().GetEndpoint())
			require.Equal(t, map[string]string{
				"x-trace":                             "abc",
				commonnexus.CallerNamespaceHeaderName: syncTestCallerNamespace,
			}, req.GetRequest().GetHeader())
			require.True(t, req.GetRequest().GetCapabilities().GetTemporalFailureResponses())
			start := req.GetRequest().GetStartOperation()
			require.Equal(t, "temporal.sdk.streams.v1.TemporalStreams", start.GetService())
			require.Equal(t, "read", start.GetOperation())
			require.Equal(t, "request-id", start.GetRequestId())
			require.Empty(t, start.GetCallback())
			require.JSONEq(t, `{"stream":"s"}`, string(start.GetPayload().GetData()))
			return syncTestDispatchResponse(&nexuspb.StartOperationResponse{
				Variant: &nexuspb.StartOperationResponse_SyncSuccess{
					SyncSuccess: &nexuspb.StartOperationResponse_Sync{Payload: result},
				},
			}), nil
		})

	resp, err := env.handler.ExecuteSyncNexusOperation(context.Background(), syncTestRequest())
	require.NoError(t, err)
	require.Equal(t, result.GetData(), resp.GetResult().GetData())
	require.Equal(t, []namespace.ID{syncTestCallerID}, env.endpointCalls)
	require.Equal(t,
		[]string{syncTestTargetNamespace + " " + configs.DispatchNexusTaskByEndpointAPIName},
		env.rateLimiter.calls)
}

func TestExecuteSyncNexusOperation_SyncResultWithNoValue(t *testing.T) {
	env := newSyncTestEnv(t, true, true)
	env.matching.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).Return(
		syncTestDispatchResponse(&nexuspb.StartOperationResponse{
			Variant: &nexuspb.StartOperationResponse_SyncSuccess{
				SyncSuccess: &nexuspb.StartOperationResponse_Sync{},
			},
		}), nil)

	resp, err := env.handler.ExecuteSyncNexusOperation(context.Background(), syncTestRequest())
	require.NoError(t, err)
	require.NotNil(t, resp.GetResult())
}

func TestExecuteSyncNexusOperation_Failures(t *testing.T) {
	handlerFailure := &failurepb.Failure{
		Message: "bad stream name",
		FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
			NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{Type: "BAD_REQUEST"},
		},
	}
	operationFailure := &failurepb.Failure{
		Message: "replay mismatch",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type: "StreamReplayMismatchError",
			},
		},
	}
	canceledFailure := &failurepb.Failure{
		Message: "canceled",
		FailureInfo: &failurepb.Failure_CanceledFailureInfo{
			CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
		},
	}

	cases := []struct {
		name     string
		response *matchingservice.DispatchNexusTaskResponse
		want     *failurepb.Failure
	}{
		{
			name: "handler error",
			response: &matchingservice.DispatchNexusTaskResponse{
				Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
					Failure: handlerFailure,
				},
			},
			want: handlerFailure,
		},
		{
			name: "operation failure",
			response: syncTestDispatchResponse(&nexuspb.StartOperationResponse{
				Variant: &nexuspb.StartOperationResponse_Failure{Failure: operationFailure},
			}),
			want: operationFailure,
		},
		{
			name: "operation canceled",
			response: syncTestDispatchResponse(&nexuspb.StartOperationResponse{
				Variant: &nexuspb.StartOperationResponse_Failure{Failure: canceledFailure},
			}),
			want: canceledFailure,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newSyncTestEnv(t, true, true)
			env.matching.EXPECT().
				DispatchNexusTask(gomock.Any(), gomock.Any()).
				Return(tc.response, nil)

			resp, err := env.handler.ExecuteSyncNexusOperation(
				context.Background(),
				syncTestRequest(),
			)
			require.NoError(t, err)
			require.Nil(t, resp.GetResult())
			require.Equal(t, tc.want.GetMessage(), resp.GetFailure().GetMessage())
			require.Equal(t, tc.want.GetFailureInfo(), resp.GetFailure().GetFailureInfo())
		})
	}
}

func TestExecuteSyncNexusOperation_AsyncAnswerRefused(t *testing.T) {
	env := newSyncTestEnv(t, true, true)
	env.matching.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).Return(
		syncTestDispatchResponse(&nexuspb.StartOperationResponse{
			Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
				AsyncSuccess: &nexuspb.StartOperationResponse_Async{OperationToken: "token"},
			},
		}), nil)

	_, err := env.handler.ExecuteSyncNexusOperation(context.Background(), syncTestRequest())
	var precondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &precondition)
}

func TestExecuteSyncNexusOperation_NoWorkerAnswered(t *testing.T) {
	env := newSyncTestEnv(t, true, true)
	env.matching.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).Return(
		&matchingservice.DispatchNexusTaskResponse{
			Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
				RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
			},
		}, nil)

	_, err := env.handler.ExecuteSyncNexusOperation(context.Background(), syncTestRequest())
	var deadline *serviceerror.DeadlineExceeded
	require.ErrorAs(t, err, &deadline)
}

func TestExecuteSyncNexusOperation_RefusedBeforeDispatch(t *testing.T) {
	cases := []struct {
		name         string
		enabled      bool
		targetActive bool
		rateLimitErr error
		mutate       func(*workflowservice.ExecuteSyncNexusOperationRequest)
		check        func(*testing.T, error)
	}{
		{
			name:         "disabled",
			enabled:      false,
			targetActive: true,
			check: func(t *testing.T, err error) {
				var unimplemented *serviceerror.Unimplemented
				require.ErrorAs(t, err, &unimplemented)
			},
		},
		{
			name:         "unknown endpoint",
			enabled:      true,
			targetActive: true,
			mutate: func(req *workflowservice.ExecuteSyncNexusOperationRequest) {
				req.Endpoint = "missing"
			},
			check: func(t *testing.T, err error) {
				var notFound *serviceerror.NotFound
				require.ErrorAs(t, err, &notFound)
			},
		},
		{
			name:         "external endpoint",
			enabled:      true,
			targetActive: true,
			mutate: func(req *workflowservice.ExecuteSyncNexusOperationRequest) {
				req.Endpoint = "external-endpoint"
			},
			check: func(t *testing.T, err error) {
				var invalid *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalid)
			},
		},
		{
			name:         "target namespace not active",
			enabled:      true,
			targetActive: false,
			check: func(t *testing.T, err error) {
				var notActive *serviceerror.NamespaceNotActive
				require.ErrorAs(t, err, &notActive)
				require.Equal(t, cluster.TestAlternativeClusterName, notActive.ActiveCluster)
			},
		},
		{
			name:         "target namespace rate limited",
			enabled:      true,
			targetActive: true,
			rateLimitErr: serviceerror.NewResourceExhausted(0, "namespace rate limit exceeded"),
			check: func(t *testing.T, err error) {
				var exhausted *serviceerror.ResourceExhausted
				require.ErrorAs(t, err, &exhausted)
			},
		},
		{
			name:         "input above the blob size limit",
			enabled:      true,
			targetActive: true,
			mutate: func(req *workflowservice.ExecuteSyncNexusOperationRequest) {
				limitFn := dynamicconfig.BlobSizeLimitError.Get(dynamicconfig.NewNoopCollection())
				limit := limitFn(syncTestCallerNamespace)
				req.Input = &commonpb.Payload{Data: make([]byte, limit+1)}
			},
			check: func(t *testing.T, err error) {
				var invalid *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalid)
				require.Contains(t, invalid.Error(), "input exceeds size limit")
			},
		},
		{
			name:         "missing service",
			enabled:      true,
			targetActive: true,
			mutate: func(req *workflowservice.ExecuteSyncNexusOperationRequest) {
				req.Service = ""
			},
			check: func(t *testing.T, err error) {
				var invalid *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalid)
			},
		},
		{
			name:         "caller namespace header from the client",
			enabled:      true,
			targetActive: true,
			mutate: func(req *workflowservice.ExecuteSyncNexusOperationRequest) {
				req.NexusHeader = map[string]string{"Temporal-Caller-Namespace": "someone-else"}
			},
			check: func(t *testing.T, err error) {
				var invalid *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalid)
			},
		},
		{
			name:         "disallowed header",
			enabled:      true,
			targetActive: true,
			mutate: func(req *workflowservice.ExecuteSyncNexusOperationRequest) {
				req.NexusHeader = map[string]string{"Request-Timeout": "1s"}
			},
			check: func(t *testing.T, err error) {
				var invalid *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalid)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newSyncTestEnv(t, tc.enabled, tc.targetActive)
			env.rateLimiter.err = tc.rateLimitErr
			req := syncTestRequest()
			if tc.mutate != nil {
				tc.mutate(req)
			}
			// The matching mock fails the test if anything dispatches.
			_, err := env.handler.ExecuteSyncNexusOperation(context.Background(), req)
			tc.check(t, err)
		})
	}
}

func TestExecuteSyncNexusOperation_AuthorizesTheEndpoint(t *testing.T) {
	env := newSyncTestEnv(t, true, true)
	env.authorizer.err = serviceerror.NewPermissionDenied("endpoint not allowed", "")
	_, err := env.handler.ExecuteSyncNexusOperation(context.Background(), syncTestRequest())
	var denied *serviceerror.PermissionDenied
	require.ErrorAs(t, err, &denied)
	require.Len(t, env.authorizer.targets, 1)
	require.Equal(t, syncTestCallerNamespace, env.authorizer.targets[0].Namespace)
	require.Equal(t, syncTestEndpoint, env.authorizer.targets[0].NexusEndpointName)
	require.Empty(t, env.endpointCalls, "a denied caller learns nothing about the endpoint")
}

func TestExecuteSyncNexusOperation_RefusesATargetThatTakesNoTasks(t *testing.T) {
	env := newSyncTestEnv(t, true, true, func(info *persistencespb.NamespaceInfo) {
		info.State = enumspb.NAMESPACE_STATE_DELETED
	})
	_, err := env.handler.ExecuteSyncNexusOperation(context.Background(), syncTestRequest())
	var invalidState *serviceerror.NamespaceInvalidState
	require.ErrorAs(t, err, &invalidState, "the same answer as on the Nexus HTTP route")
	require.Empty(t, env.countLimiter.calls)
}

func TestExecuteSyncNexusOperation_TakesTheTargetConcurrencyLimit(t *testing.T) {
	env := newSyncTestEnv(t, true, true)
	env.countLimiter.err = serviceerror.NewResourceExhausted(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "too many")
	_, err := env.handler.ExecuteSyncNexusOperation(context.Background(), syncTestRequest())
	var exhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t,
		[]string{syncTestTargetNamespace + " " + configs.DispatchNexusTaskByEndpointAPIName},
		env.countLimiter.calls)
	require.Empty(t, env.rateLimiter.calls)
}
