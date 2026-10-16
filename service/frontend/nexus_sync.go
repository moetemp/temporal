package frontend

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/service/frontend/configs"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	errExecuteSyncNexusOperationDisabled = serviceerror.NewUnimplemented(
		"ExecuteSyncNexusOperation is disabled for this namespace")
	errSyncNexusOperationAnsweredAsync = serviceerror.NewFailedPrecondition(
		"the Nexus operation answered asynchronously. " +
			"Use StartNexusOperationExecution for asynchronous operations")
	errSyncNexusOperationNoAnswer = serviceerror.NewDeadlineExceeded(
		"no worker answered the Nexus task before the deadline")
)

// ExecuteSyncNexusOperation runs one synchronous Nexus operation for a caller in the request
// namespace. The gRPC interceptors already authorized the caller and counted its rate limit
// against the request namespace. Nothing is persisted, and the caller owns the retries.
func (wh *WorkflowHandler) ExecuteSyncNexusOperation(
	ctx context.Context,
	request *workflowservice.ExecuteSyncNexusOperationRequest,
) (_ *workflowservice.ExecuteSyncNexusOperationResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}
	if request.GetNamespace() == "" {
		return nil, errNamespaceNotSet
	}
	if !wh.config.EnableExecuteSyncNexusOperation(request.GetNamespace()) {
		return nil, errExecuteSyncNexusOperationDisabled
	}

	callerNamespace, err := wh.namespaceRegistry.GetNamespace(
		namespace.Name(request.GetNamespace()),
	)
	if err != nil {
		return nil, err
	}
	err = chasmnexus.ValidateAndNormalizeSyncRequest(wh.nexusOperationConfig, wh.logger, request)
	if err != nil {
		return nil, err
	}

	// The gRPC interceptor authorized the caller namespace without the endpoint. An authorizer that
	// limits endpoints by name on the Nexus HTTP route gets the name here too.
	claims, _ := ctx.Value(authorization.MappedClaims).(*authorization.Claims)
	if _, err := wh.syncNexusChecks.Authorizer.Authorize(ctx, claims, &authorization.CallTarget{
		APIName:           api.WorkflowServicePrefix + "ExecuteSyncNexusOperation",
		Namespace:         callerNamespace.Name().String(),
		NexusEndpointName: request.GetEndpoint(),
		Request:           request,
	}); err != nil {
		return nil, err
	}

	// The registry is the allowlist seam. It answers NotFound for an endpoint that the caller
	// namespace can't reach, so a caller can't probe endpoint names.
	entry, err := wh.endpointRegistry.GetByName(ctx, callerNamespace.ID(), request.GetEndpoint())
	if err != nil {
		return nil, err
	}
	workerTarget := entry.GetEndpoint().GetSpec().GetTarget().GetWorker()
	if workerTarget == nil {
		return nil, serviceerror.NewInvalidArgumentf(
			"endpoint %q has no worker target. ExecuteSyncNexusOperation calls worker targets only",
			request.GetEndpoint())
	}

	targetNamespace, err := wh.namespaceRegistry.GetNamespaceByID(
		namespace.ID(workerTarget.GetNamespaceId()),
	)
	if err != nil {
		return nil, err
	}
	// A target that is deleted or in a handover takes no task, as on the Nexus HTTP route.
	err = wh.syncNexusChecks.StateChecker.ValidateState(
		targetNamespace,
		configs.DispatchNexusTaskByEndpointAPIName,
		namespace.EmptyBusinessID,
	)
	if err != nil {
		return nil, err
	}
	currentCluster := wh.clusterMetadata.GetCurrentClusterName()
	//nolint:forbidigo // Nexus requests are not tied to a business ID, as on the Nexus HTTP route.
	if !targetNamespace.ActiveInCluster(currentCluster) {
		return nil, serviceerror.NewNamespaceNotActive(
			targetNamespace.Name().String(),
			currentCluster,
			targetNamespace.ActiveClusterName(namespace.RoutingKey{}),
		)
	}
	release, err := wh.syncNexusChecks.CountLimiter.Allow(
		targetNamespace.Name(),
		configs.DispatchNexusTaskByEndpointAPIName,
		wh.metricsScope(ctx),
		request,
	)
	if err != nil {
		return nil, err
	}
	defer release()
	// The target namespace pays for the dispatch as it does for a call on the Nexus HTTP route.
	if err := wh.namespaceRateLimiter.Allow(
		ctx,
		targetNamespace.Name(),
		configs.DispatchNexusTaskByEndpointAPIName,
		headers.NewGRPCHeaderGetter(ctx),
	); err != nil {
		return nil, err
	}

	// A shorter caller deadline wins. Matching passes the deadline to the handler.
	ctx, cancel := context.WithTimeout(
		ctx,
		wh.nexusOperationConfig.RequestTimeout(
			callerNamespace.Name().String(),
			request.GetEndpoint(),
		),
	)
	defer cancel()

	resp, err := wh.matchingClient.DispatchNexusTask(ctx, &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: targetNamespace.ID().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: workerTarget.GetTaskQueue(),
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Request: &nexuspb.Request{
			Header:        request.GetNexusHeader(),
			ScheduledTime: timestamppb.New(time.Now().UTC()),
			Endpoint:      request.GetEndpoint(),
			Variant: &nexuspb.Request_StartOperation{
				StartOperation: &nexuspb.StartOperationRequest{
					Service:   request.GetService(),
					Operation: request.GetOperation(),
					RequestId: request.GetRequestId(),
					Payload:   request.GetInput(),
				},
			},
			Capabilities: &nexuspb.Request_Capabilities{
				TemporalFailureResponses: true,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return syncNexusOperationResponse(commonnexus.ClassifyStartOperationDispatch(resp))
}

func syncNexusOperationResponse(
	result commonnexus.DispatchResult,
) (*workflowservice.ExecuteSyncNexusOperationResponse, error) {
	switch result.Outcome {
	case commonnexus.DispatchOutcomeSyncSuccess:
		payload := result.OperationResult
		if payload == nil {
			// An operation may succeed with no value. The response still says that it succeeded.
			payload = &commonpb.Payload{}
		}
		return &workflowservice.ExecuteSyncNexusOperationResponse{
			Outcome: &workflowservice.ExecuteSyncNexusOperationResponse_Result{Result: payload},
		}, nil
	case commonnexus.DispatchOutcomeHandlerFailure, commonnexus.DispatchOutcomeOperationFailure:
		return &workflowservice.ExecuteSyncNexusOperationResponse{
			Outcome: &workflowservice.ExecuteSyncNexusOperationResponse_Failure{
				Failure: result.Failure,
			},
		}, nil
	case commonnexus.DispatchOutcomeAsyncSuccess:
		return nil, errSyncNexusOperationAnsweredAsync
	case commonnexus.DispatchOutcomeRequestTimeout:
		return nil, errSyncNexusOperationNoAnswer
	default:
		return nil, serviceerror.NewInternalf(
			"unrecognized Nexus dispatch outcome: %s",
			result.Outcome,
		)
	}
}
