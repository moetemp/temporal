package frontend

import (
	"context"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
)

// StartNexusOperation runs one operation of the System Nexus endpoint for the caller's namespace,
// as a direct call. It creates no caller-side state and no History event, so a Workflow can read
// through it with nothing in its History.
func (wh *WorkflowHandler) StartNexusOperation(
	ctx context.Context,
	req *workflowservice.StartNexusOperationRequest,
) (*workflowservice.StartNexusOperationResponse, error) {
	if !wh.config.EnableDirectNexusCalls(req.GetNamespace()) {
		return nil, serviceerror.NewUnimplemented("Direct Nexus calls are disabled")
	}
	if req.GetEndpoint() != commonnexus.SystemEndpoint {
		return nil, serviceerror.NewUnimplemented(
			"Direct Nexus calls serve only the __temporal_system endpoint",
		)
	}
	ns, err := wh.namespaceRegistry.GetNamespace(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}
	requestID := req.GetRequestId()
	if requestID == "" {
		requestID = uuid.NewString()
	}
	processed, err := wh.registry.NexusEndpointProcessor.ProcessInput(
		chasm.NexusOperationProcessorContext{
			Namespace:               ns,
			RequestID:               requestID,
			ReserializeInputPayload: true,
		},
		req.GetService(),
		req.GetOperation(),
		req.GetInput(),
	)
	if err != nil {
		return nil, err
	}
	resp, err := wh.historyClient.StartNexusOperation(
		ctx,
		&historyservice.StartNexusOperationRequest{
			NamespaceId: ns.ID().String(),
			ShardId:     processed.RoutingKey.ShardID(wh.config.NumHistoryShards),
			Request: &nexuspb.StartOperationRequest{
				Service:   req.GetService(),
				Operation: req.GetOperation(),
				Payload:   processed.ReserializedInputPayload,
				RequestId: requestID,
				Links:     workflowEventLinks(req.GetLinks(), requestID),
			},
		},
	)
	if err != nil {
		return nil, err
	}
	switch v := resp.GetResponse().GetVariant().(type) {
	case *nexuspb.StartOperationResponse_SyncSuccess:
		return &workflowservice.StartNexusOperationResponse{
			Variant: &workflowservice.StartNexusOperationResponse_SyncSuccess_{
				SyncSuccess: &workflowservice.StartNexusOperationResponse_SyncSuccess{
					Payload: v.SyncSuccess.GetPayload(),
				},
			},
			Links: v.SyncSuccess.GetLinks(),
		}, nil
	case *nexuspb.StartOperationResponse_AsyncSuccess:
		return &workflowservice.StartNexusOperationResponse{
			Variant: &workflowservice.StartNexusOperationResponse_AsyncSuccess_{
				AsyncSuccess: &workflowservice.StartNexusOperationResponse_AsyncSuccess{
					OperationToken: v.AsyncSuccess.GetOperationToken(),
				},
			},
			Links: v.AsyncSuccess.GetLinks(),
		}, nil
	case *nexuspb.StartOperationResponse_Failure:
		return &workflowservice.StartNexusOperationResponse{
			Variant: &workflowservice.StartNexusOperationResponse_OperationFailure_{
				OperationFailure: &workflowservice.StartNexusOperationResponse_OperationFailure{
					Failure: v.Failure,
				},
			},
		}, nil
	default:
		return nil, serviceerror.NewInternal("unexpected Nexus start response")
	}
}

// workflowEventLinks gives an operation the caller's Workflow links, so it knows the reading run.
// The Nexus form of a link needs a reference, so a link with none refers to the call's request id.
func workflowEventLinks(links []*commonpb.Link, requestID string) []*nexuspb.Link {
	var out []nexus.Link
	for _, link := range links {
		event := link.GetWorkflowEvent()
		if event == nil {
			continue
		}
		if event.GetReference() == nil {
			event = common.CloneProto(event)
			event.Reference = &commonpb.Link_WorkflowEvent_RequestIdRef{
				RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{RequestId: requestID},
			}
		}
		out = append(out, commonnexus.ConvertLinkWorkflowEventToNexusLink(event))
	}
	return commonnexus.ConvertLinksToProto(out)
}
