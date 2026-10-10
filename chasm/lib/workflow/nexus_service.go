package workflow

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/streamnotifier"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/searchattribute"
)

var ErrSignalWithStartOperationDisabled = serviceerror.NewUnimplemented("SignalWithStart operation is disabled")

var errStreamNotifierUnavailable = serviceerror.NewUnimplemented("the stream notifier is not served by this host")

type workflowServiceNexusHandler struct {
	config            Config
	namespaceRegistry namespace.Registry
	historyHandler    historyservice.HistoryServiceServer
	streamNotifier    streamnotifier.SystemNexusHandler
}

// signalWithStartWorkflowExecution implements the SignalWithStartWorkflowExecution Nexus operation.
func (h *workflowServiceNexusHandler) signalWithStartWorkflowExecution(
	ctx context.Context,
	req *workflowservice.SignalWithStartWorkflowExecutionRequest,
	options nexus.StartOperationOptions,
) (*workflowservice.SignalWithStartWorkflowExecutionResponse, error) {
	if !h.config.enableSignalWithStartFromWorkflow(req.GetNamespace()) {
		return nil, ErrSignalWithStartOperationDisabled
	}
	nsID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, serviceerror.NewInvalidArgumentf("Invalid namespace %q: %v", req.GetNamespace(), err)
	}
	res, err := h.historyHandler.SignalWithStartWorkflowExecution(ctx, &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId:            nsID.String(),
		SignalWithStartRequest: req,
	})
	if err != nil {
		return nil, err
	}

	// Persist the link from the signaling workflow to its target workflow.
	// The backlink is already taken care of within the historyHandler.
	signalLink := res.GetSignalLink()
	link := commonnexus.ConvertLinkWorkflowEventToNexusLink(signalLink.GetWorkflowEvent())
	nexus.AddHandlerLinks(ctx, link)

	return &workflowservice.SignalWithStartWorkflowExecutionResponse{
		RunId:      res.GetRunId(),
		Started:    res.GetStarted(),
		SignalLink: signalLink,
	}, nil
}

func (h *workflowServiceNexusHandler) attachStreamCallback(
	ctx context.Context,
	req *workflowservice.AttachStreamCallbackRequest,
	_ nexus.StartOperationOptions,
) (*workflowservice.AttachStreamCallbackResponse, error) {
	if h.streamNotifier == nil {
		return nil, errStreamNotifierUnavailable
	}
	return h.streamNotifier.AttachStreamCallback(ctx, req)
}

func (h *workflowServiceNexusHandler) detachStreamCallback(
	ctx context.Context,
	req *workflowservice.DetachStreamCallbackRequest,
	_ nexus.StartOperationOptions,
) (*workflowservice.DetachStreamCallbackResponse, error) {
	if h.streamNotifier == nil {
		return nil, errStreamNotifierUnavailable
	}
	return h.streamNotifier.DetachStreamCallback(ctx, req)
}

func (h *workflowServiceNexusHandler) notifyStream(
	ctx context.Context,
	req *workflowservice.NotifyStreamRequest,
	_ nexus.StartOperationOptions,
) (*workflowservice.NotifyStreamResponse, error) {
	if h.streamNotifier == nil {
		return nil, errStreamNotifierUnavailable
	}
	return h.streamNotifier.NotifyStream(ctx, req)
}

func mustNewWorkflowServiceNexusHandler(
	handler *workflowServiceNexusHandler,
) *nexus.Service {
	ops := workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService
	svc := nexus.NewService(ops.ServiceName)
	svc.MustRegister(nexus.NewSyncOperation(
		ops.SignalWithStartWorkflowExecution.Name(),
		handler.signalWithStartWorkflowExecution,
	))
	svc.MustRegister(nexus.NewSyncOperation(ops.AttachStreamCallback.Name(), handler.attachStreamCallback))
	svc.MustRegister(nexus.NewSyncOperation(ops.DetachStreamCallback.Name(), handler.detachStreamCallback))
	svc.MustRegister(nexus.NewSyncOperation(ops.NotifyStream.Name(), handler.notifyStream))
	return svc
}

func (h *workflowServiceNexusHandler) setHistoryHandler(handler historyservice.HistoryServiceServer) {
	h.historyHandler = handler
}

func (h *workflowServiceNexusHandler) setStreamNotifier(handler streamnotifier.SystemNexusHandler) {
	h.streamNotifier = handler
}

// streamNotifierRequest is a notifier request a Workflow sends through System Nexus.
type streamNotifierRequest interface {
	GetNamespace() string
	GetStreamRef() *streampb.StreamReference
}

// streamNotifierOperationProcessor routes a notifier request to the history shard that owns the
// stream's notifier. The namespace comes from the calling Workflow and the identity is not taken
// from it, matching the System Nexus API that leaves both out.
type streamNotifierOperationProcessor[R streamNotifierRequest] struct {
	setNamespace  func(R, string)
	clearIdentity func(R)
}

func (o streamNotifierOperationProcessor[R]) ProcessInput(ctx chasm.NexusOperationProcessorContext, request R) (*chasm.NexusOperationProcessorResult, error) {
	if any(request) == nil {
		return nil, serviceerror.NewInvalidArgument("Request is empty")
	}
	if request.GetNamespace() == "" {
		o.setNamespace(request, ctx.Namespace.Name().String())
	} else if request.GetNamespace() != ctx.Namespace.Name().String() {
		return nil, serviceerror.NewInvalidArgumentf("Namespace in request %q does not match namespace in context %q", request.GetNamespace(), ctx.Namespace.Name().String())
	}
	o.clearIdentity(request)
	if err := streamnotifier.ValidateReference(request.GetStreamRef()); err != nil {
		return nil, err
	}
	return &chasm.NexusOperationProcessorResult{
		RoutingKey: chasm.NexusOperationRoutingKeyExecution{
			NamespaceID: ctx.Namespace.ID().String(),
			BusinessID:  streamnotifier.BusinessID(request.GetStreamRef()),
		},
	}, nil
}

type SignalWithStartOperationProcessor struct {
	validator *RequestValidator
}

func (o SignalWithStartOperationProcessor) ProcessInput(ctx chasm.NexusOperationProcessorContext, request *workflowservice.SignalWithStartWorkflowExecutionRequest) (*chasm.NexusOperationProcessorResult, error) {
	if !o.validator.config.enableSignalWithStartFromWorkflow(ctx.Namespace.Name().String()) {
		return nil, ErrSignalWithStartOperationDisabled
	}
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("Request is empty")
	}
	if request.GetNamespace() == "" {
		request.Namespace = ctx.Namespace.Name().String()
	} else if request.GetNamespace() != ctx.Namespace.Name().String() {
		return nil, serviceerror.NewInvalidArgumentf("Namespace in request %q does not match namespace in context %q", request.GetNamespace(), ctx.Namespace.Name().String())
	}

	if request.GetRequestId() != "" {
		return nil, serviceerror.NewInvalidArgument("RequestID should not be set on the request")
	}
	request.RequestId = ctx.RequestID

	if len(request.GetLinks()) > 0 {
		return nil, serviceerror.NewInvalidArgument("Links should not be set on the request")
	}
	request.Links = make([]*commonpb.Link, len(ctx.Links))
	for i, link := range ctx.Links {
		wLink, err := commonnexus.ConvertNexusLinkToLinkWorkflowEvent(link)
		if err != nil {
			return nil, serviceerror.NewInvalidArgumentf("Cannot convert %v link %v: %v", link.Type, link.URL, err)
		}
		request.Links[i] = &commonpb.Link{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: wLink,
			},
		}
	}

	if err := o.validator.ValidateSignalWithStartRequest(request); err != nil {
		return nil, err
	}

	return &chasm.NexusOperationProcessorResult{
		RoutingKey: chasm.NexusOperationRoutingKeyExecution{
			NamespaceID: ctx.Namespace.ID().String(),
			BusinessID:  request.WorkflowId,
		},
	}, nil
}

func NewWorkflowServiceNexusServiceProcessor(
	config Config,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
) *chasm.NexusServiceProcessor {
	ops := workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService
	sp := chasm.NewNexusServiceProcessor(ops.ServiceName)
	op := SignalWithStartOperationProcessor{validator: NewValidator(config, saMapperProvider, saValidator)}
	sp.MustRegisterOperation(
		ops.SignalWithStartWorkflowExecution.Name(),
		chasm.NewRegisterableNexusOperationProcessor(op),
	)
	sp.MustRegisterOperation(ops.AttachStreamCallback.Name(), chasm.NewRegisterableNexusOperationProcessor(
		streamNotifierOperationProcessor[*workflowservice.AttachStreamCallbackRequest]{
			setNamespace:  func(r *workflowservice.AttachStreamCallbackRequest, ns string) { r.Namespace = ns },
			clearIdentity: func(r *workflowservice.AttachStreamCallbackRequest) { r.Identity = "" },
		},
	))
	sp.MustRegisterOperation(ops.DetachStreamCallback.Name(), chasm.NewRegisterableNexusOperationProcessor(
		streamNotifierOperationProcessor[*workflowservice.DetachStreamCallbackRequest]{
			setNamespace:  func(r *workflowservice.DetachStreamCallbackRequest, ns string) { r.Namespace = ns },
			clearIdentity: func(r *workflowservice.DetachStreamCallbackRequest) { r.Identity = "" },
		},
	))
	sp.MustRegisterOperation(ops.NotifyStream.Name(), chasm.NewRegisterableNexusOperationProcessor(
		streamNotifierOperationProcessor[*workflowservice.NotifyStreamRequest]{
			setNamespace:  func(r *workflowservice.NotifyStreamRequest, ns string) { r.Namespace = ns },
			clearIdentity: func(r *workflowservice.NotifyStreamRequest) { r.Identity = "" },
		},
	))
	return sp
}
