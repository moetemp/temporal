package streamnotifier

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	streamnotifierpb "go.temporal.io/server/chasm/lib/streamnotifier/gen/streamnotifierpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

// maxMetadataBytes bounds a notification's metadata, so progress stays a notification and the data
// travels on the read path.
const maxMetadataBytes = 2 * 1024

// maxPositionBytes bounds a notification's position, which every callback copies into its
// caller's history.
const maxPositionBytes = 1024

// FrontendHandler serves the stream notifier RPCs of the WorkflowService.
type FrontendHandler interface {
	AttachStreamCallback(context.Context, *workflowservice.AttachStreamCallbackRequest) (*workflowservice.AttachStreamCallbackResponse, error)
	DetachStreamCallback(context.Context, *workflowservice.DetachStreamCallbackRequest) (*workflowservice.DetachStreamCallbackResponse, error)
	NotifyStream(context.Context, *workflowservice.NotifyStreamRequest) (*workflowservice.NotifyStreamResponse, error)
	DescribeStreamNotifier(context.Context, *workflowservice.DescribeStreamNotifierRequest) (*workflowservice.DescribeStreamNotifierResponse, error)
}

type frontendHandler struct {
	client            streamnotifierpb.StreamNotifierServiceClient
	config            *Config
	namespaceRegistry namespace.Registry
	callbackValidator callbacks.Validator
	metricsHandler    metrics.Handler
	logger            log.Logger
}

func NewFrontendHandler(
	client streamnotifierpb.StreamNotifierServiceClient,
	config *Config,
	namespaceRegistry namespace.Registry,
	callbackValidator callbacks.Validator,
	metricsHandler metrics.Handler,
	logger log.Logger,
) FrontendHandler {
	return &frontendHandler{
		client:            client,
		config:            config,
		namespaceRegistry: namespaceRegistry,
		callbackValidator: callbackValidator,
		metricsHandler:    metricsHandler,
		logger:            logger,
	}
}

// resolve checks that the notifier is enabled and the stream is one it serves, and answers the
// namespace ID and the notifier's business ID.
func (h *frontendHandler) resolve(namespaceName string, ref *streampb.StreamReference) (namespaceID string, businessID string, err error) {
	if !h.config.Enabled(namespaceName) {
		return "", "", serviceerror.NewUnimplemented("the stream notifier is not enabled for this namespace")
	}
	// The notifier is a CHASM execution of its own, whatever the callers' operations run on.
	if !h.config.ChasmEnabled(namespaceName) {
		return "", "", serviceerror.NewUnimplemented("the stream notifier requires CHASM, which is not enabled for this namespace")
	}
	if err := ValidateReference(ref); err != nil {
		return "", "", err
	}
	// Both become part of the notifier's business ID.
	if len(ref.GetWorkflowId()) > h.config.MaxIDLength() {
		return "", "", serviceerror.NewInvalidArgument("stream_ref.workflow_id is too long")
	}
	if len(ref.GetTopic()) > h.config.MaxIDLength() {
		return "", "", serviceerror.NewInvalidArgument("stream_ref.topic is too long")
	}
	id, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(namespaceName))
	if err != nil {
		return "", "", err
	}
	return id.String(), BusinessID(ref), nil
}

// ValidateReference rejects a stream reference the notifier cannot key a stream by.
func ValidateReference(ref *streampb.StreamReference) error {
	if ref.GetOwnerKind() != enumspb.STREAM_OWNER_KIND_WORKFLOW {
		return serviceerror.NewInvalidArgument("stream_ref.owner_kind must be STREAM_OWNER_KIND_WORKFLOW")
	}
	if ref.GetWorkflowId() == "" {
		return serviceerror.NewInvalidArgument("stream_ref.workflow_id is required")
	}
	if ref.GetTopic() == "" {
		return serviceerror.NewInvalidArgument("stream_ref.topic is required")
	}
	// The run chain's first run keys the notifier, so a later chain reusing the Workflow ID gets
	// its own; without it the request would reach a different notifier.
	if ref.GetRunId() == "" {
		return serviceerror.NewInvalidArgument("stream_ref.run_id must name the run chain's first run")
	}
	return nil
}

func (h *frontendHandler) AttachStreamCallback(
	ctx context.Context,
	req *workflowservice.AttachStreamCallbackRequest,
) (*workflowservice.AttachStreamCallbackResponse, error) {
	namespaceID, businessID, err := h.resolve(req.GetNamespace(), req.GetStreamRef())
	if err != nil {
		return nil, err
	}
	if req.GetRequestId() == "" {
		return nil, serviceerror.NewInvalidArgument("request_id is required")
	}
	if len(req.GetRequestId()) > h.config.MaxIDLength() {
		return nil, serviceerror.NewInvalidArgument("request_id is too long")
	}
	if req.GetCallback().GetUrl() == "" {
		return nil, serviceerror.NewInvalidArgument("callback.url is required")
	}
	// The validator checks the URL against the namespace's allowed addresses and normalizes the
	// headers in place.
	cb := &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{Nexus: req.GetCallback()}}
	if err := h.callbackValidator.Validate(ctx, req.GetNamespace(), []*commonpb.Callback{cb}, callbacks.ValidatorOptions{
		EnabledKinds: []callbacks.Kind{callbacks.KindNexus},
	}); err != nil {
		return nil, err
	}
	resp, err := h.client.AttachStreamCallback(ctx, &streamnotifierpb.AttachStreamCallbackRequest{
		NamespaceId:     namespaceID,
		BusinessId:      businessID,
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}

func (h *frontendHandler) DetachStreamCallback(
	ctx context.Context,
	req *workflowservice.DetachStreamCallbackRequest,
) (*workflowservice.DetachStreamCallbackResponse, error) {
	namespaceID, businessID, err := h.resolve(req.GetNamespace(), req.GetStreamRef())
	if err != nil {
		return nil, err
	}
	if req.GetRequestId() == "" {
		return nil, serviceerror.NewInvalidArgument("request_id is required")
	}
	if len(req.GetRequestId()) > h.config.MaxIDLength() {
		return nil, serviceerror.NewInvalidArgument("request_id is too long")
	}
	resp, err := h.client.DetachStreamCallback(ctx, &streamnotifierpb.DetachStreamCallbackRequest{
		NamespaceId:     namespaceID,
		BusinessId:      businessID,
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}

func (h *frontendHandler) NotifyStream(
	ctx context.Context,
	req *workflowservice.NotifyStreamRequest,
) (*workflowservice.NotifyStreamResponse, error) {
	namespaceID, businessID, err := h.resolve(req.GetNamespace(), req.GetStreamRef())
	if err != nil {
		return nil, err
	}
	if req.GetCounter() <= 0 {
		return nil, serviceerror.NewInvalidArgument("counter must be positive")
	}
	if len(req.GetPosition()) > maxPositionBytes {
		return nil, serviceerror.NewInvalidArgumentf("position is %d bytes, more than the %d allowed", len(req.GetPosition()), maxPositionBytes)
	}
	size := 0
	for key, value := range req.GetMetadata() {
		size += len(key) + len(value)
	}
	if size > maxMetadataBytes {
		return nil, serviceerror.NewInvalidArgumentf("metadata is %d bytes, more than the %d allowed", size, maxMetadataBytes)
	}
	if req.GetCloseResult() != nil && !req.GetClose() {
		return nil, serviceerror.NewInvalidArgument("close_result is only allowed with close")
	}
	if err := common.CheckEventBlobSizeLimit(
		req.GetCloseResult().Size(),
		h.config.BlobSizeLimitWarn(req.GetNamespace()),
		h.config.BlobSizeLimitError(req.GetNamespace()),
		req.GetNamespace(),
		req.GetStreamRef().GetWorkflowId(),
		"",
		h.metricsHandler,
		h.logger,
		"NotifyStream",
	); err != nil {
		return nil, err
	}
	resp, err := h.client.NotifyStream(ctx, &streamnotifierpb.NotifyStreamRequest{
		NamespaceId:     namespaceID,
		BusinessId:      businessID,
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}

func (h *frontendHandler) DescribeStreamNotifier(
	ctx context.Context,
	req *workflowservice.DescribeStreamNotifierRequest,
) (*workflowservice.DescribeStreamNotifierResponse, error) {
	namespaceID, businessID, err := h.resolve(req.GetNamespace(), req.GetStreamRef())
	if err != nil {
		return nil, err
	}
	resp, err := h.client.DescribeStreamNotifier(ctx, &streamnotifierpb.DescribeStreamNotifierRequest{
		NamespaceId:     namespaceID,
		BusinessId:      businessID,
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}
