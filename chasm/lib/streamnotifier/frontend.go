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
	"golang.org/x/sync/errgroup"
)

// maxMetadataBytes bounds a notification's metadata, so progress stays a notification and the data
// travels on the read path.
const maxMetadataBytes = 2 * 1024

// maxSubscriptionIDBytes bounds a Workflow reader's subscription ID, which every wake copies into
// the reader's History.
const maxSubscriptionIDBytes = 256

// maxPositionBytes bounds a notification's position, which every callback copies into its
// caller's history.
const maxPositionBytes = 1024

// notifyBatchParallelism bounds the notifier calls one NotifyStreams request has in flight.
const notifyBatchParallelism = 32

// FrontendHandler serves the stream notifier RPCs of the WorkflowService.
type FrontendHandler interface {
	AttachStreamCallback(
		context.Context,
		*workflowservice.AttachStreamCallbackRequest,
	) (*workflowservice.AttachStreamCallbackResponse, error)
	DetachStreamCallback(
		context.Context,
		*workflowservice.DetachStreamCallbackRequest,
	) (*workflowservice.DetachStreamCallbackResponse, error)
	NotifyStream(
		context.Context,
		*workflowservice.NotifyStreamRequest,
	) (*workflowservice.NotifyStreamResponse, error)
	DescribeStreamNotifier(
		context.Context,
		*workflowservice.DescribeStreamNotifierRequest,
	) (*workflowservice.DescribeStreamNotifierResponse, error)
	PollStreamNotifier(
		context.Context,
		*workflowservice.PollStreamNotifierRequest,
	) (*workflowservice.PollStreamNotifierResponse, error)
	NotifyStreams(
		context.Context,
		*workflowservice.NotifyStreamsRequest,
	) (*workflowservice.NotifyStreamsResponse, error)
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
func (h *frontendHandler) resolve(
	namespaceName string,
	ref *streampb.StreamReference,
) (namespaceID string, businessID string, err error) {
	if !h.config.Enabled(namespaceName) {
		return "", "", serviceerror.NewUnimplemented(
			"the stream notifier is not enabled for this namespace",
		)
	}
	// The notifier is a CHASM execution of its own, whatever the callers' operations run on.
	if !h.config.ChasmEnabled(namespaceName) {
		return "", "", serviceerror.NewUnimplemented(
			"the stream notifier requires CHASM, which is not enabled for this namespace",
		)
	}
	if err := h.validateOwner(namespaceName, ref); err != nil {
		return "", "", err
	}
	if ref.GetTopic() == "" {
		return "", "", serviceerror.NewInvalidArgument("stream_ref.topic is required")
	}
	// The run that keys the notifier: without it the request would reach a different notifier.
	if ref.GetRunId() == "" {
		return "", "", serviceerror.NewInvalidArgument(
			"stream_ref.run_id must name the run chain's first run or the Activity run",
		)
	}
	// They become part of the notifier's business ID.
	if len(ref.GetTopic()) > h.config.MaxIDLength() {
		return "", "", serviceerror.NewInvalidArgument("stream_ref.topic is too long")
	}
	id, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(namespaceName))
	if err != nil {
		return "", "", err
	}
	return id.String(), BusinessID(ref), nil
}

// validateOwner checks the owner fields of each kind. A kind that is off for the namespace is a
// failed precondition and never Unimplemented, since Core takes Unimplemented as a server with no
// notifier and stops its notifier calls for every stream.
func (h *frontendHandler) validateOwner(
	namespaceName string,
	ref *streampb.StreamReference,
) error {
	switch ref.GetOwnerKind() {
	case enumspb.STREAM_OWNER_KIND_WORKFLOW:
		if ref.GetWorkflowId() == "" {
			return serviceerror.NewInvalidArgument("stream_ref.workflow_id is required")
		}
		if ref.GetActivityId() != "" {
			return serviceerror.NewInvalidArgument(
				"stream_ref.activity_id must be empty for STREAM_OWNER_KIND_WORKFLOW",
			)
		}
		if len(ref.GetWorkflowId()) > h.config.MaxIDLength() {
			return serviceerror.NewInvalidArgument("stream_ref.workflow_id is too long")
		}
	case enumspb.STREAM_OWNER_KIND_ACTIVITY:
		if ref.GetActivityId() == "" {
			return serviceerror.NewInvalidArgument("stream_ref.activity_id is required")
		}
		if ref.GetWorkflowId() != "" {
			return serviceerror.NewInvalidArgument(
				"stream_ref.workflow_id must be empty for STREAM_OWNER_KIND_ACTIVITY",
			)
		}
		if len(ref.GetActivityId()) > h.config.MaxIDLength() {
			return serviceerror.NewInvalidArgument("stream_ref.activity_id is too long")
		}
		if !h.config.ActivityEnabled(namespaceName) {
			return serviceerror.NewFailedPrecondition(
				"streams owned by a standalone Activity need standalone Activities, " +
					"which are not enabled for this namespace",
			)
		}
	default:
		return serviceerror.NewInvalidArgument(
			"stream_ref.owner_kind must be STREAM_OWNER_KIND_WORKFLOW or " +
				"STREAM_OWNER_KIND_ACTIVITY",
		)
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
	if (req.GetCallback() == nil) == (req.GetWorkflowTarget() == nil) {
		return nil, serviceerror.NewInvalidArgument(
			"exactly one of callback and workflow_target is required",
		)
	}
	if target := req.GetWorkflowTarget(); target != nil {
		if err := h.validateWorkflowTarget(target); err != nil {
			return nil, err
		}
	} else {
		if req.GetCallback().GetUrl() == "" {
			return nil, serviceerror.NewInvalidArgument("callback.url is required")
		}
		// The validator checks the URL against the namespace's allowed addresses and normalizes
		// the headers in place.
		cb := &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{Nexus: req.GetCallback()}}
		opts := callbacks.ValidatorOptions{EnabledKinds: []callbacks.Kind{callbacks.KindNexus}}
		err = h.callbackValidator.Validate(ctx, req.GetNamespace(), []*commonpb.Callback{cb}, opts)
		if err != nil {
			return nil, err
		}
	}
	resp, err := h.client.AttachStreamCallback(ctx, &streamnotifierpb.AttachStreamCallbackRequest{
		NamespaceId:     namespaceID,
		BusinessId:      businessID,
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}

// validateWorkflowTarget checks a Workflow reader's target. The Workflow is in the request's
// namespace, so a target names no namespace.
func (h *frontendHandler) validateWorkflowTarget(target *streampb.WorkflowWakeTarget) error {
	if target.GetWorkflowId() == "" || target.GetRunId() == "" {
		return serviceerror.NewInvalidArgument(
			"workflow_target must name a workflow ID and a run ID",
		)
	}
	if len(target.GetWorkflowId()) > h.config.MaxIDLength() ||
		len(target.GetRunId()) > h.config.MaxIDLength() {
		return serviceerror.NewInvalidArgument("workflow_target names an ID that is too long")
	}
	if target.GetSubscriptionId() == "" {
		return serviceerror.NewInvalidArgument("workflow_target.subscription_id is required")
	}
	if len(target.GetSubscriptionId()) > maxSubscriptionIDBytes {
		return serviceerror.NewInvalidArgumentf(
			"workflow_target.subscription_id is %d bytes, more than the %d allowed",
			len(target.GetSubscriptionId()),
			maxSubscriptionIDBytes,
		)
	}
	return nil
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
	err = validateNotification(req.GetPosition(), req.GetCounter(), req.GetMetadata())
	if err != nil {
		return nil, err
	}
	if req.GetCloseResult() != nil && !req.GetClose() {
		return nil, serviceerror.NewInvalidArgument("close_result is only allowed with close")
	}
	if req.GetCloseFailure() != nil && !req.GetClose() {
		return nil, serviceerror.NewInvalidArgument("close_failure is only allowed with close")
	}
	if req.GetCloseResult() != nil && req.GetCloseFailure() != nil {
		return nil, serviceerror.NewInvalidArgument(
			"a close carries close_result or close_failure, not both",
		)
	}
	// Only one of the two is set, so the sum is the size of the one the callers get.
	if err := common.CheckEventBlobSizeLimit(
		req.GetCloseResult().Size()+req.GetCloseFailure().Size(),
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

// validateNotification checks the fields that NotifyStream and a NotifyStreams entry share.
func validateNotification(position string, counter int64, metadata map[string]string) error {
	if counter <= 0 {
		return serviceerror.NewInvalidArgument("counter must be positive")
	}
	if len(position) > maxPositionBytes {
		return serviceerror.NewInvalidArgumentf(
			"position is %d bytes, more than the %d allowed",
			len(position),
			maxPositionBytes,
		)
	}
	size := 0
	for key, value := range metadata {
		size += len(key) + len(value)
	}
	if size > maxMetadataBytes {
		return serviceerror.NewInvalidArgumentf(
			"metadata is %d bytes, more than the %d allowed",
			size,
			maxMetadataBytes,
		)
	}
	return nil
}

// NotifyStreams checks every entry before it applies any, so a bad entry fails the request and
// leaves no notifier changed. Then it applies each entry as a NotifyStream on the notifier of its
// stream. The first failure fails the request, and the entries already applied stay applied.
func (h *frontendHandler) NotifyStreams(
	ctx context.Context,
	req *workflowservice.NotifyStreamsRequest,
) (*workflowservice.NotifyStreamsResponse, error) {
	entries := req.GetNotifications()
	if len(entries) == 0 {
		return nil, serviceerror.NewInvalidArgument("notifications is empty")
	}
	if limit := h.config.MaxNotifyBatch(req.GetNamespace()); len(entries) > limit {
		return nil, serviceerror.NewInvalidArgumentf(
			"notifications has %d entries, more than the %d allowed",
			len(entries),
			limit,
		)
	}
	requests := make([]*streamnotifierpb.NotifyStreamRequest, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for i, entry := range entries {
		namespaceID, businessID, err := h.resolve(req.GetNamespace(), entry.GetStreamRef())
		if err != nil {
			return nil, err
		}
		if err := validateNotification(
			entry.GetPosition(),
			entry.GetCounter(),
			entry.GetMetadata(),
		); err != nil {
			return nil, err
		}
		// Two entries for one stream would race on its notifier, and a driver folds them anyway.
		if _, ok := seen[businessID]; ok {
			return nil, serviceerror.NewInvalidArgumentf(
				"notifications has two entries for the stream %q",
				businessID,
			)
		}
		seen[businessID] = struct{}{}
		requests[i] = &streamnotifierpb.NotifyStreamRequest{
			NamespaceId: namespaceID,
			BusinessId:  businessID,
			FrontendRequest: &workflowservice.NotifyStreamRequest{
				Namespace: req.GetNamespace(),
				StreamRef: entry.GetStreamRef(),
				Position:  entry.GetPosition(),
				Counter:   entry.GetCounter(),
				Metadata:  entry.GetMetadata(),
				Identity:  req.GetIdentity(),
			},
		}
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(notifyBatchParallelism)
	for _, request := range requests {
		group.Go(func() error {
			_, err := h.client.NotifyStream(groupCtx, request)
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return &workflowservice.NotifyStreamsResponse{}, nil
}

func (h *frontendHandler) DescribeStreamNotifier(
	ctx context.Context,
	req *workflowservice.DescribeStreamNotifierRequest,
) (*workflowservice.DescribeStreamNotifierResponse, error) {
	namespaceID, businessID, err := h.resolve(req.GetNamespace(), req.GetStreamRef())
	if err != nil {
		return nil, err
	}
	resp, err := h.client.DescribeStreamNotifier(
		ctx,
		&streamnotifierpb.DescribeStreamNotifierRequest{
			NamespaceId:     namespaceID,
			BusinessId:      businessID,
			FrontendRequest: req,
		},
	)
	return resp.GetFrontendResponse(), err
}

func (h *frontendHandler) PollStreamNotifier(
	ctx context.Context,
	req *workflowservice.PollStreamNotifierRequest,
) (*workflowservice.PollStreamNotifierResponse, error) {
	namespaceID, businessID, err := h.resolve(req.GetNamespace(), req.GetStreamRef())
	if err != nil {
		return nil, err
	}
	if req.GetAfterCounter() < 0 {
		return nil, serviceerror.NewInvalidArgument("after_counter must not be negative")
	}
	resp, err := h.client.PollStreamNotifier(ctx, &streamnotifierpb.PollStreamNotifierRequest{
		NamespaceId:     namespaceID,
		BusinessId:      businessID,
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}
