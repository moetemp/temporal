package streamnotifier

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	chasmcallback "go.temporal.io/server/chasm/lib/callback"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	streamnotifierpb "go.temporal.io/server/chasm/lib/streamnotifier/gen/streamnotifierpb/v1"
	"go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

// SystemNexusHandler serves the notifier RPCs a Workflow calls through the __temporal_system
// endpoint. It runs on the history host that owns the stream's notifier, since the operation is
// routed by the notifier's business ID, so it calls the notifier in process.
type SystemNexusHandler interface {
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
}

// localClient hands requests to the notifier's handler without a network hop.
type localClient struct {
	handler *handler
}

func (c localClient) AttachStreamCallback(
	ctx context.Context,
	in *streamnotifierpb.AttachStreamCallbackRequest,
	_ ...grpc.CallOption,
) (*streamnotifierpb.AttachStreamCallbackResponse, error) {
	return c.handler.AttachStreamCallback(ctx, in)
}

func (c localClient) DetachStreamCallback(
	ctx context.Context,
	in *streamnotifierpb.DetachStreamCallbackRequest,
	_ ...grpc.CallOption,
) (*streamnotifierpb.DetachStreamCallbackResponse, error) {
	return c.handler.DetachStreamCallback(ctx, in)
}

func (c localClient) NotifyStream(
	ctx context.Context,
	in *streamnotifierpb.NotifyStreamRequest,
	_ ...grpc.CallOption,
) (*streamnotifierpb.NotifyStreamResponse, error) {
	return c.handler.NotifyStream(ctx, in)
}

func (c localClient) DescribeStreamNotifier(
	ctx context.Context,
	in *streamnotifierpb.DescribeStreamNotifierRequest,
	_ ...grpc.CallOption,
) (*streamnotifierpb.DescribeStreamNotifierResponse, error) {
	return c.handler.DescribeStreamNotifier(ctx, in)
}

// newSystemNexusHandler applies the same checks as the frontend, with the callback rules the
// frontend reads, so a Workflow cannot attach or notify anything a client could not.
func newSystemNexusHandler(
	h *handler,
	config *Config,
	namespaceRegistry namespace.Registry,
	dc *dynamicconfig.Collection,
	metricsHandler metrics.Handler,
	logger log.Logger,
) (SystemNexusHandler, error) {
	validator, err := callbacks.NewValidator(callbacks.ValidatorConfig{
		MaxCallbacksPerExecution:         chasmcallback.MaxPerExecution.Get(dc),
		MaxIDLengthLimit:                 dynamicconfig.MaxIDLengthLimit.Get(dc),
		URLMaxLength:                     dynamicconfig.FrontendCallbackURLMaxLength.Get(dc),
		HeaderMaxSize:                    dynamicconfig.FrontendCallbackHeaderMaxSize.Get(dc),
		EndpointRules:                    chasmcallback.AllowedAddresses.Get(dc),
		MaxServiceNameLength:             chasmnexus.MaxServiceNameLength.Get(dc),
		MaxOperationNameLength:           chasmnexus.MaxOperationNameLength.Get(dc),
		NexusHandlerSourceContextMaxSize: chasmcallback.NexusHandlerSourceContextMaxSize.Get(dc),
	}, namespaceRegistry)
	if err != nil {
		return nil, err
	}
	return &frontendHandler{
		client:            localClient{handler: h},
		config:            config,
		namespaceRegistry: namespaceRegistry,
		callbackValidator: validator,
		metricsHandler:    metricsHandler,
		logger:            logger,
	}, nil
}
