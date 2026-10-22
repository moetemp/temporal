package stream

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/streamservice/v1"
	"go.temporal.io/server/chasm"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

// ServiceName is the System Nexus service of the Stream operations.
const ServiceName = "temporal.api.streamservice.v1.StreamService"

// callerOf gives the caller of a System Nexus operation. A Workflow caller is in the links of
// the start, which point at the event that scheduled the operation.
func callerOf(options nexus.StartOperationOptions) Caller {
	caller := Caller{RequestID: options.RequestID}
	for _, link := range options.Links {
		if event, err := commonnexus.ConvertNexusLinkToLinkWorkflowEvent(link); err == nil {
			caller.WorkflowID, caller.RunID = event.GetWorkflowId(), event.GetRunId()
			break
		}
	}
	if options.CallbackURL != "" {
		caller.Callback = &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{
			Url:    options.CallbackURL,
			Header: options.CallbackHeader,
		}}}
	}
	return caller
}

// nexusError gives the Nexus form of a Stream error. A Stream error fails the operation with
// an application failure whose type is the reason token, so the Workflow sees the same error
// class as a client. Any other error stays a handler error, which the caller retries.
func nexusError(err error) error {
	if err == nil {
		return nil
	}
	if reason := ReasonOf(err); reason != "" && reason != ReasonStorage {
		opErr, convErr := operationError(FailureOf(err))
		if convErr != nil {
			return convErr
		}
		return opErr
	}
	if _, ok := errors.AsType[*serviceerror.ResourceExhausted](err); ok {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeResourceExhausted, "%s", err.Error())
	}
	return err
}

func newNexusService(h *handler) *nexus.Service {
	service := nexus.NewService(ServiceName)
	service.MustRegister(
		nexus.NewSyncOperation("CreateStream",
			func(ctx context.Context, req *streamservice.CreateStreamRequest, options nexus.StartOperationOptions) (*streamservice.CreateStreamResponse, error) {
				resp, err := h.CreateStream(ctx, req, callerOf(options))
				return resp, nexusError(err)
			}),
		nexus.NewSyncOperation("AppendRecords",
			func(ctx context.Context, req *streamservice.AppendRecordsRequest, options nexus.StartOperationOptions) (*streamservice.AppendRecordsResponse, error) {
				resp, err := h.AppendRecords(ctx, req, callerOf(options))
				return resp, nexusError(err)
			}),
		nexus.NewSyncOperation("CloseStream",
			func(ctx context.Context, req *streamservice.CloseStreamRequest, options nexus.StartOperationOptions) (*streamservice.CloseStreamResponse, error) {
				resp, err := h.CloseStream(ctx, req, callerOf(options))
				return resp, nexusError(err)
			}),
		nexus.NewSyncOperation("DescribeStream",
			func(ctx context.Context, req *streamservice.DescribeStreamRequest, _ nexus.StartOperationOptions) (*streamservice.DescribeStreamResponse, error) {
				resp, err := h.DescribeStream(ctx, req)
				return resp, nexusError(err)
			}),
		&readRecordsOperation{handler: h},
	)
	return service
}

// readRecordsOperation answers a read at once when the Stream has Records for it or ends. Any
// other read is an async operation that the Stream completes when Records come or the Stream
// closes, so a Workflow waits on the server with no timer.
type readRecordsOperation struct {
	nexus.UnimplementedOperation[*streamservice.ReadRecordsRequest, *streamservice.ReadRecordsResponse]

	handler *handler
}

func (o *readRecordsOperation) Name() string {
	return "ReadRecords"
}

func (o *readRecordsOperation) Start(
	ctx context.Context,
	req *streamservice.ReadRecordsRequest,
	options nexus.StartOperationOptions,
) (nexus.HandlerStartOperationResult[*streamservice.ReadRecordsResponse], error) {
	resp, err := o.handler.ReadOrPark(ctx, req, callerOf(options))
	if err != nil {
		return nil, nexusError(err)
	}
	if resp == nil {
		token, err := json.Marshal(readToken{
			Namespace: req.GetNamespace(),
			StreamID:  req.GetStream().GetId(),
			RequestID: options.RequestID,
		})
		if err != nil {
			return nil, err
		}
		return &nexus.HandlerStartOperationResultAsync{OperationToken: string(token)}, nil
	}
	return &nexus.HandlerStartOperationResultSync[*streamservice.ReadRecordsResponse]{Value: resp}, nil
}

func (o *readRecordsOperation) Cancel(ctx context.Context, token string, _ nexus.CancelOperationOptions) error {
	var t readToken
	if err := json.Unmarshal([]byte(token), &t); err != nil {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid operation token")
	}
	return nexusError(o.handler.CancelRead(ctx, t.Namespace, t.StreamID, t.RequestID))
}

// readToken names a parked read.
type readToken struct {
	Namespace string `json:"n"`
	StreamID  string `json:"s"`
	RequestID string `json:"r"`
}

// operationError gives the failed or canceled operation error of a failure.
func operationError(failure *failurepb.Failure) (*nexus.OperationError, error) {
	state := nexus.OperationStateFailed
	if failure.GetCanceledFailureInfo() != nil {
		state = nexus.OperationStateCanceled
	}
	nf, err := commonnexus.TemporalFailureToNexusFailure(failure)
	if err != nil {
		return nil, err
	}
	opErr := &nexus.OperationError{
		State:   state,
		Message: failure.GetMessage(),
		Cause:   &nexus.FailureError{Failure: nf},
	}
	if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
		return nil, err
	}
	return opErr, nil
}

// streamRequest is what the processor needs of each request.
type streamRequest interface {
	GetNamespace() string
}

// processor checks a request where the command is accepted and routes the operation to the
// shard of the Stream.
type processor[R streamRequest] struct {
	config       *Config
	streamID     func(R) string
	setNamespace func(R, string)
}

func (p processor[R]) ProcessInput(
	ctx chasm.NexusOperationProcessorContext,
	req R,
) (*chasm.NexusOperationProcessorResult, error) {
	name := ctx.Namespace.Name().String()
	if !p.config.Enabled(name) {
		return nil, serviceerror.NewUnimplemented("The Stream service is disabled")
	}
	id := p.streamID(req)
	if err := CheckStreamID(id); err != nil {
		return nil, err
	}
	switch req.GetNamespace() {
	case "":
		p.setNamespace(req, name)
	case name:
	default:
		return nil, serviceerror.NewInvalidArgumentf("Namespace in request %q does not match namespace in context %q", req.GetNamespace(), name)
	}
	return &chasm.NexusOperationProcessorResult{
		RoutingKey: chasm.NexusOperationRoutingKeyExecution{
			NamespaceID: ctx.Namespace.ID().String(),
			BusinessID:  id,
		},
	}, nil
}

func newNexusServiceProcessor(config *Config) *chasm.NexusServiceProcessor {
	sp := chasm.NewNexusServiceProcessor(ServiceName)
	sp.MustRegisterOperation("CreateStream", chasm.NewRegisterableNexusOperationProcessor(processor[*streamservice.CreateStreamRequest]{
		config:       config,
		streamID:     (*streamservice.CreateStreamRequest).GetStreamId,
		setNamespace: func(r *streamservice.CreateStreamRequest, ns string) { r.Namespace = ns },
	}))
	sp.MustRegisterOperation("AppendRecords", chasm.NewRegisterableNexusOperationProcessor(processor[*streamservice.AppendRecordsRequest]{
		config:       config,
		streamID:     func(r *streamservice.AppendRecordsRequest) string { return r.GetStream().GetId() },
		setNamespace: func(r *streamservice.AppendRecordsRequest, ns string) { r.Namespace = ns },
	}))
	sp.MustRegisterOperation("CloseStream", chasm.NewRegisterableNexusOperationProcessor(processor[*streamservice.CloseStreamRequest]{
		config:       config,
		streamID:     func(r *streamservice.CloseStreamRequest) string { return r.GetStream().GetId() },
		setNamespace: func(r *streamservice.CloseStreamRequest, ns string) { r.Namespace = ns },
	}))
	sp.MustRegisterOperation("ReadRecords", chasm.NewRegisterableNexusOperationProcessor(processor[*streamservice.ReadRecordsRequest]{
		config:       config,
		streamID:     func(r *streamservice.ReadRecordsRequest) string { return r.GetStream().GetId() },
		setNamespace: func(r *streamservice.ReadRecordsRequest, ns string) { r.Namespace = ns },
	}))
	sp.MustRegisterOperation("DescribeStream", chasm.NewRegisterableNexusOperationProcessor(processor[*streamservice.DescribeStreamRequest]{
		config:       config,
		streamID:     func(r *streamservice.DescribeStreamRequest) string { return r.GetStream().GetId() },
		setNamespace: func(r *streamservice.DescribeStreamRequest, ns string) { r.Namespace = ns },
	}))
	return sp
}
