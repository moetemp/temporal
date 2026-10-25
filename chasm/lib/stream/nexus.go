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

// readerOf gives the Nexus request id and the completion callback of a read.
func readerOf(options nexus.StartOperationOptions) Reader {
	reader := Reader{RequestID: options.RequestID, Run: readerRunOf(options.Links)}
	if options.CallbackURL != "" {
		reader.Callback = &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{
			Url:    options.CallbackURL,
			Header: options.CallbackHeader,
		}}}
	}
	return reader
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

// newNexusService serves the reads and the close to Workflow code. The close is a command, so the
// server runs it only when the Workflow Task that issued it commits. Workflow writes go over the
// frontend from the Core extension, which knows the Workflow Task facts of each write.
func newNexusService(h *handler) *nexus.Service {
	service := nexus.NewService(ServiceName)
	service.MustRegister(
		nexus.NewSyncOperation("DescribeStream",
			func(ctx context.Context, req *streamservice.DescribeStreamRequest, _ nexus.StartOperationOptions) (*streamservice.DescribeStreamResponse, error) {
				resp, err := h.DescribeStream(ctx, req)
				return resp, nexusError(err)
			}),
		nexus.NewSyncOperation("CloseStream",
			func(ctx context.Context, req *streamservice.CloseStreamRequest, options nexus.StartOperationOptions) (*streamservice.CloseStreamResponse, error) {
				// A command's start always has the system callback, and a direct call never has
				// one. A direct call waits for no Workflow Task to commit, so it can't close.
				if options.CallbackURL == "" {
					return nil, nexusError(invalid("A direct System Nexus call can't close a Stream, so use the gRPC CloseStream"))
				}
				resp, err := h.CloseFromRun(ctx, req, readerRunOf(options.Links))
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
	// A direct call has no callback, so it reads as the frontend does, a long poll up to wait.
	if options.CallbackURL == "" {
		resp, err := o.handler.ReadRecords(ctx, req, readerRunOf(options.Links))
		// The direct call answers a gRPC error, and the frontend masks a Nexus handler error as
		// Unknown. So the reader cap stays ResourceExhausted, which the caller backs off on.
		if _, ok := errors.AsType[*serviceerror.ResourceExhausted](err); ok {
			return nil, err
		}
		if err != nil {
			return nil, nexusError(err)
		}
		return &nexus.HandlerStartOperationResultSync[*streamservice.ReadRecordsResponse]{Value: resp}, nil
	}
	resp, err := o.handler.ReadOrPark(ctx, req, readerOf(options))
	if err != nil {
		return nil, nexusError(err)
	}
	if resp == nil {
		token, err := json.Marshal(readToken{
			Namespace:     req.GetNamespace(),
			StreamID:      req.GetStream().GetId(),
			WorkflowID:    req.GetStream().GetWorkflowId(),
			FirstRunID:    req.GetStream().GetFirstExecutionRunId(),
			ActivityID:    req.GetStream().GetActivityId(),
			ActivityRunID: req.GetStream().GetActivityRunId(),
			RequestID:     options.RequestID,
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
	ref := &streamservice.StreamRef{
		Id:                  t.StreamID,
		WorkflowId:          t.WorkflowID,
		FirstExecutionRunId: t.FirstRunID,
		ActivityId:          t.ActivityID,
		ActivityRunId:       t.ActivityRunID,
	}
	return nexusError(o.handler.CancelRead(ctx, t.Namespace, ref, t.RequestID))
}

// readToken names a parked read.
type readToken struct {
	Namespace     string `json:"n"`
	StreamID      string `json:"s"`
	WorkflowID    string `json:"w,omitempty"`
	FirstRunID    string `json:"f,omitempty"`
	ActivityID    string `json:"a,omitempty"`
	ActivityRunID string `json:"ar,omitempty"`
	RequestID     string `json:"r"`
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
	GetStream() *streamservice.StreamRef
}

// processor checks a request where the command is accepted and routes the operation to the
// shard of the Stream. A Workflow Stream is on its Workflow's shard.
type processor[R streamRequest] struct {
	config       *Config
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
	ref := req.GetStream()
	if err := CheckStreamID(ref.GetId()); err != nil {
		return nil, err
	}
	switch req.GetNamespace() {
	case "":
		p.setNamespace(req, name)
	case name:
	default:
		return nil, serviceerror.NewInvalidArgumentf("Namespace in request %q does not match namespace in context %q", req.GetNamespace(), name)
	}
	businessID := ref.GetWorkflowId()
	if businessID == "" {
		businessID = ref.GetActivityId()
	}
	if businessID == "" {
		businessID = ref.GetId()
	}
	return &chasm.NexusOperationProcessorResult{
		RoutingKey: chasm.NexusOperationRoutingKeyExecution{
			NamespaceID: ctx.Namespace.ID().String(),
			BusinessID:  businessID,
		},
	}, nil
}

func newNexusServiceProcessor(config *Config) *chasm.NexusServiceProcessor {
	sp := chasm.NewNexusServiceProcessor(ServiceName)
	sp.MustRegisterOperation("ReadRecords", chasm.NewRegisterableNexusOperationProcessor(processor[*streamservice.ReadRecordsRequest]{
		config:       config,
		setNamespace: func(r *streamservice.ReadRecordsRequest, ns string) { r.Namespace = ns },
	}))
	sp.MustRegisterOperation("DescribeStream", chasm.NewRegisterableNexusOperationProcessor(processor[*streamservice.DescribeStreamRequest]{
		config:       config,
		setNamespace: func(r *streamservice.DescribeStreamRequest, ns string) { r.Namespace = ns },
	}))
	sp.MustRegisterOperation("CloseStream", chasm.NewRegisterableNexusOperationProcessor(processor[*streamservice.CloseStreamRequest]{
		config:       config,
		setNamespace: func(r *streamservice.CloseStreamRequest, ns string) { r.Namespace = ns },
	}))
	return sp
}
