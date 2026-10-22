package stream

import (
	"context"
	"errors"
	"fmt"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/streamservice/v1"
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/namespace"
)

// handler runs the five Stream operations on the history shard that owns the Stream. The
// System Nexus operations and the frontend service both call it.
type handler struct {
	config            *Config
	namespaceRegistry namespace.Registry
}

func newHandler(config *Config, namespaceRegistry namespace.Registry) *handler {
	return &handler{config: config, namespaceRegistry: namespaceRegistry}
}

// Caller is who calls an operation. A Workflow calls through System Nexus, so the server knows
// its run and sets the Producer. Any other caller names its own Producer.
type Caller struct {
	// Set for a Workflow caller.
	WorkflowID string
	RunID      string
	// The Nexus request id, which a retry of the operation start carries again.
	RequestID string
	// The completion callback of a Workflow read. A read with none answers at once.
	Callback *commonpb.Callback
}

func (c Caller) workflow() bool {
	return c.RunID != ""
}

// producer is the Producer of a write. A Workflow's Producer is its run, as the crate names
// it. Outside a Workflow the request names it.
func (c Caller) producer(requested *streamservice.Producer) (*streamservice.Producer, error) {
	if c.workflow() {
		return &streamservice.Producer{
			Kind:  streamservice.PRODUCER_KIND_WORKFLOW,
			Id:    "wf:" + encodePart(c.RunID),
			RunId: c.RunID,
		}, nil
	}
	if requested == nil {
		return nil, invalid("The request has no Producer")
	}
	if requested.GetKind() == streamservice.PRODUCER_KIND_WORKFLOW {
		return nil, invalid("A Workflow Producer writes through System Nexus only")
	}
	prefix, ok := producerPrefixes[requested.GetKind()]
	if !ok {
		return nil, invalid("The Producer has no kind")
	}
	if !strings.HasPrefix(requested.GetId(), prefix) {
		return nil, invalid("The Producer id %q doesn't start with %q, as its kind requires", requested.GetId(), prefix)
	}
	return requested, nil
}

var producerPrefixes = map[streamservice.ProducerKind]string{
	streamservice.PRODUCER_KIND_LOCAL_ACTIVITY:      "la:",
	streamservice.PRODUCER_KIND_ACTIVITY:            "act:",
	streamservice.PRODUCER_KIND_STANDALONE_ACTIVITY: "sa:",
	streamservice.PRODUCER_KIND_CLIENT:              "client:",
}

// encodePart percent-encodes every byte but the unreserved ones, as the crate builds ids.
func encodePart(text string) string {
	var out strings.Builder
	for _, b := range []byte(text) {
		if ('a' <= b && b <= 'z') || ('A' <= b && b <= 'Z') || ('0' <= b && b <= '9') ||
			b == '_' || b == '.' || b == '-' || b == '~' {
			out.WriteByte(b)
		} else {
			fmt.Fprintf(&out, "%%%02X", b)
		}
	}
	return out.String()
}

// CheckStreamID refuses an empty or too long Stream id.
func CheckStreamID(id string) error {
	if id == "" {
		return invalid("The Stream id is empty")
	}
	if len(id) > MaxStreamIDBytes {
		return invalid("The Stream id has %d bytes, more than %d", len(id), MaxStreamIDBytes)
	}
	return nil
}

func (h *handler) key(namespaceName, streamID string) (chasm.ExecutionKey, Limits, error) {
	if err := CheckStreamID(streamID); err != nil {
		return chasm.ExecutionKey{}, Limits{}, err
	}
	if !h.config.Enabled(namespaceName) {
		return chasm.ExecutionKey{}, Limits{}, serviceerror.NewUnimplemented("The Stream service is disabled")
	}
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(namespaceName))
	if err != nil {
		return chasm.ExecutionKey{}, Limits{}, err
	}
	return chasm.ExecutionKey{NamespaceID: namespaceID.String(), BusinessID: streamID}, h.config.LimitsFor(namespaceName), nil
}

var (
	useExisting = chasm.WithBusinessIDPolicy(
		chasm.BusinessIDReusePolicyRejectDuplicate, chasm.BusinessIDConflictPolicyUseExisting)
	replaceAbandoned = chasm.WithBusinessIDPolicy(
		chasm.BusinessIDReusePolicyAllowDuplicate, chasm.BusinessIDConflictPolicyUseExisting)
)

// phase is the phase of the current run of a Stream, or false when there is none.
func phase(ctx context.Context, key chasm.ExecutionKey) (streampb.StreamPhase, bool) {
	p, err := chasm.ReadComponent(ctx, chasm.NewComponentRef[*Stream](key),
		func(s *Stream, _ chasm.Context, _ struct{}) (streampb.StreamPhase, error) {
			return s.Phase, nil
		}, struct{}{})
	return p, err == nil
}

// streamError gives a Stream error for err. An error of the framework, such as a missing or a
// closed execution, becomes the error of the Stream's state.
func streamError(ctx context.Context, key chasm.ExecutionKey, err error) error {
	if err == nil || ReasonOf(err) != "" {
		return err
	}
	p, found := phase(ctx, key)
	if !found {
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			return notFound()
		}
		return err
	}
	switch p {
	case streampb.STREAM_PHASE_PENDING, streampb.STREAM_PHASE_ABANDONED:
		return notFound()
	case streampb.STREAM_PHASE_EXPIRED:
		return NewError(ReasonExpired, "The Stream expired")
	default:
		return err
	}
}

// CreateStream creates a Stream, or answers the state of the one that has the id.
func (h *handler) CreateStream(
	ctx context.Context,
	req *streamservice.CreateStreamRequest,
	caller Caller,
) (*streamservice.CreateStreamResponse, error) {
	key, limits, err := h.key(req.GetNamespace(), req.GetStreamId())
	if err != nil {
		return nil, err
	}
	in := CreateInput{Request: req, WorkflowOwned: caller.workflow(), Limits: limits}
	if caller.workflow() {
		in.Producer, _ = caller.producer(nil)
	}
	create := func(policy chasm.TransitionOption) (CreateOutput, error) {
		result, err := chasm.UpdateWithStartExecution(ctx, key, StartStream, (*Stream).Create, in, policy)
		out := result.UpdateOutput
		out.Created = out.Created || result.Created
		return out, err
	}
	out, err := create(useExisting)
	if err != nil && ReasonOf(err) == "" {
		switch p, _ := phase(ctx, key); p {
		case streampb.STREAM_PHASE_ABANDONED:
			out, err = create(replaceAbandoned)
		case streampb.STREAM_PHASE_EXPIRED:
			out, err = CreateOutput{Status: streamservice.STREAM_STATUS_EXPIRED}, nil
		default:
		}
	}
	if err != nil {
		return nil, err
	}
	return &streamservice.CreateStreamResponse{
		Stream:  &streamservice.StreamRef{Id: req.GetStreamId()},
		Created: out.Created,
		Status:  out.Status,
	}, nil
}

// AppendRecords appends one batch.
func (h *handler) AppendRecords(
	ctx context.Context,
	req *streamservice.AppendRecordsRequest,
	caller Caller,
) (*streamservice.AppendRecordsResponse, error) {
	key, limits, err := h.key(req.GetNamespace(), req.GetStream().GetId())
	if err != nil {
		return nil, err
	}
	producer, err := caller.producer(req.GetProducer())
	if err != nil {
		return nil, err
	}
	resp, _, err := chasm.UpdateComponent(ctx, chasm.NewComponentRef[*Stream](key), (*Stream).Append, AppendInput{
		Request:   req,
		Producer:  producer,
		RequestID: caller.RequestID,
		Limits:    limits,
	})
	return resp, streamError(ctx, key, err)
}

// CloseStream closes a Stream.
func (h *handler) CloseStream(
	ctx context.Context,
	req *streamservice.CloseStreamRequest,
	caller Caller,
) (*streamservice.CloseStreamResponse, error) {
	key, limits, err := h.key(req.GetNamespace(), req.GetStream().GetId())
	if err != nil {
		return nil, err
	}
	producer, err := caller.producer(req.GetProducer())
	if err != nil {
		return nil, err
	}
	resp, _, err := chasm.UpdateComponent(ctx, chasm.NewComponentRef[*Stream](key), (*Stream).Close, CloseInput{
		Request:  req,
		Producer: producer,
		Limits:   limits,
	})
	return resp, streamError(ctx, key, err)
}

// DescribeStream answers the state of a Stream.
func (h *handler) DescribeStream(
	ctx context.Context,
	req *streamservice.DescribeStreamRequest,
) (*streamservice.DescribeStreamResponse, error) {
	key, limits, err := h.key(req.GetNamespace(), req.GetStream().GetId())
	if err != nil {
		return nil, err
	}
	if err := h.applyDue(ctx, key, limits); err != nil {
		return nil, streamError(ctx, key, err)
	}
	resp, err := chasm.ReadComponent(ctx, chasm.NewComponentRef[*Stream](key),
		func(s *Stream, _ chasm.Context, _ struct{}) (*streamservice.DescribeStreamResponse, error) {
			return s.Describe()
		}, struct{}{})
	return resp, streamError(ctx, key, err)
}

// applyDue runs a write step when a deadline such as the auto close passed, so that a read
// never sees the Stream open after it.
func (h *handler) applyDue(ctx context.Context, key chasm.ExecutionKey, limits Limits) error {
	ref := chasm.NewComponentRef[*Stream](key)
	due, err := chasm.ReadComponent(ctx, ref, (*Stream).Due, limits)
	if err != nil || !due {
		return err
	}
	_, _, err = chasm.UpdateComponent(ctx, ref,
		func(s *Stream, ctx chasm.MutableContext, limits Limits) (struct{}, error) {
			s.ApplyDue(ctx, limits)
			return struct{}{}, nil
		}, limits)
	return err
}

// ReadOrPark answers a Workflow read at once when the Stream has Records for it or ends.
// Otherwise it parks the read and answers nil, and the Stream completes the read's Nexus
// operation later.
func (h *handler) ReadOrPark(
	ctx context.Context,
	req *streamservice.ReadRecordsRequest,
	caller Caller,
) (*streamservice.ReadRecordsResponse, error) {
	key, limits, err := h.key(req.GetNamespace(), req.GetStream().GetId())
	if err != nil {
		return nil, err
	}
	in := ReadInput{Request: req, RequestID: caller.RequestID, Callback: caller.Callback, Limits: limits}
	read := func(policy chasm.TransitionOption) (*streamservice.ReadRecordsResponse, error) {
		result, err := chasm.UpdateWithStartExecution(ctx, key, StartForRead, (*Stream).ReadOrPark, in, policy)
		return result.UpdateOutput, err
	}
	resp, err := read(useExisting)
	if err != nil && ReasonOf(err) == "" {
		if p, _ := phase(ctx, key); p == streampb.STREAM_PHASE_ABANDONED {
			resp, err = read(replaceAbandoned)
		}
	}
	if err != nil {
		return nil, streamError(ctx, key, err)
	}
	return resp, nil
}

// CancelRead completes a parked read as canceled.
func (h *handler) CancelRead(ctx context.Context, namespaceName, streamID, requestID string) error {
	key, limits, err := h.key(namespaceName, streamID)
	if err != nil {
		return err
	}
	_, _, err = chasm.UpdateComponent(ctx, chasm.NewComponentRef[*Stream](key),
		func(s *Stream, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			return struct{}{}, s.CancelRead(ctx, requestID, limits)
		}, struct{}{})
	return err
}
