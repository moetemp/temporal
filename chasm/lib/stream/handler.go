package stream

import (
	"context"
	"errors"
	"strings"
	"time"

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

// Reader is a Workflow read through System Nexus.
type Reader struct {
	// The Nexus request id, which a retry of the operation start carries again.
	RequestID string
	// The completion callback of the read's operation. A read with none answers at once.
	Callback *commonpb.Callback
}

// checkProducer refuses a write with no Producer, or with an id that lacks the prefix of its
// kind, so one kind can't take another's id.
func checkProducer(producer *streamservice.Producer) error {
	if producer == nil {
		return invalid("The request has no Producer")
	}
	prefix, ok := producerPrefixes[producer.GetKind()]
	if !ok {
		return invalid("The Producer has no kind")
	}
	if !strings.HasPrefix(producer.GetId(), prefix) {
		return invalid("The Producer id %q doesn't start with %q, as its kind requires", producer.GetId(), prefix)
	}
	return nil
}

var producerPrefixes = map[streamservice.ProducerKind]string{
	streamservice.PRODUCER_KIND_WORKFLOW:            "wf:",
	streamservice.PRODUCER_KIND_LOCAL_ACTIVITY:      "la:",
	streamservice.PRODUCER_KIND_ACTIVITY:            "act:",
	streamservice.PRODUCER_KIND_STANDALONE_ACTIVITY: "sa:",
	streamservice.PRODUCER_KIND_CLIENT:              "client:",
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
) (*streamservice.CreateStreamResponse, error) {
	key, limits, err := h.key(req.GetNamespace(), req.GetStreamId())
	if err != nil {
		return nil, err
	}
	if owner := req.GetOwner(); owner != nil {
		if err := checkProducer(owner); err != nil {
			return nil, err
		}
	}
	if req.GetCloseWithRun() && (req.GetOwner().GetKind() != streamservice.PRODUCER_KIND_WORKFLOW ||
		req.GetRef().GetWorkflowId() == "" || req.GetRef().GetFirstExecutionRunId() == "") {
		return nil, invalid("close_with_run needs a Workflow owner and a ref with workflow_id and first_execution_run_id")
	}
	in := CreateInput{Request: req, Limits: limits, OwnerCheckInterval: h.config.OwnerCheckInterval(req.GetNamespace())}
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
) (*streamservice.AppendRecordsResponse, error) {
	key, limits, err := h.key(req.GetNamespace(), req.GetStream().GetId())
	if err != nil {
		return nil, err
	}
	if err := checkProducer(req.GetProducer()); err != nil {
		return nil, err
	}
	resp, _, err := chasm.UpdateComponent(ctx, chasm.NewComponentRef[*Stream](key), (*Stream).Append,
		AppendInput{Request: req, Limits: limits})
	return resp, streamError(ctx, key, err)
}

// CloseStream closes a Stream.
func (h *handler) CloseStream(
	ctx context.Context,
	req *streamservice.CloseStreamRequest,
) (*streamservice.CloseStreamResponse, error) {
	key, limits, err := h.key(req.GetNamespace(), req.GetStream().GetId())
	if err != nil {
		return nil, err
	}
	if err := checkProducer(req.GetProducer()); err != nil {
		return nil, err
	}
	resp, _, err := chasm.UpdateComponent(ctx, chasm.NewComponentRef[*Stream](key), (*Stream).Close,
		CloseInput{Request: req, Limits: limits})
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
	reader Reader,
) (*streamservice.ReadRecordsResponse, error) {
	key, limits, err := h.key(req.GetNamespace(), req.GetStream().GetId())
	if err != nil {
		return nil, err
	}
	in := ReadInput{Request: req, RequestID: reader.RequestID, Callback: reader.Callback, Limits: limits}
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

// pollTick is how often a read looks again for a Stream that nobody created yet.
const pollTick = 100 * time.Millisecond

// longPollBuffer is the time a read leaves before the caller's deadline, so the empty answer
// gets back in time.
const longPollBuffer = time.Second

// ReadRecords is a frontend read. It waits up to create_wait for the create and up to wait for
// a Record, and answers an empty page after the wait.
func (h *handler) ReadRecords(
	ctx context.Context,
	req *streamservice.ReadRecordsRequest,
) (*streamservice.ReadRecordsResponse, error) {
	key, limits, err := h.key(req.GetNamespace(), req.GetStream().GetId())
	if err != nil {
		return nil, err
	}
	ref := chasm.NewComponentRef[*Stream](key)
	wait, createWait := ReadTimeout(req)
	// nolint:forbidigo // A long poll measures wall-clock time, not component time.
	started := time.Now()
	deadline := started.Add(wait)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Add(-longPollBuffer).Before(deadline) {
		deadline = ctxDeadline.Add(-longPollBuffer)
	}

	if err := h.waitForCreate(ctx, key, started.Add(createWait)); err != nil {
		return nil, err
	}
	if err := h.applyDue(ctx, key, limits); err != nil {
		return nil, streamError(ctx, key, err)
	}
	cursor, err := chasm.ReadComponent(ctx, ref,
		func(s *Stream, _ chasm.Context, req *streamservice.ReadRecordsRequest) (int64, error) {
			return s.StartOffset(req)
		}, req)
	if err != nil {
		return nil, streamError(ctx, key, err)
	}
	for {
		page, err := chasm.ReadComponent(ctx, ref,
			func(s *Stream, ctx chasm.Context, from int64) (pagedRead, error) {
				resp, found, err := s.Page(ctx, req, from)
				return pagedRead{resp: resp, found: found}, err
			}, cursor)
		if err != nil {
			return nil, streamError(ctx, key, err)
		}
		// nolint:forbidigo // A long poll measures wall-clock time, not component time.
		if page.found || !time.Now().Before(deadline) {
			return page.resp, nil
		}
		cursor = max(cursor, page.resp.GetNextOffset())
		if err := h.waitForChange(ctx, ref, cursor, deadline); err != nil {
			return nil, streamError(ctx, key, err)
		}
	}
}

type pagedRead struct {
	resp  *streamservice.ReadRecordsResponse
	found bool
}

// waitForCreate waits until the Stream exists, polling, since nothing notifies the create of
// an execution that does not exist yet.
func (h *handler) waitForCreate(ctx context.Context, key chasm.ExecutionKey, until time.Time) error {
	for {
		p, found := phase(ctx, key)
		if found && p != streampb.STREAM_PHASE_PENDING && p != streampb.STREAM_PHASE_ABANDONED {
			return nil
		}
		// nolint:forbidigo // A long poll measures wall-clock time, not component time.
		left := time.Until(until)
		if left <= 0 {
			return notFound()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(min(left, pollTick)):
		}
	}
}

// waitForChange waits until the Stream has an entry at or past cursor, or is no longer open, or
// the deadline passes. The auto close and the expiry are timer tasks, so they end the wait too.
func (h *handler) waitForChange(ctx context.Context, ref chasm.ComponentRef, cursor int64, deadline time.Time) error {
	pollCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	_, _, err := chasm.PollComponent(pollCtx, ref,
		func(s *Stream, _ chasm.Context, cursor int64) (struct{}, bool, error) {
			return struct{}{}, s.NewestOffset >= cursor || s.Phase != streampb.STREAM_PHASE_OPEN, nil
		}, max(cursor, 1))
	if err != nil && pollCtx.Err() != nil && ctx.Err() == nil {
		return nil
	}
	return err
}
