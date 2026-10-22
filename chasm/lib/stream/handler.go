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

// target resolves the namespace, the limits and the place of a Stream.
func (h *handler) target(namespaceName, streamID string, ref *streamservice.StreamRef) (target, Limits, error) {
	if err := CheckStreamID(streamID); err != nil {
		return target{}, Limits{}, err
	}
	if !h.config.Enabled(namespaceName) {
		return target{}, Limits{}, serviceerror.NewUnimplemented("The Stream service is disabled")
	}
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(namespaceName))
	if err != nil {
		return target{}, Limits{}, err
	}
	t, err := targetOf(namespaceID.String(), streamID, ref)
	limits := h.config.LimitsFor(namespaceName)
	if t.nested {
		limits.MaxWorkflowStateBytes = int64(h.config.MaxWorkflowStateBytes(namespaceName))
	}
	return t, limits, err
}

var (
	useExisting = chasm.WithBusinessIDPolicy(
		chasm.BusinessIDReusePolicyRejectDuplicate, chasm.BusinessIDConflictPolicyUseExisting)
	replaceAbandoned = chasm.WithBusinessIDPolicy(
		chasm.BusinessIDReusePolicyAllowDuplicate, chasm.BusinessIDConflictPolicyUseExisting)
)

// phase is the phase of a Stream, or false when there is none.
func phase(ctx context.Context, t target) (streampb.StreamPhase, bool) {
	p, err := read(ctx, t, func(s *Stream, _ chasm.Context) (streampb.StreamPhase, error) {
		return s.Phase, nil
	})
	return p, err == nil
}

// streamError gives a Stream error for err. An error of the framework, such as a missing or a
// closed execution, becomes the error of the Stream's state.
func streamError(ctx context.Context, t target, err error) error {
	if err == nil || ReasonOf(err) != "" {
		return err
	}
	p, found := phase(ctx, t)
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

// CreateStream creates a Stream, or answers the state of the one that has the id. A Stream
// whose ref names a Workflow nests in that Workflow's chain.
func (h *handler) CreateStream(
	ctx context.Context,
	req *streamservice.CreateStreamRequest,
) (*streamservice.CreateStreamResponse, error) {
	t, limits, err := h.target(req.GetNamespace(), req.GetStreamId(), req.GetRef())
	if err != nil {
		return nil, err
	}
	if owner := req.GetOwner(); owner != nil {
		if err := checkProducer(owner); err != nil {
			return nil, err
		}
	}
	if req.GetCloseWithRun() && (!t.nested || req.GetOwner().GetKind() != streamservice.PRODUCER_KIND_WORKFLOW) {
		return nil, invalid("close_with_run needs a Workflow owner and a ref with workflow_id and first_execution_run_id")
	}
	in := CreateInput{Request: req, Limits: limits}
	var out CreateOutput
	if t.nested {
		out, err = update(ctx, t, newNestedStream,
			func(s *Stream, mctx chasm.MutableContext) (CreateOutput, error) { return s.Create(mctx, in) })
	} else {
		out, err = h.createStandalone(ctx, t, in)
	}
	if err != nil {
		return nil, err
	}
	return &streamservice.CreateStreamResponse{
		Stream:  &streamservice.StreamRef{Id: req.GetStreamId(), WorkflowId: req.GetRef().GetWorkflowId(), FirstExecutionRunId: req.GetRef().GetFirstExecutionRunId()},
		Created: out.Created,
		Status:  out.Status,
	}, nil
}

func (h *handler) createStandalone(ctx context.Context, t target, in CreateInput) (CreateOutput, error) {
	create := func(policy chasm.TransitionOption) (CreateOutput, error) {
		result, err := chasm.UpdateWithStartExecution(ctx, t.key, StartStream,
			func(s *Stream, mctx chasm.MutableContext, in CreateInput) (CreateOutput, error) {
				out, err := s.Create(mctx, in)
				return out, carry(err)
			}, in, policy)
		out := result.UpdateOutput
		out.Created = out.Created || result.Created
		return out, uncarry(err)
	}
	out, err := create(useExisting)
	if err != nil && ReasonOf(err) == "" {
		switch p, _ := phase(ctx, t); p {
		case streampb.STREAM_PHASE_ABANDONED:
			return create(replaceAbandoned)
		case streampb.STREAM_PHASE_EXPIRED:
			return CreateOutput{Status: streamservice.STREAM_STATUS_EXPIRED}, nil
		default:
		}
	}
	return out, err
}

// AppendRecords appends one batch.
func (h *handler) AppendRecords(
	ctx context.Context,
	req *streamservice.AppendRecordsRequest,
) (*streamservice.AppendRecordsResponse, error) {
	t, limits, err := h.target(req.GetNamespace(), req.GetStream().GetId(), req.GetStream())
	if err != nil {
		return nil, err
	}
	if err := checkProducer(req.GetProducer()); err != nil {
		return nil, err
	}
	resp, err := update(ctx, t, nil, func(s *Stream, mctx chasm.MutableContext) (*streamservice.AppendRecordsResponse, error) {
		return s.Append(mctx, AppendInput{Request: req, Limits: limits})
	})
	return resp, streamError(ctx, t, err)
}

// CloseStream closes a Stream.
func (h *handler) CloseStream(
	ctx context.Context,
	req *streamservice.CloseStreamRequest,
) (*streamservice.CloseStreamResponse, error) {
	t, limits, err := h.target(req.GetNamespace(), req.GetStream().GetId(), req.GetStream())
	if err != nil {
		return nil, err
	}
	if err := checkProducer(req.GetProducer()); err != nil {
		return nil, err
	}
	resp, err := update(ctx, t, nil, func(s *Stream, mctx chasm.MutableContext) (*streamservice.CloseStreamResponse, error) {
		return s.Close(mctx, CloseInput{Request: req, Limits: limits})
	})
	return resp, streamError(ctx, t, err)
}

// DescribeStream answers the state of a Stream.
func (h *handler) DescribeStream(
	ctx context.Context,
	req *streamservice.DescribeStreamRequest,
) (*streamservice.DescribeStreamResponse, error) {
	t, limits, err := h.target(req.GetNamespace(), req.GetStream().GetId(), req.GetStream())
	if err != nil {
		return nil, err
	}
	if err := h.applyDue(ctx, t, limits); err != nil {
		return nil, streamError(ctx, t, err)
	}
	resp, err := read(ctx, t, func(s *Stream, _ chasm.Context) (*streamservice.DescribeStreamResponse, error) {
		return s.Describe()
	})
	return resp, streamError(ctx, t, err)
}

// applyDue runs a write step when a deadline such as the auto close passed, so that a read
// never sees the Stream open after it.
func (h *handler) applyDue(ctx context.Context, t target, limits Limits) error {
	due, err := read(ctx, t, func(s *Stream, cctx chasm.Context) (bool, error) { return s.Due(cctx, limits) })
	if err != nil || !due {
		return err
	}
	_, err = update(ctx, t, nil, func(s *Stream, mctx chasm.MutableContext) (struct{}, error) {
		s.ApplyDue(mctx, limits)
		return struct{}{}, nil
	})
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
	t, limits, err := h.target(req.GetNamespace(), req.GetStream().GetId(), req.GetStream())
	if err != nil {
		return nil, err
	}
	in := ReadInput{Request: req, RequestID: reader.RequestID, Callback: reader.Callback, Limits: limits}
	if t.nested {
		resp, err := update(ctx, t, newNestedStream,
			func(s *Stream, mctx chasm.MutableContext) (*streamservice.ReadRecordsResponse, error) {
				return s.ReadOrPark(mctx, in)
			})
		return resp, streamError(ctx, t, err)
	}
	readStandalone := func(policy chasm.TransitionOption) (*streamservice.ReadRecordsResponse, error) {
		result, err := chasm.UpdateWithStartExecution(ctx, t.key, StartForRead,
			func(s *Stream, mctx chasm.MutableContext, in ReadInput) (*streamservice.ReadRecordsResponse, error) {
				resp, err := s.ReadOrPark(mctx, in)
				return resp, carry(err)
			}, in, policy)
		return result.UpdateOutput, uncarry(err)
	}
	resp, err := readStandalone(useExisting)
	// An abandoned Stream answers stream_not_found, and the read starts a new execution for the id.
	if reason := ReasonOf(err); err != nil && (reason == "" || reason == ReasonNotFound) {
		if p, _ := phase(ctx, t); p == streampb.STREAM_PHASE_ABANDONED {
			resp, err = readStandalone(replaceAbandoned)
		}
	}
	if err != nil {
		return nil, streamError(ctx, t, err)
	}
	return resp, nil
}

// CancelRead completes a parked read as canceled.
func (h *handler) CancelRead(ctx context.Context, namespaceName string, ref *streamservice.StreamRef, requestID string) error {
	t, limits, err := h.target(namespaceName, ref.GetId(), ref)
	if err != nil {
		return err
	}
	_, err = update(ctx, t, nil, func(s *Stream, mctx chasm.MutableContext) (struct{}, error) {
		return struct{}{}, s.CancelRead(mctx, requestID, limits)
	})
	return err
}

// pollTick is how often a read looks again for a Stream that nobody created yet.
const pollTick = 100 * time.Millisecond

// longPollBuffer is the time a read leaves before the caller's deadline, so the empty answer
// gets back in time.
const longPollBuffer = time.Second

// ReadRecords is a frontend read. It waits up to create_wait for the create and up to wait for
// a Record, and answers an empty page after the wait. A page of an open Workflow run, run, keeps
// the run's floor on a Workflow Stream first, so no trim drops what it reads.
func (h *handler) ReadRecords(
	ctx context.Context,
	req *streamservice.ReadRecordsRequest,
	run *ReaderRun,
) (*streamservice.ReadRecordsResponse, error) {
	t, limits, err := h.target(req.GetNamespace(), req.GetStream().GetId(), req.GetStream())
	if err != nil {
		return nil, err
	}
	wait, createWait := ReadTimeout(req)
	// nolint:forbidigo // A long poll measures wall-clock time, not component time.
	started := time.Now()
	deadline := started.Add(wait)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Add(-longPollBuffer).Before(deadline) {
		deadline = ctxDeadline.Add(-longPollBuffer)
	}

	if err := h.waitForCreate(ctx, t, started.Add(createWait)); err != nil {
		return nil, err
	}
	if err := h.applyDue(ctx, t, limits); err != nil {
		return nil, streamError(ctx, t, err)
	}
	if err := h.keepFloor(ctx, t, limits, req, run); err != nil {
		return nil, streamError(ctx, t, err)
	}
	start, err := read(ctx, t, func(s *Stream, _ chasm.Context) (int64, error) { return s.StartOffset(req) })
	if err != nil {
		return nil, streamError(ctx, t, err)
	}
	cursor := start
	for {
		from := cursor
		page, err := read(ctx, t, func(s *Stream, cctx chasm.Context) (pagedRead, error) {
			resp, found, err := s.Page(cctx, req, from)
			if found && req.GetWakeOnly() {
				resp = s.wakeAnswer(start)
			}
			return pagedRead{resp: resp, found: found}, err
		})
		if err != nil {
			return nil, streamError(ctx, t, err)
		}
		// nolint:forbidigo // A long poll measures wall-clock time, not component time.
		if page.found || !time.Now().Before(deadline) {
			if !page.found && req.GetWakeOnly() {
				return &streamservice.ReadRecordsResponse{NextOffset: start}, nil
			}
			return page.resp, nil
		}
		cursor = max(cursor, page.resp.GetNextOffset())
		if err := h.waitForChange(ctx, t, cursor, deadline); err != nil {
			return nil, streamError(ctx, t, err)
		}
	}
}

// keepFloor writes the run's floor only when the read lowers it, which is mostly its first page.
// A wake reads no Records, and only a Workflow Stream keeps floors.
func (h *handler) keepFloor(ctx context.Context, t target, limits Limits, req *streamservice.ReadRecordsRequest, run *ReaderRun) error {
	if run == nil || !t.nested || req.GetWakeOnly() {
		return nil
	}
	lowers, err := read(ctx, t, func(s *Stream, _ chasm.Context) (bool, error) {
		_, ok := s.readerFloor(run.RunID, req)
		return ok, nil
	})
	if err != nil || !lowers {
		return err
	}
	_, err = update(ctx, t, nil, func(s *Stream, mctx chasm.MutableContext) (struct{}, error) {
		return struct{}{}, s.AddReader(mctx, *run, req, limits)
	})
	return err
}

type pagedRead struct {
	resp  *streamservice.ReadRecordsResponse
	found bool
}

// waitForCreate waits until the Stream exists, polling, since nothing notifies the create of
// an execution that does not exist yet.
func (h *handler) waitForCreate(ctx context.Context, t target, until time.Time) error {
	for {
		p, found := phase(ctx, t)
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
func (h *handler) waitForChange(ctx context.Context, t target, cursor int64, deadline time.Time) error {
	pollCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	cursor = max(cursor, 1)
	err := poll(pollCtx, t, func(s *Stream) bool {
		return s.NewestOffset >= cursor || s.Phase != streampb.STREAM_PHASE_OPEN
	})
	if err != nil && pollCtx.Err() != nil && ctx.Err() == nil {
		return nil
	}
	return err
}
