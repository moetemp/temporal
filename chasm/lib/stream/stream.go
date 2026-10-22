package stream

import (
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/streamservice/v1"
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// MaxWait is the longest wait of one read, for new Records and for the create.
	MaxWait = 60 * time.Second
	// MaxRecords is the most Records one read gives.
	MaxRecords = 1000
	// MaxStreamIDBytes is the longest Stream id.
	MaxStreamIDBytes = 4096
	// The most entries one page scans, since a Topic filter can skip many.
	maxScan = 10 * MaxRecords
	// An offset is the append time in ms shifted by these bits, plus a sequence, as the Redis
	// Store packs its entry ids. So the offsets of the two backends sort the same way, and the
	// Retention of a Record shows in its offset.
	seqBits = 20
)

// Stream is one Stream of a namespace, keyed by the Stream id. The state holds the offsets, the
// batch index and the Producers. The Records are in the batch data nodes.
type Stream struct {
	chasm.UnimplementedComponent

	*streampb.StreamState

	// Batches are keyed by the offset of their first entry.
	Batches chasm.Map[int64, *streampb.RecordBatch]

	// Reads are the Workflow reads that wait for Records, keyed by their Nexus request id.
	Reads chasm.Map[string, *ParkedRead]
}

var _ chasm.RootComponent = (*Stream)(nil)

// NewStream builds a Stream in the given phase. An open Stream gets its settings from create.
func NewStream(ctx chasm.MutableContext, phase streampb.StreamPhase) *Stream {
	s := &Stream{
		StreamState: &streampb.StreamState{
			Phase:     phase,
			Producers: make(map[string]*streampb.ProducerState),
		},
		Batches: make(chasm.Map[int64, *streampb.RecordBatch]),
		Reads:   make(chasm.Map[string, *ParkedRead]),
	}
	now := timestamppb.New(ctx.Now(s))
	s.CreateTime = now
	s.LastWriteTime = now
	return s
}

// LifecycleState keeps a closed Stream running, so it stays readable for its Retention. Only an
// expired or abandoned Stream is done, and the framework then deletes it after the namespace
// retention.
func (s *Stream) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch s.Phase {
	case streampb.STREAM_PHASE_EXPIRED, streampb.STREAM_PHASE_ABANDONED:
		return chasm.LifecycleStateCompleted
	default:
		return chasm.LifecycleStateRunning
	}
}

func (s *Stream) ContextMetadata(_ chasm.Context) map[string]string {
	return nil
}

// Terminate drops the Records. The framework terminates an execution that grew too large, and
// a Stream that keeps them would grow again on the next append.
func (s *Stream) Terminate(
	ctx chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	s.expire(ctx)
	return chasm.TerminateComponentResponse{}, nil
}

// Status is the public status of the Stream.
func (s *Stream) Status() streamservice.StreamStatus {
	switch s.Phase {
	case streampb.STREAM_PHASE_OPEN:
		return streamservice.STREAM_STATUS_OPEN
	case streampb.STREAM_PHASE_CLOSED:
		return streamservice.STREAM_STATUS_CLOSED
	case streampb.STREAM_PHASE_EXPIRED:
		return streamservice.STREAM_STATUS_EXPIRED
	default:
		return streamservice.STREAM_STATUS_UNSPECIFIED
	}
}

// Exists is false for a Stream that only a read waits for, or that no create ever came for.
func (s *Stream) Exists() bool {
	return s.Phase != streampb.STREAM_PHASE_PENDING && s.Phase != streampb.STREAM_PHASE_ABANDONED
}

// CreateInput is a create of a Stream.
type CreateInput struct {
	Request *streamservice.CreateStreamRequest
	Limits  Limits
}

// CreateOutput is the answer of a create.
type CreateOutput struct {
	Created bool
	Status  streamservice.StreamStatus
}

// StartStream is the start of an execution for a create. The start opens the Stream itself,
// since a child node that the update of a new execution adds is not persisted.
func StartStream(ctx chasm.MutableContext, in CreateInput) (*Stream, error) {
	s := NewStream(ctx, streampb.STREAM_PHASE_PENDING)
	if _, err := s.Create(ctx, in); err != nil {
		return nil, err
	}
	return s, nil
}

// Create opens the Stream, idempotently for one Retention and one auto close.
func (s *Stream) Create(ctx chasm.MutableContext, in CreateInput) (CreateOutput, error) {
	s.applyDue(ctx, in.Limits)
	retention := in.Request.GetRetention().AsDuration()
	if retention <= 0 {
		retention = in.Limits.DefaultRetention
	}
	autoClose := in.Request.GetAutoClose().AsDuration()
	switch s.Phase {
	case streampb.STREAM_PHASE_PENDING, streampb.STREAM_PHASE_UNSPECIFIED:
		now := timestamppb.New(ctx.Now(s))
		s.Phase = streampb.STREAM_PHASE_OPEN
		s.Retention = durationpb.New(retention)
		s.AutoClose = nil
		if autoClose > 0 {
			s.AutoClose = durationpb.New(autoClose)
		}
		s.WorkflowOwned = in.Request.GetOwner().GetKind() == streamservice.PRODUCER_KIND_WORKFLOW
		s.CreateTime = now
		s.LastWriteTime = now
		s.Owner = in.Request.GetOwner()
		s.CloseWithRun = in.Request.GetCloseWithRun()
		s.withdrawOnCreate(ctx, in.Request)
		s.finish(ctx, in.Limits)
		return CreateOutput{Created: true, Status: s.Status()}, nil
	case streampb.STREAM_PHASE_OPEN:
		if s.GetRetention().AsDuration() != retention || s.GetAutoClose().AsDuration() != autoClose {
			return CreateOutput{}, NewError(ReasonAlreadyExists,
				"The Stream exists with the Retention %v and the auto close %v",
				s.GetRetention().AsDuration(), s.GetAutoClose().AsDuration())
		}
		if in.Request.GetOwner().GetKind() == streamservice.PRODUCER_KIND_WORKFLOW {
			s.WorkflowOwned = true
		}
		s.withdrawOnCreate(ctx, in.Request)
		s.finish(ctx, in.Limits)
		return CreateOutput{Status: s.Status()}, nil
	case streampb.STREAM_PHASE_ABANDONED:
		// The caller starts a new execution for the id.
		return CreateOutput{}, serviceerror.NewNotFound("abandoned stream")
	default:
		s.finish(ctx, in.Limits)
		return CreateOutput{Status: s.Status()}, nil
	}
}

// AppendInput is a batch of one Producer.
type AppendInput struct {
	Request *streamservice.AppendRecordsRequest
	Limits  Limits
}

// Append writes one batch of one Producer, with the checks and the withdrawal of the crate's
// append script in one transition. The same epoch and sequence answers the first offsets
// again, so a retried batch goes in once.
func (s *Stream) Append(ctx chasm.MutableContext, in AppendInput) (*streamservice.AppendRecordsResponse, error) {
	req := in.Request
	producer := req.GetProducer()
	if req.GetSequence() < 1 {
		return nil, invalid("The sequence must be 1 or more")
	}
	s.applyDue(ctx, in.Limits)
	if !s.Exists() {
		return nil, notFound()
	}
	if s.Phase == streampb.STREAM_PHASE_EXPIRED {
		return nil, NewError(ReasonExpired, "The Stream expired")
	}
	key := producerKey(producer)
	if known := s.Producers[key]; known != nil && CompareEpochs(producer.GetEpoch(), known.GetEpoch()) == 0 {
		if req.GetSequence() == known.GetSequence() {
			return &streamservice.AppendRecordsResponse{FirstOffset: known.GetFirstOffset(), LastOffset: known.GetLastOffset()}, nil
		}
		if req.GetSequence() < known.GetSequence() {
			return nil, NewError(ReasonSequence,
				"The sequence %d is below the last one, %d", req.GetSequence(), known.GetSequence())
		}
	}
	// A Workflow batch with no Records only settles its task facts, which a closed Stream
	// still takes, as the Superseded Records after the close.
	deferred := producer.GetKind() == streamservice.PRODUCER_KIND_WORKFLOW && req.GetDeferWithdrawal()
	settles := producer.GetKind() == streamservice.PRODUCER_KIND_WORKFLOW && len(req.GetRecords()) == 0 && !deferred
	if s.Phase == streampb.STREAM_PHASE_CLOSED && !settles {
		return nil, NewError(ReasonClosed, "The Stream is closed")
	}
	w := Withdrawal{
		PreviousCompleted: req.GetPreviousCompletedTask(),
		FailedTasks:       req.GetFailedTasks(),
		WithdrawRun:       req.GetWithdrawRun(),
		Defer:             deferred,
	}
	flagged, err := s.admit(producer, w)
	if err != nil {
		return nil, err
	}

	var entries []*streamservice.StreamRecord
	if flagged {
		entries = append(entries, supersededRecord(producer, w))
	}
	for _, record := range req.GetRecords() {
		entries = append(entries, &streamservice.StreamRecord{
			Kind:     streamservice.STREAM_RECORD_KIND_DATA,
			Topic:    record.GetTopic(),
			Producer: producer,
			Body:     record.GetBody(),
		})
	}
	if size := batchSize(entries); size > in.Limits.MaxBatchBytes {
		return nil, NewError(ReasonTooLarge, "The batch has %d bytes, more than %d", size, in.Limits.MaxBatchBytes)
	}

	first, last := s.write(ctx, entries)
	wrote := len(req.GetRecords()) > 0
	s.settle(producer, w, wrote)
	if s.Producers == nil {
		s.Producers = make(map[string]*streampb.ProducerState)
	}
	state := &streampb.ProducerState{
		Epoch:       producer.GetEpoch(),
		Sequence:    req.GetSequence(),
		FirstOffset: first,
		LastOffset:  last,
		Holds:       s.Producers[key].GetHolds(),
	}
	if producer.GetKind() != streamservice.PRODUCER_KIND_WORKFLOW {
		if wrote {
			state.Holds = producer.GetEpoch()
		} else if flagged {
			state.Holds = nil
		}
	}
	s.Producers[key] = state
	if wrote {
		s.LastWriteTime = timestamppb.New(ctx.Now(s))
	}
	s.limitProducers(in.Limits.MaxProducers)
	s.finish(ctx, in.Limits)
	return &streamservice.AppendRecordsResponse{FirstOffset: first, LastOffset: last}, nil
}

// CloseInput is a close of the Stream.
type CloseInput struct {
	Request *streamservice.CloseStreamRequest
	Limits  Limits
}

// Close writes the close Record, with the Producer checks and the withdrawal of a batch. A
// second close does nothing.
func (s *Stream) Close(ctx chasm.MutableContext, in CloseInput) (*streamservice.CloseStreamResponse, error) {
	req := in.Request
	producer := req.GetProducer()
	s.applyDue(ctx, in.Limits)
	if s.Phase == streampb.STREAM_PHASE_CLOSED {
		return &streamservice.CloseStreamResponse{}, nil
	}
	if err := s.writable(); err != nil {
		return nil, err
	}
	w := Withdrawal{PreviousCompleted: req.GetPreviousCompletedTask(), FailedTasks: req.GetFailedTasks(), WithdrawRun: req.GetWithdrawRun()}
	flagged, err := s.admit(producer, w)
	if err != nil {
		return nil, err
	}
	if flagged {
		s.write(ctx, []*streamservice.StreamRecord{supersededRecord(producer, w)})
	}
	s.settle(producer, w, false)
	s.close(ctx, producer, req.GetResult(), req.GetFailure())
	s.finish(ctx, in.Limits)
	return &streamservice.CloseStreamResponse{}, nil
}

// Describe answers the state of the Stream.
func (s *Stream) Describe() (*streamservice.DescribeStreamResponse, error) {
	if !s.Exists() {
		return nil, notFound()
	}
	return &streamservice.DescribeStreamResponse{
		Status:     s.Status(),
		LastOffset: s.NewestOffset,
		Retention:  s.Retention,
		AutoClose:  s.AutoClose,
	}, nil
}

// Due says whether a deadline passed that a write step applies, such as the auto close. A read
// step writes nothing, so the caller runs an update first.
func (s *Stream) Due(ctx chasm.Context, limits Limits) (bool, error) {
	deadline, ok := s.nextDeadline(ctx, limits)
	return ok && !deadline.After(ctx.Now(s)), nil
}

// ApplyDue applies the deadlines that passed.
func (s *Stream) ApplyDue(ctx chasm.MutableContext, limits Limits) {
	s.applyDue(ctx, limits)
	s.finish(ctx, limits)
}

func (s *Stream) writable() error {
	switch s.Phase {
	case streampb.STREAM_PHASE_OPEN:
		return nil
	case streampb.STREAM_PHASE_CLOSED:
		return NewError(ReasonClosed, "The Stream is closed")
	case streampb.STREAM_PHASE_EXPIRED:
		return NewError(ReasonExpired, "The Stream expired")
	default:
		return notFound()
	}
}

// withdrawOnCreate applies a retry run's withdraw_run at its first create.
func (s *Stream) withdrawOnCreate(ctx chasm.MutableContext, req *streamservice.CreateStreamRequest) {
	w := Withdrawal{WithdrawRun: req.GetWithdrawRun()}
	if s.withdrawsRun(w.WithdrawRun) {
		s.write(ctx, []*streamservice.StreamRecord{supersededRecord(req.GetOwner(), w)})
	}
	s.settle(req.GetOwner(), w, false)
}

// write gives the entries their offsets and stores them as one batch.
func (s *Stream) write(ctx chasm.MutableContext, entries []*streamservice.StreamRecord) (first int64, last int64) {
	if len(entries) == 0 {
		return 0, 0
	}
	next := max(s.NewestOffset+1, ctx.Now(s).UnixMilli()<<seqBits)
	batch := &streampb.RecordBatch{Records: entries}
	for _, entry := range entries {
		entry.Offset = next
		next++
	}
	first, last = entries[0].GetOffset(), entries[len(entries)-1].GetOffset()
	if s.Batches == nil {
		s.Batches = make(chasm.Map[int64, *streampb.RecordBatch])
	}
	s.Batches[first] = chasm.NewDataField(ctx, batch)
	size := int64(proto.Size(batch))
	s.Index = append(s.Index, &streampb.BatchInfo{FirstOffset: first, LastOffset: last, Bytes: size})
	s.StoredBytes += size
	s.NewestOffset = last
	return first, last
}

func (s *Stream) close(
	ctx chasm.MutableContext,
	producer *streamservice.Producer,
	result *commonpb.Payload,
	failure *failurepb.Failure,
) {
	offset, _ := s.write(ctx, []*streamservice.StreamRecord{{
		Kind:         streamservice.STREAM_RECORD_KIND_CLOSE,
		Producer:     producer,
		CloseResult:  result,
		CloseFailure: failure,
	}})
	s.Phase = streampb.STREAM_PHASE_CLOSED
	s.CloseOffset = offset
	s.CloseTime = timestamppb.New(ctx.Now(s))
}

// expire drops every Record. The state stays, so a read tells an expired Stream from a
// missing one until the framework deletes the execution.
func (s *Stream) expire(ctx chasm.MutableContext) {
	for offset := range s.Batches {
		delete(s.Batches, offset)
	}
	if len(s.Index) > 0 {
		s.TrimmedOffset = max(s.TrimmedOffset, s.Index[len(s.Index)-1].GetLastOffset())
	}
	s.Index = nil
	s.StoredBytes = 0
	s.Phase = streampb.STREAM_PHASE_EXPIRED
	s.resolveReads(ctx, func(*ParkedRead) (readOutcome, bool) {
		return readOutcome{err: NewError(ReasonExpired, "The Stream expired")}, true
	})
}

// trimTo drops the batches whose entries are all at or below offset. The batch of the close
// Record stays, so every read past the close still ends with it.
func (s *Stream) trimTo(offset int64) {
	for len(s.Index) > 0 {
		oldest := s.Index[0]
		if oldest.GetLastOffset() > offset || oldest.GetFirstOffset() == s.CloseOffset {
			return
		}
		s.dropOldest()
	}
}

func (s *Stream) dropOldest() {
	oldest := s.Index[0]
	delete(s.Batches, oldest.GetFirstOffset())
	s.Index = s.Index[1:]
	s.StoredBytes -= oldest.GetBytes()
	s.TrimmedOffset = max(s.TrimmedOffset, oldest.GetLastOffset())
}

// limitStored drops the oldest batches while the Stream holds more than the cap. The newest
// batch stays, since one batch is at most the batch limit.
func (s *Stream) limitStored(maxBytes int64) {
	for s.StoredBytes > maxBytes && len(s.Index) > 1 && s.Index[0].GetFirstOffset() != s.CloseOffset {
		s.dropOldest()
	}
}

func (s *Stream) limitProducers(maxProducers int) {
	for maxProducers > 0 && len(s.Producers) > maxProducers {
		oldestKey := ""
		var oldest int64
		for key, producer := range s.Producers {
			if oldestKey == "" || producer.GetLastOffset() < oldest {
				oldestKey, oldest = key, producer.GetLastOffset()
			}
		}
		delete(s.Producers, oldestKey)
	}
}

// retainedFrom is the offset below which the Retention hides the Records at now.
func (s *Stream) retainedFrom(now time.Time) int64 {
	retained := s.TrimmedOffset + 1
	if retention := s.GetRetention().AsDuration(); retention > 0 {
		if cutMs := now.Add(-retention).UnixMilli(); cutMs > 0 {
			retained = max(retained, cutMs<<seqBits)
		}
	}
	return retained
}

// applyDue applies every deadline that passed, in the order they came.
func (s *Stream) applyDue(ctx chasm.MutableContext, limits Limits) {
	now := ctx.Now(s)
	switch s.Phase {
	case streampb.STREAM_PHASE_PENDING:
		s.resolveReads(ctx, func(read *ParkedRead) (readOutcome, bool) {
			if deadline := read.GetCreateDeadline(); deadline != nil && !deadline.AsTime().After(now) {
				return readOutcome{err: notFound()}, true
			}
			return readOutcome{}, false
		})
		return
	case streampb.STREAM_PHASE_OPEN:
		if at, ok := s.autoCloseAt(); ok && !at.After(now) {
			s.LastWriteTime = timestamppb.New(at)
			s.close(ctx, &streamservice.Producer{}, nil, nil)
		} else if !s.ttlAt(limits).After(now) {
			s.expire(ctx)
			return
		}
	default:
	}
	if s.Phase == streampb.STREAM_PHASE_CLOSED && !s.closedExpiryAt().After(now) {
		s.expire(ctx)
		return
	}
	// A read whose wait ended answers an empty page, as the frontend long poll does.
	s.resolveReads(ctx, func(read *ParkedRead) (readOutcome, bool) {
		if deadline := read.GetWaitDeadline(); deadline != nil && !deadline.AsTime().After(now) {
			return readOutcome{resp: &streamservice.ReadRecordsResponse{NextOffset: read.GetScanOffset()}}, true
		}
		return readOutcome{}, false
	})
	if retention := s.GetRetention().AsDuration(); retention > 0 && len(s.Index) > 0 {
		if cutMs := now.Add(-retention).UnixMilli(); cutMs > 0 {
			s.trimTo((cutMs << seqBits) - 1)
		}
	}
}

func (s *Stream) autoCloseAt() (time.Time, bool) {
	autoClose := s.GetAutoClose().AsDuration()
	if s.Phase != streampb.STREAM_PHASE_OPEN || autoClose <= 0 {
		return time.Time{}, false
	}
	return s.GetLastWriteTime().AsTime().Add(autoClose), true
}

// ttlAt is when an open Stream expires with no write. A Workflow's Stream lives for the safety
// TTL, any other one for one Retention.
func (s *Stream) ttlAt(limits Limits) time.Time {
	ttl := s.GetRetention().AsDuration()
	if s.WorkflowOwned {
		ttl = limits.SafetyTTL
	}
	return s.GetLastWriteTime().AsTime().Add(ttl)
}

func (s *Stream) closedExpiryAt() time.Time {
	return s.GetCloseTime().AsTime().Add(s.GetRetention().AsDuration())
}

// nextDeadline is the earliest deadline of the Stream, if it has one.
func (s *Stream) nextDeadline(ctx chasm.Context, limits Limits) (time.Time, bool) {
	var deadline time.Time
	found := false
	consider := func(at time.Time) {
		if !found || at.Before(deadline) {
			deadline, found = at, true
		}
	}
	switch s.Phase {
	case streampb.STREAM_PHASE_PENDING:
		for _, field := range s.Reads {
			read, ok := field.TryGet(ctx)
			if ok && !read.GetResolved() && read.GetCreateDeadline() != nil {
				consider(read.GetCreateDeadline().AsTime())
			}
		}
		return deadline, found
	case streampb.STREAM_PHASE_OPEN:
		if at, ok := s.autoCloseAt(); ok {
			consider(at)
		}
		consider(s.ttlAt(limits))
	case streampb.STREAM_PHASE_CLOSED:
		consider(s.closedExpiryAt())
	default:
		return deadline, false
	}
	for _, field := range s.Reads {
		if read, ok := field.TryGet(ctx); ok && !read.GetResolved() && read.GetWaitDeadline() != nil {
			consider(read.GetWaitDeadline().AsTime())
		}
	}
	if retention := s.GetRetention().AsDuration(); retention > 0 && len(s.Index) > 0 &&
		s.Index[0].GetFirstOffset() != s.CloseOffset {
		consider(time.UnixMilli(s.Index[0].GetLastOffset() >> seqBits).Add(retention + time.Millisecond))
	}
	return deadline, found
}

// finish wakes the reads that can answer now, drops the reads whose answer went out, applies
// the size cap and arms the timer for the next deadline.
func (s *Stream) finish(ctx chasm.MutableContext, limits Limits) {
	s.limitStored(limits.MaxStoredBytes)
	if s.Phase != streampb.STREAM_PHASE_PENDING {
		s.resolveReads(ctx, func(read *ParkedRead) (readOutcome, bool) {
			return s.wake(ctx, read)
		})
	} else if s.waitingReads(ctx) == 0 {
		// No read waits for the create any more, so the execution can go.
		s.Phase = streampb.STREAM_PHASE_ABANDONED
	}
	s.pruneReads(ctx)
	deadline, ok := s.nextDeadline(ctx, limits)
	if !ok {
		s.TimerDeadline = nil
		return
	}
	if s.TimerDeadline != nil && s.TimerDeadline.AsTime().Equal(deadline) {
		return
	}
	s.TimerDeadline = timestamppb.New(deadline)
	ctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: deadline}, &streampb.StreamTimerTask{
		Deadline: s.TimerDeadline,
	})
}

// producerKey keys the Producer table by kind, id and run.
func producerKey(producer *streamservice.Producer) string {
	return strings.Join([]string{producer.GetKind().String(), producer.GetId(), producer.GetRunId()}, "|")
}

// CompareEpochs compares two epochs field by field, in the order of the crate. The order is the
// Workflow Task's started event id, its attempt and its start time, then the attempt's
// scheduled time and the attempt. An unset field compares as zero.
func CompareEpochs(a, b *streamservice.ProducerEpoch) int {
	if c := compareInts(a.GetTaskStartedEventId(), b.GetTaskStartedEventId()); c != 0 {
		return c
	}
	if c := compareInts(int64(a.GetTaskAttempt()), int64(b.GetTaskAttempt())); c != 0 {
		return c
	}
	if c := compareTimestamps(a.GetTaskStartedTime(), b.GetTaskStartedTime()); c != 0 {
		return c
	}
	if c := compareTimestamps(a.GetAttemptScheduledTime(), b.GetAttemptScheduledTime()); c != 0 {
		return c
	}
	return compareInts(int64(a.GetAttempt()), int64(b.GetAttempt()))
}

func compareTimestamps(a, b *timestamppb.Timestamp) int {
	if c := compareInts(a.GetSeconds(), b.GetSeconds()); c != 0 {
		return c
	}
	return compareInts(int64(a.GetNanos()), int64(b.GetNanos()))
}

func compareInts(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
