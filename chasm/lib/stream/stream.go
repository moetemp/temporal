package stream

import (
	"slices"
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
	// A Workflow created the Stream, so it keeps the safety TTL while it is open.
	WorkflowOwned bool
	// The run of the creating Workflow. A retry run's create withdraws withdraw_run for it.
	Producer *streamservice.Producer
	Limits   Limits
}

// CreateOutput is the answer of a create.
type CreateOutput struct {
	Created bool
	Status  streamservice.StreamStatus
}

// StartStream is the start of an execution for a create.
func StartStream(ctx chasm.MutableContext, in CreateInput) (*Stream, error) {
	return NewStream(ctx, streampb.STREAM_PHASE_PENDING), nil
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
		s.WorkflowOwned = in.WorkflowOwned
		s.CreateTime = now
		s.LastWriteTime = now
		if err := s.withdrawRun(ctx, in.Request.GetWithdrawRun(), in.Producer); err != nil {
			return CreateOutput{}, err
		}
		s.finish(ctx, in.Limits)
		return CreateOutput{Created: true, Status: s.Status()}, nil
	case streampb.STREAM_PHASE_OPEN:
		if s.GetRetention().AsDuration() != retention || s.GetAutoClose().AsDuration() != autoClose {
			return CreateOutput{}, NewError(ReasonAlreadyExists,
				"The Stream exists with the Retention %v and the auto close %v",
				s.GetRetention().AsDuration(), s.GetAutoClose().AsDuration())
		}
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
	// The Producer of the batch, from the request or, in a Workflow, from the caller's links.
	Producer *streamservice.Producer
	// The Nexus request id of a Workflow batch. A retry of the operation start carries it again.
	RequestID string
	Limits    Limits
}

// Append writes the Records of one batch, after the Producer checks.
func (s *Stream) Append(ctx chasm.MutableContext, in AppendInput) (*streamservice.AppendRecordsResponse, error) {
	s.applyDue(ctx, in.Limits)
	if err := s.writable(); err != nil {
		return nil, err
	}
	producer := in.Producer
	key := producerKey(producer)
	known := s.Producers[key]
	superseded := false
	if producer.GetKind() == streamservice.PRODUCER_KIND_WORKFLOW {
		// A Workflow writes from completed Workflow Tasks only, so it has no stale attempt to
		// fence. Its batches are deduplicated by the Nexus request id.
		if known != nil && in.RequestID != "" && known.GetRequestId() == in.RequestID {
			return &streamservice.AppendRecordsResponse{
				FirstOffset: known.GetFirstOffset(),
				LastOffset:  known.GetLastOffset(),
			}, nil
		}
	} else {
		if in.Request.GetSequence() < 1 {
			return nil, invalid("The sequence must be 1 or more")
		}
		if known != nil {
			switch c := CompareEpochs(producer.GetEpoch(), known.GetEpoch()); {
			case c < 0:
				return nil, NewError(ReasonFenced, "A newer attempt of the Producer %s wrote the Stream", producer.GetId())
			case c == 0 && in.Request.GetSequence() == known.GetSequence():
				return &streamservice.AppendRecordsResponse{
					FirstOffset: known.GetFirstOffset(),
					LastOffset:  known.GetLastOffset(),
				}, nil
			case c == 0 && in.Request.GetSequence() < known.GetSequence():
				return nil, NewError(ReasonSequence,
					"The sequence %d is below the last one, %d", in.Request.GetSequence(), known.GetSequence())
			case c > 0:
				superseded = true
			default:
			}
		}
	}
	if err := s.checkRun(producer); err != nil {
		return nil, err
	}

	var entries []*streamservice.StreamRecord
	if in.Request.GetWithdrawRun() != "" && !slices.Contains(s.FencedRuns, in.Request.GetWithdrawRun()) {
		s.FencedRuns = append(s.FencedRuns, in.Request.GetWithdrawRun())
		entries = append(entries, supersededRecord(producer, in.Request.GetWithdrawRun()))
	} else if superseded {
		entries = append(entries, supersededRecord(producer, ""))
	}
	for _, record := range in.Request.GetRecords() {
		entries = append(entries, &streamservice.StreamRecord{
			Kind:     streamservice.STREAM_RECORD_KIND_DATA,
			Topic:    record.GetTopic(),
			Producer: producer,
			Body:     record.GetBody(),
		})
	}
	size := 0
	for _, entry := range entries {
		size += proto.Size(entry)
	}
	if int64(size) > in.Limits.MaxBatchBytes {
		return nil, NewError(ReasonTooLarge, "The batch has %d bytes, more than %d", size, in.Limits.MaxBatchBytes)
	}

	first, last := s.write(ctx, entries)
	s.LastWriteTime = timestamppb.New(ctx.Now(s))
	if s.Producers == nil {
		s.Producers = make(map[string]*streampb.ProducerState)
	}
	s.Producers[key] = &streampb.ProducerState{
		Epoch:       producer.GetEpoch(),
		Sequence:    in.Request.GetSequence(),
		FirstOffset: first,
		LastOffset:  last,
		RequestId:   in.RequestID,
	}
	s.limitProducers(in.Limits.MaxProducers)
	s.finish(ctx, in.Limits)
	return &streamservice.AppendRecordsResponse{FirstOffset: first, LastOffset: last}, nil
}

// CloseInput is a close of the Stream.
type CloseInput struct {
	Request  *streamservice.CloseStreamRequest
	Producer *streamservice.Producer
	Limits   Limits
}

// Close writes the close Record. A second close does nothing.
func (s *Stream) Close(ctx chasm.MutableContext, in CloseInput) (*streamservice.CloseStreamResponse, error) {
	s.applyDue(ctx, in.Limits)
	if s.Phase == streampb.STREAM_PHASE_CLOSED {
		return &streamservice.CloseStreamResponse{}, nil
	}
	if err := s.writable(); err != nil {
		return nil, err
	}
	if in.Producer.GetKind() != streamservice.PRODUCER_KIND_WORKFLOW {
		if known := s.Producers[producerKey(in.Producer)]; known != nil &&
			CompareEpochs(in.Producer.GetEpoch(), known.GetEpoch()) < 0 {
			return nil, NewError(ReasonFenced, "A newer attempt of the Producer %s wrote the Stream", in.Producer.GetId())
		}
	}
	if err := s.checkRun(in.Producer); err != nil {
		return nil, err
	}
	s.close(ctx, in.Producer, in.Request.GetResult(), in.Request.GetFailure())
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

// checkRun refuses a Producer of a run that a retry run withdrew.
func (s *Stream) checkRun(producer *streamservice.Producer) error {
	if run := producer.GetRunId(); run != "" && slices.Contains(s.FencedRuns, run) {
		return NewError(ReasonFenced, "A retry run withdrew the run %s", run)
	}
	return nil
}

func (s *Stream) withdrawRun(ctx chasm.MutableContext, run string, producer *streamservice.Producer) error {
	if run == "" || slices.Contains(s.FencedRuns, run) {
		return nil
	}
	s.FencedRuns = append(s.FencedRuns, run)
	s.write(ctx, []*streamservice.StreamRecord{supersededRecord(producer, run)})
	return nil
}

func supersededRecord(producer *streamservice.Producer, withdrawRun string) *streamservice.StreamRecord {
	return &streamservice.StreamRecord{
		Kind: streamservice.STREAM_RECORD_KIND_SUPERSEDED,
		Supersession: &streamservice.Supersession{
			ProducerKind: producer.GetKind(),
			ProducerId:   producer.GetId(),
			Epoch:        producer.GetEpoch(),
			WithdrawRun:  withdrawRun,
			RunId:        producer.GetRunId(),
		},
	}
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

// CompareEpochs compares two epochs field by field, in the order of the crate: the Workflow
// Task's started event id, its attempt and its start time, then the attempt's scheduled time
// and the attempt. An unset field compares as zero.
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
