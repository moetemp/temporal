package stream

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"maps"
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

	// Producers are keyed by the Producer kind, id and run.
	Producers chasm.Map[string, *streampb.ProducerState]

	// Runs are the Workflow Task facts of each run that wrote, keyed by run id.
	Runs chasm.Map[string, *streampb.RunTasks]

	// Readers are the floors of the open Workflow runs that read the Stream, keyed by run id.
	Readers chasm.Map[string, *streampb.ReaderFloor]

	// Reads are the Workflow reads that wait for Records, keyed by their Nexus request id.
	Reads chasm.Map[string, *ParkedRead]
}

var _ chasm.RootComponent = (*Stream)(nil)

// NewStream builds a Stream in the given phase. An open Stream gets its settings from create.
func NewStream(ctx chasm.MutableContext, phase streampb.StreamPhase) *Stream {
	s := &Stream{
		StreamState: &streampb.StreamState{Phase: phase},
		Batches:     make(chasm.Map[int64, *streampb.RecordBatch]),
		Producers:   make(chasm.Map[string, *streampb.ProducerState]),
		Runs:        make(chasm.Map[string, *streampb.RunTasks]),
		Readers:     make(chasm.Map[string, *streampb.ReaderFloor]),
		Reads:       make(chasm.Map[string, *ParkedRead]),
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

// Create opens the Stream, idempotently for one Retention.
func (s *Stream) Create(ctx chasm.MutableContext, in CreateInput) (CreateOutput, error) {
	s.applyDue(ctx, in.Limits)
	retention := in.Request.GetRetention().AsDuration()
	if retention <= 0 {
		retention = in.Limits.DefaultRetention
	}
	switch s.Phase {
	case streampb.STREAM_PHASE_PENDING, streampb.STREAM_PHASE_UNSPECIFIED:
		now := timestamppb.New(ctx.Now(s))
		s.Phase = streampb.STREAM_PHASE_OPEN
		s.Retention = durationpb.New(retention)
		s.WorkflowOwned = owned(in.Request.GetOwner())
		s.CreateTime = now
		s.LastWriteTime = now
		s.Owner = in.Request.GetOwner()
		s.CloseWithRun = in.Request.GetCloseWithRun()
		s.withdrawOnCreate(ctx, in.Request)
		s.finish(ctx, in.Limits)
		return CreateOutput{Created: true, Status: s.Status()}, nil
	case streampb.STREAM_PHASE_OPEN:
		if s.GetRetention().AsDuration() != retention {
			return CreateOutput{}, NewError(ReasonAlreadyExists,
				"The Stream exists with the Retention %v", s.GetRetention().AsDuration())
		}
		if owned(in.Request.GetOwner()) {
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

// owned says whether a create's owner keeps the Stream open past one Retention with no write: a
// Workflow, or a Standalone Activity, which closes the Stream when it ends.
func owned(owner *streamservice.Producer) bool {
	kind := owner.GetKind()
	return kind == streamservice.PRODUCER_KIND_WORKFLOW ||
		kind == streamservice.PRODUCER_KIND_STANDALONE_ACTIVITY
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
	digest := BatchDigest(req.GetRecords())
	known := s.producer(ctx, key)
	if known != nil && CompareEpochs(producer.GetEpoch(), known.GetEpoch()) == 0 {
		if req.GetSequence() == known.GetSequence() {
			if len(known.GetDigest()) > 0 && !bytes.Equal(known.GetDigest(), digest) {
				return nil, NewError(ReasonSeqDivergent,
					"The sequence %d has other content than its batch", req.GetSequence())
			}
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
	w := Withdrawal{WithdrawRun: req.GetWithdrawRun(), Defer: deferred}
	if producer.GetKind() == streamservice.PRODUCER_KIND_WORKFLOW {
		w.PreviousCompleted, w.FailedTasks = req.GetPreviousCompletedTask(), req.GetFailedTasks()
	}
	flagged, err := s.admit(ctx, producer, w)
	if err != nil {
		return nil, err
	}
	orphans, orphaned := s.orphans(ctx, req)

	entries := batchEntries(req, w, flagged, len(orphaned.withdrawn) > 0)
	if err := s.checkSize(ctx, batchSize(entries), in.Limits); err != nil {
		return nil, err
	}

	first, last := s.write(ctx, entries)
	// The answer gives the batch's own Records, which have offsets one after another, so a caller
	// that merged writes counts each one's offset from the first.
	if n := len(req.GetRecords()); n > 0 {
		first = entries[len(entries)-n].GetOffset()
	} else if len(orphaned.withdrawn) > 0 && len(entries) > 1 {
		first = entries[1].GetOffset()
	}
	wrote := len(req.GetRecords()) > 0
	s.settle(ctx, producer, w, wrote)
	if orphans {
		s.settleOrphans(ctx, producer, orphaned, wrote)
	}
	state := &streampb.ProducerState{
		Epoch:       producer.GetEpoch(),
		Sequence:    req.GetSequence(),
		FirstOffset: first,
		LastOffset:  last,
		Holds:       s.producer(ctx, key).GetHolds(),
		Digest:      digest,
	}
	if withdrawsEpochs(producer.GetKind()) {
		if wrote {
			state.Holds = producer.GetEpoch()
		} else if flagged {
			state.Holds = nil
		}
	}
	s.setProducer(ctx, key, state)
	if wrote {
		s.LastWriteTime = timestamppb.New(ctx.Now(s))
	}
	s.limitProducers(ctx, in.Limits.MaxProducers)
	s.finish(ctx, in.Limits)
	return &streamservice.AppendRecordsResponse{FirstOffset: first, LastOffset: last}, nil
}

// BatchDigest is the SHA-256 of a batch's content, as the crate and the memory Store compute it.
// Each Record gives its Topic, its metadata count, its metadata sorted by key, and its data, each
// part with a big-endian u64 length first. A resend with the same sequence must have the same
// digest.
func BatchDigest(records []*streamservice.AppendRecord) []byte {
	h := sha256.New()
	writeLen := func(n int) {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n))
		_, _ = h.Write(b[:])
	}
	writeBytes := func(b []byte) {
		writeLen(len(b))
		_, _ = h.Write(b)
	}
	for _, record := range records {
		writeBytes([]byte(record.GetTopic()))
		metadata := record.GetBody().GetMetadata()
		keys := slices.Sorted(maps.Keys(metadata))
		writeLen(len(keys))
		for _, key := range keys {
			writeBytes([]byte(key))
			writeBytes(metadata[key])
		}
		writeBytes(record.GetBody().GetData())
	}
	return h.Sum(nil)
}

// batchEntries are the entries of one write: the orphans' Superseded Record, the batch's own
// Superseded Record, then the Records.
func batchEntries(req *streamservice.AppendRecordsRequest, w Withdrawal, flagged, orphans bool) []*streamservice.StreamRecord {
	producer := req.GetProducer()
	var entries []*streamservice.StreamRecord
	// The orphans go in an entry of their own, since the batch's first entry may carry its own
	// Superseded Record.
	if orphans {
		entries = append(entries, orphanRecord(req))
	}
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
	return entries
}

// checkSize refuses a batch over the batch limit, or one that the Stream can't take while its
// readers keep their Records, or one that its Workflow can't take.
func (s *Stream) checkSize(ctx chasm.MutableContext, size int64, limits Limits) error {
	if size > limits.MaxBatchBytes {
		return NewError(ReasonTooLarge, "The batch has %d bytes, more than %d", size, limits.MaxBatchBytes)
	}
	if kept := s.keptBytes(); kept+size > limits.MaxStoredBytes {
		return NewError(ReasonTooLarge, "The Stream keeps %d bytes for its readers, and the cap is %d", kept, limits.MaxStoredBytes)
	}
	// The Records of a Workflow Stream are in the Workflow's mutable state, which terminates the
	// run past its own limit, so the write fails first.
	if limits.MaxWorkflowStateBytes > 0 &&
		int64(ctx.ExecutionInfo().ApproximateStateSize)+size > limits.MaxWorkflowStateBytes {
		return NewError(ReasonTooLarge, "The Workflow state would pass %d bytes", limits.MaxWorkflowStateBytes)
	}
	return nil
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
	status, err := callerStatus(req)
	if err != nil {
		return nil, err
	}
	s.applyDue(ctx, in.Limits)
	if s.Phase == streampb.STREAM_PHASE_CLOSED {
		return &streamservice.CloseStreamResponse{}, nil
	}
	if err := s.writable(); err != nil {
		return nil, err
	}
	if err := s.checkCloser(producer); err != nil {
		return nil, err
	}
	w := Withdrawal{PreviousCompleted: req.GetPreviousCompletedTask(), FailedTasks: req.GetFailedTasks(), WithdrawRun: req.GetWithdrawRun()}
	flagged, err := s.admit(ctx, producer, w)
	if err != nil {
		return nil, err
	}
	if flagged {
		s.write(ctx, []*streamservice.StreamRecord{supersededRecord(producer, w)})
	}
	s.settle(ctx, producer, w, false)
	s.close(ctx, producer, req.GetResult(), req.GetFailure(), status)
	s.finish(ctx, in.Limits)
	return &streamservice.CloseStreamResponse{}, nil
}

// callerStatus is the status of a caller's close. Only the owner's end terminates or times out a
// Stream, so a reader can trust those two.
func callerStatus(req *streamservice.CloseStreamRequest) (streamservice.CloseStatus, error) {
	switch status := req.GetStatus(); status {
	case streamservice.CLOSE_STATUS_TERMINATED, streamservice.CLOSE_STATUS_TIMED_OUT:
		return 0, invalid("A caller can't close a Stream as %v", status)
	case streamservice.CLOSE_STATUS_UNSPECIFIED:
		if req.GetFailure() != nil {
			return streamservice.CLOSE_STATUS_FAILED, nil
		}
		return streamservice.CLOSE_STATUS_COMPLETED, nil
	default:
		return status, nil
	}
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
	}, nil
}

// Due says whether a deadline passed that a write step applies, such as the TTL. A read step
// writes nothing, so the caller runs an update first.
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
	if s.withdrawsRun(ctx, w.WithdrawRun) {
		s.write(ctx, []*streamservice.StreamRecord{supersededRecord(req.GetOwner(), w)})
	}
	s.settle(ctx, req.GetOwner(), w, false)
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
	status streamservice.CloseStatus,
) {
	offset, _ := s.write(ctx, []*streamservice.StreamRecord{{
		Kind:         streamservice.STREAM_RECORD_KIND_CLOSE,
		Producer:     producer,
		CloseResult:  result,
		CloseFailure: failure,
		CloseStatus:  status,
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
		if oldest.GetLastOffset() > offset || s.kept(oldest) {
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
	for s.StoredBytes > maxBytes && len(s.Index) > 1 && !s.kept(s.Index[0]) {
		s.dropOldest()
	}
}

// kept says whether a batch stays when the Retention or the size cap would drop it. The close
// Record stays so that every read past the close ends with it. A batch at or past a reader's
// floor stays so that the reader's replay finds it.
func (s *Stream) kept(info *streampb.BatchInfo) bool {
	if info.GetFirstOffset() == s.CloseOffset {
		return true
	}
	floor, ok := s.floor()
	return ok && info.GetLastOffset() >= floor
}

// keptBytes is the size of the batches that no trim can drop now.
func (s *Stream) keptBytes() int64 {
	if len(s.Readers) == 0 {
		return 0
	}
	var kept int64
	for _, info := range s.Index {
		if s.kept(info) {
			kept += info.GetBytes()
		}
	}
	return kept
}

// floor is the lowest offset that an open Workflow reader read.
func (s *Stream) floor() (int64, bool) {
	return s.Floor, s.Floor > 0 && len(s.Readers) > 0
}

// retainedFrom is the offset below which the Retention hides the Records at now. A Workflow Stream
// hides none that it still stores, so a read from the start begins at the same offset on replay.
func (s *Stream) retainedFrom(now time.Time) int64 {
	retained := s.TrimmedOffset + 1
	if retention := s.GetRetention().AsDuration(); retention > 0 && !s.Nested {
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
		if !s.ttlAt(limits).After(now) && len(s.Readers) == 0 {
			s.expire(ctx)
			return
		}
	default:
	}
	// A reader's replay needs its Records, so the Stream expires only when it has no reader.
	if s.Phase == streampb.STREAM_PHASE_CLOSED && !s.closedExpiryAt().After(now) && len(s.Readers) == 0 {
		s.expire(ctx)
		return
	}
	// A read whose wait ended answers an empty page, as the frontend long poll does.
	s.resolveReads(ctx, func(read *ParkedRead) (readOutcome, bool) {
		if deadline := read.GetWaitDeadline(); deadline != nil && !deadline.AsTime().After(now) {
			next := read.GetScanOffset()
			if read.GetRequest().GetWakeOnly() {
				next = read.GetStartOffset()
			}
			return readOutcome{resp: &streamservice.ReadRecordsResponse{
				NextOffset:             next,
				ContinueAsNewSuggested: s.suggestsContinueAsNew(ctx, read.GetReaderRunId(), limits),
			}}, true
		}
		return readOutcome{}, false
	})
	if retention := s.GetRetention().AsDuration(); retention > 0 && len(s.Index) > 0 {
		if cutMs := now.Add(-retention).UnixMilli(); cutMs > 0 {
			s.trimTo((cutMs << seqBits) - 1)
		}
	}
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
		if len(s.Readers) == 0 {
			consider(s.ttlAt(limits))
		}
	case streampb.STREAM_PHASE_CLOSED:
		if len(s.Readers) == 0 {
			consider(s.closedExpiryAt())
		}
	default:
		return deadline, false
	}
	for _, field := range s.Reads {
		if read, ok := field.TryGet(ctx); ok && !read.GetResolved() && read.GetWaitDeadline() != nil {
			consider(read.GetWaitDeadline().AsTime())
		}
	}
	if retention := s.GetRetention().AsDuration(); retention > 0 && len(s.Index) > 0 && !s.kept(s.Index[0]) {
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
			return s.wake(ctx, read, limits)
		})
	} else if s.waitingReads(ctx) == 0 {
		// No read waits for the create any more, so the execution can go.
		s.Phase = streampb.STREAM_PHASE_ABANDONED
	}
	s.pruneReads(ctx)
	s.armReadersCheck(ctx, limits)
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
