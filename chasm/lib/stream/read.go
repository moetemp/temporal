package stream

import (
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/streamservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// StartOffset is where a read starts. That is its from_offset, or the newest offset + 1 for a
// read from the end.
func (s *Stream) StartOffset(req *streamservice.ReadRecordsRequest) (int64, error) {
	if !s.Exists() {
		return 0, notFound()
	}
	if s.Phase == streampb.STREAM_PHASE_EXPIRED {
		return 0, NewError(ReasonExpired, "The Stream expired")
	}
	from := req.GetFromOffset()
	if from < 0 {
		return 0, NewError(ReasonOffset, "The offset is below zero")
	}
	if from == 0 && req.GetStart() == streamservice.READ_START_END {
		from = s.NewestOffset + 1
	}
	if from > s.NewestOffset+1 {
		return 0, NewError(ReasonOffset, "The offset %d is past the newest offset + 1, %d", from, s.NewestOffset+1)
	}
	return from, nil
}

// Page reads the Records from the offset from. It is false when the page has no Record, so a
// reader waits. A page ends at the close Record, and a read past the close gives the close
// Record again, so every read of a closed Stream ends with it.
func (s *Stream) Page(
	ctx chasm.Context,
	req *streamservice.ReadRecordsRequest,
	from int64,
) (*streamservice.ReadRecordsResponse, bool, error) {
	if s.Phase == streampb.STREAM_PHASE_EXPIRED || (from > 0 && from <= s.TrimmedOffset) {
		return nil, false, expired()
	}
	limit := MaxRecords
	if n := int(req.GetMaxRecords()); n > 0 && n < MaxRecords {
		limit = n
	}
	if s.Phase == streampb.STREAM_PHASE_CLOSED && from > s.CloseOffset {
		records, after := s.afterClose(ctx, from)
		return &streamservice.ReadRecordsResponse{
			Records:    append(records, s.closeRecord(ctx)),
			NextOffset: max(from, after),
			Done:       true,
		}, true, nil
	}
	resp, err := s.scan(ctx, req, from, limit)
	if err != nil {
		return nil, false, err
	}
	return resp, len(resp.GetRecords()) > 0, nil
}

// scan collects the Records from from that pass the Topic filter, up to limit.
func (s *Stream) scan(
	ctx chasm.Context,
	req *streamservice.ReadRecordsRequest,
	from int64,
	limit int,
) (*streamservice.ReadRecordsResponse, error) {
	// A read from the start skips what the Retention hides. A read from an offset fails on it,
	// since the reader asked for those Records.
	retained := s.retainedFrom(ctx.Now(s))
	start := from
	if start == 0 {
		start = retained
	}
	resp := &streamservice.ReadRecordsResponse{NextOffset: from}
	scanned := 0
	for _, info := range s.Index {
		if info.GetLastOffset() < start {
			continue
		}
		for _, entry := range s.Batches[info.GetFirstOffset()].Get(ctx).GetRecords() {
			if entry.GetOffset() < start {
				continue
			}
			if resp.Done || len(resp.Records) >= limit || scanned >= maxScan {
				return resp, nil
			}
			scanned++
			resp.NextOffset = entry.GetOffset() + 1
			if entry.GetOffset() < retained {
				if from > 0 {
					return nil, expired()
				}
				continue
			}
			s.take(ctx, req, resp, entry)
		}
	}
	return resp, nil
}
// take adds one entry to a page. A Superseded item passes every Topic filter, and the close
// Record ends the page after the Superseded items written after it.
func (s *Stream) take(
	ctx chasm.Context,
	req *streamservice.ReadRecordsRequest,
	resp *streamservice.ReadRecordsResponse,
	entry *streamservice.StreamRecord,
) {
	switch entry.GetKind() {
	case streamservice.STREAM_RECORD_KIND_CLOSE:
		records, after := s.afterClose(ctx, entry.GetOffset()+1)
		resp.Records = append(append(resp.Records, records...), common.CloneProto(entry))
		resp.NextOffset = max(resp.NextOffset, after)
		resp.Done = true
	case streamservice.STREAM_RECORD_KIND_SUPERSEDED:
		resp.Records = append(resp.Records, common.CloneProto(entry))
	default:
		if passes(req, entry.GetTopic()) {
			resp.Records = append(resp.Records, common.CloneProto(entry))
		}
	}
}

func passes(req *streamservice.ReadRecordsRequest, topic string) bool {
	if len(req.GetTopics()) == 0 && len(req.GetTopicPrefixes()) == 0 {
		return true
	}
	for _, t := range req.GetTopics() {
		if t == topic {
			return true
		}
	}
	for _, prefix := range req.GetTopicPrefixes() {
		if strings.HasPrefix(topic, prefix) {
			return true
		}
	}
	return false
}

// afterClose gives the Superseded items written after the close from the offset from, and the
// offset after the last entry, zero if none. A Workflow Task that completes after the close
// still withdraws, and a reader stops at the close Record, so a page puts them before it.
func (s *Stream) afterClose(ctx chasm.Context, from int64) ([]*streamservice.StreamRecord, int64) {
	var records []*streamservice.StreamRecord
	var after int64
	for _, info := range s.Index {
		if info.GetLastOffset() < from || info.GetLastOffset() <= s.CloseOffset {
			continue
		}
		for _, entry := range s.Batches[info.GetFirstOffset()].Get(ctx).GetRecords() {
			if entry.GetOffset() < from || entry.GetOffset() <= s.CloseOffset {
				continue
			}
			after = entry.GetOffset() + 1
			if entry.GetKind() == streamservice.STREAM_RECORD_KIND_SUPERSEDED {
				records = append(records, common.CloneProto(entry))
			}
		}
	}
	return records, after
}

func (s *Stream) closeRecord(ctx chasm.Context) *streamservice.StreamRecord {
	for _, entry := range s.Batches[s.CloseOffset].Get(ctx).GetRecords() {
		if entry.GetOffset() == s.CloseOffset {
			return common.CloneProto(entry)
		}
	}
	return &streamservice.StreamRecord{
		Kind:   streamservice.STREAM_RECORD_KIND_CLOSE,
		Offset: s.CloseOffset,
	}
}

// ReadInput is a Workflow read through System Nexus.
type ReadInput struct {
	Request *streamservice.ReadRecordsRequest
	// The Nexus request id. A retry of the start finds the read it parked.
	RequestID string
	// The completion callback of the Nexus operation. A read with none answers at once.
	Callback *commonpb.Callback
	Limits   Limits
}

// StartForRead is the start of an execution for a read. The Stream waits for its create. The
// start parks the read itself, since a child node that the update of a new execution adds is
// not persisted.
func StartForRead(ctx chasm.MutableContext, in ReadInput) (*Stream, error) {
	s := NewStream(ctx, streampb.STREAM_PHASE_PENDING)
	if _, err := s.ReadOrPark(ctx, in); err != nil {
		return nil, err
	}
	return s, nil
}

// ReadOrPark answers a read that has Records or ends, and parks any other read until the
// Stream can answer it. It answers nil for a parked read.
func (s *Stream) ReadOrPark(ctx chasm.MutableContext, in ReadInput) (*streamservice.ReadRecordsResponse, error) {
	s.applyDue(ctx, in.Limits)
	if field, ok := s.Reads[in.RequestID]; ok {
		read := field.Get(ctx)
		if !read.GetResolved() {
			return nil, nil
		}
		if read.GetFailure() != nil {
			return nil, errorOfFailure(read.GetFailure())
		}
		return common.CloneProto(read.GetResponse()), nil
	}
	now := ctx.Now(s)
	read := &ParkedRead{
		ParkedReadState: &streampb.ParkedReadState{
			Request:   in.Request,
			RequestId: in.RequestID,
			StartTime: timestamppb.New(now),
		},
	}
	if s.Phase == streampb.STREAM_PHASE_PENDING {
		createWait := min(in.Request.GetCreateWait().AsDuration(), MaxWait)
		if createWait <= 0 || in.Callback == nil {
			return nil, notFound()
		}
		read.CreateDeadline = timestamppb.New(now.Add(createWait))
	} else {
		from, err := s.StartOffset(in.Request)
		if err != nil {
			return nil, err
		}
		resp, found, err := s.Page(ctx, in.Request, from)
		if err != nil {
			return nil, err
		}
		if found || in.Callback == nil {
			s.finish(ctx, in.Limits)
			return resp, nil
		}
		read.ScanOffset = max(from, resp.GetNextOffset())
	}
	if s.waitingReads(ctx) >= in.Limits.MaxParkedReads {
		return nil, serviceerror.NewResourceExhausted(
			enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			"The Stream has the most waiting reads")
	}
	if s.Reads == nil {
		s.Reads = make(chasm.Map[string, *ParkedRead])
	}
	s.Reads[in.RequestID] = chasm.NewComponentField(ctx, read)
	if err := callback.Attach(ctx, read, in.RequestID, timestamppb.New(now), []*commonpb.Callback{in.Callback}); err != nil {
		return nil, err
	}
	s.finish(ctx, in.Limits)
	return nil, nil
}

// CancelRead completes a parked read as canceled.
func (s *Stream) CancelRead(ctx chasm.MutableContext, requestID string, limits Limits) error {
	if field, ok := s.Reads[requestID]; ok {
		if read := field.Get(ctx); !read.GetResolved() {
			read.resolve(ctx, nil, errCanceled)
		}
	}
	s.finish(ctx, limits)
	return nil
}

// readOutcome is the answer of a parked read, a page or an error.
type readOutcome struct {
	resp *streamservice.ReadRecordsResponse
	err  error
}

// wake answers a parked read when the Stream can. A look that finds only other Topics moves
// the read's offset on, so the next look starts after it.
func (s *Stream) wake(ctx chasm.MutableContext, read *ParkedRead) (readOutcome, bool) {
	if read.GetCreateDeadline() != nil {
		from, err := s.StartOffset(read.GetRequest())
		if err != nil {
			return readOutcome{err: err}, true
		}
		read.CreateDeadline = nil
		read.ScanOffset = from
	}
	resp, found, err := s.Page(ctx, read.GetRequest(), read.GetScanOffset())
	if err != nil {
		return readOutcome{err: err}, true
	}
	if found {
		return readOutcome{resp: resp}, true
	}
	read.ScanOffset = max(read.GetScanOffset(), resp.GetNextOffset())
	return readOutcome{}, false
}

// resolveReads completes each waiting read for which decide is done.
func (s *Stream) resolveReads(ctx chasm.MutableContext, decide func(*ParkedRead) (readOutcome, bool)) {
	for _, field := range s.Reads {
		read := field.Get(ctx)
		if read.GetResolved() {
			continue
		}
		if outcome, done := decide(read); done {
			read.resolve(ctx, outcome.resp, outcome.err)
		}
	}
}

func (s *Stream) waitingReads(ctx chasm.Context) int {
	waiting := 0
	for _, field := range s.Reads {
		if read, ok := field.TryGet(ctx); ok && !read.GetResolved() {
			waiting++
		}
	}
	return waiting
}

// pruneReads drops the completed reads whose callbacks are done.
func (s *Stream) pruneReads(ctx chasm.MutableContext) {
	for key, field := range s.Reads {
		read, ok := field.TryGet(ctx)
		if !ok || !read.GetResolved() {
			continue
		}
		delivered := true
		for _, cb := range read.Callbacks {
			status := cb.Get(ctx).GetStatus()
			if status != callbackspb.CALLBACK_STATUS_SUCCEEDED && status != callbackspb.CALLBACK_STATUS_FAILED {
				delivered = false
				break
			}
		}
		if delivered {
			delete(s.Reads, key)
		}
	}
}

// ReadTimeout is how long a frontend read waits, bounded by the crate's limit.
func ReadTimeout(req *streamservice.ReadRecordsRequest) (wait time.Duration, createWait time.Duration) {
	return min(max(req.GetWait().AsDuration(), 0), MaxWait), min(max(req.GetCreateWait().AsDuration(), 0), MaxWait)
}
