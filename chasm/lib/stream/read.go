package stream

import (
	"slices"
	"sort"
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
	"google.golang.org/protobuf/proto"
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
		return 0, NewError(
			ReasonOffset,
			"The offset %d is past the newest offset + 1, %d",
			from,
			s.NewestOffset+1,
		)
	}
	return from, nil
}

// Page reads the Records at or past from. Its bool is false when the page has no Record, so the
// reader waits. A read past the close gives the close Record again, so every read of a closed
// Stream ends with it.
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
	// A page from past the close answers the close Record even when to_offset is from, so the
	// replay of that page reads it again.
	if s.Phase == streampb.STREAM_PHASE_CLOSED && from > s.CloseOffset {
		records, after := s.afterClose(ctx, from, req.GetToOffset())
		return &streamservice.ReadRecordsResponse{
			Records:    append(records, s.closeRecord(ctx)),
			NextOffset: max(from, after),
			Done:       true,
		}, true, nil
	}
	if end := req.GetToOffset(); end > 0 && from >= end {
		return &streamservice.ReadRecordsResponse{NextOffset: from}, false, nil
	}
	resp, err := s.scan(ctx, req, from, limit)
	if err != nil {
		return nil, false, err
	}
	return resp, len(resp.GetRecords()) > 0, nil
}

// scan collects the Records at or past from that pass the Topic filter, up to limit.
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
	var size int64
	for _, info := range s.Index[s.firstBatchAt(start):] {
		for _, entry := range s.Batches[info.GetFirstOffset()].Get(ctx).GetRecords() {
			if entry.GetOffset() < start {
				continue
			}
			if end := req.GetToOffset(); end > 0 && entry.GetOffset() >= end {
				return resp, nil
			}
			if pageFull(resp, limit, scanned) {
				return resp, nil
			}
			if entry.GetOffset() >= retained && overBytes(req, resp, entry, size) {
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
			taken := len(resp.Records)
			s.take(ctx, req, resp, entry)
			if len(resp.Records) > taken {
				size += int64(proto.Size(entry))
			}
		}
	}
	return resp, nil
}

// pageFull says whether a page ends before its next entry. An empty page doesn't stop at the scan
// cap, so a reader can't mistake it for the end while a match is left.
func pageFull(resp *streamservice.ReadRecordsResponse, limit int, scanned int) bool {
	if len(resp.GetRecords()) == 0 {
		return false
	}
	return resp.GetDone() || len(resp.GetRecords()) >= limit || scanned >= maxScan
}

// firstBatchAt is the index of the first batch with an entry at or past offset, so a look of a
// parked read reads only the batches that came since its last look.
func (s *Stream) firstBatchAt(offset int64) int {
	return sort.Search(len(s.Index), func(i int) bool {
		return s.Index[i].GetLastOffset() >= offset
	})
}

// overBytes says whether entry would take a page past its max_bytes. A page takes its first
// Record at any size, so a read always makes progress.
func overBytes(
	req *streamservice.ReadRecordsRequest,
	resp *streamservice.ReadRecordsResponse,
	entry *streamservice.StreamRecord,
	size int64,
) bool {
	limit := req.GetMaxBytes()
	if limit <= 0 || len(resp.GetRecords()) == 0 {
		return false
	}
	if entry.GetKind() == streamservice.STREAM_RECORD_KIND_DATA && !passes(req, entry.GetTopic()) {
		return false
	}
	return size+int64(proto.Size(entry)) > limit
}

// Last reads the newest last Records that pass the filter, oldest first. A Record that the Stream
// knows a Superseded Record withdrew doesn't count. A page of a closed Stream ends with the close.
func (s *Stream) Last(
	ctx chasm.Context,
	req *streamservice.ReadRecordsRequest,
) (*streamservice.ReadRecordsResponse, error) {
	if !s.Exists() {
		return nil, notFound()
	}
	if s.Phase == streampb.STREAM_PHASE_EXPIRED {
		return nil, NewError(ReasonExpired, "The Stream expired")
	}
	want := min(int(req.GetLast()), MaxRecords)
	retained := s.retainedFrom(ctx.Now(s))
	var picked []*streamservice.StreamRecord
	var size int64
	scanned := 0
	collect := func() {
		for i := len(s.Index) - 1; i >= 0; i-- {
			entries := s.Batches[s.Index[i].GetFirstOffset()].Get(ctx).GetRecords()
			for j := len(entries) - 1; j >= 0; j-- {
				entry := entries[j]
				if entry.GetOffset() < retained || len(picked) >= want || scanned >= maxScan {
					return
				}
				scanned++
				if entry.GetKind() != streamservice.STREAM_RECORD_KIND_DATA ||
					!passes(req, entry.GetTopic()) || s.withdrawn(ctx, entry) {
					continue
				}
				entrySize := int64(proto.Size(entry))
				if limit := req.GetMaxBytes(); limit > 0 && len(picked) > 0 &&
					size+entrySize > limit {
					return
				}
				picked = append(picked, common.CloneProto(entry))
				size += entrySize
			}
		}
	}
	collect()
	slices.Reverse(picked)
	resp := &streamservice.ReadRecordsResponse{Records: picked, NextOffset: s.NewestOffset + 1}
	if s.Phase == streampb.STREAM_PHASE_CLOSED {
		resp.Records = append(resp.Records, s.closeRecord(ctx))
		resp.Done = true
	}
	return resp, nil
}

// checkLast refuses a last read where it can't work. A wake_only read answers no Records, and a
// Workflow's replay needs a fixed range, which the newest Records are not.
func checkLast(req *streamservice.ReadRecordsRequest, workflow bool) error {
	switch {
	case req.GetLast() == 0:
		return nil
	case req.GetLast() < 0:
		return invalid("last is below zero")
	case req.GetWakeOnly():
		return invalid("A wake_only read can't read the last Records")
	case workflow:
		return invalid("Workflow code can't read the last Records")
	case req.GetFromOffset() != 0 || req.GetToOffset() != 0 ||
		req.GetStart() == streamservice.READ_START_END:
		return invalid("A read of the last Records takes no start offset")
	default:
		return nil
	}
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
		records, after := s.afterClose(ctx, entry.GetOffset()+1, req.GetToOffset())
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

// wakeAnswer is the answer of a wake_only read. It has no Records, and its next offset is the
// read's start.
func (s *Stream) wakeAnswer(start int64) *streamservice.ReadRecordsResponse {
	return &streamservice.ReadRecordsResponse{
		NextOffset: start,
		Done:       s.Phase == streampb.STREAM_PHASE_CLOSED,
	}
}

// afterClose gives the Superseded items written after the close at or past from, and the offset
// after the last one. A reader stops at the close Record, so a page puts them before it.
func (s *Stream) afterClose(
	ctx chasm.Context,
	from, end int64,
) ([]*streamservice.StreamRecord, int64) {
	var records []*streamservice.StreamRecord
	var after int64
	for _, info := range s.Index[s.firstBatchAt(from):] {
		if info.GetLastOffset() <= s.CloseOffset {
			continue
		}
		for _, entry := range s.Batches[info.GetFirstOffset()].Get(ctx).GetRecords() {
			if entry.GetOffset() < from || entry.GetOffset() <= s.CloseOffset {
				continue
			}
			if end > 0 && entry.GetOffset() >= end {
				return records, after
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
	// The calling Workflow run. A wake keeps its floor, since its pages read from its start.
	Reader *ReaderRun
	Limits Limits
}

// parks says whether the read may park, a wake_only read whose operation a callback completes.
func (in ReadInput) parks() bool {
	return in.Callback != nil && in.Request.GetWakeOnly()
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

// ReadOrPark answers a read at once, but parks a wake_only read with no Record and answers nil.
// A wake's answer has no Records, so one append that wakes every parked read copies no page.
func (s *Stream) ReadOrPark(
	ctx chasm.MutableContext,
	in ReadInput,
) (*streamservice.ReadRecordsResponse, error) {
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
	if in.Reader != nil {
		read.ReaderRunId = in.Reader.RunID
	}
	parks := in.parks()
	wait, createWait := parkWaits(in.Request, in.Limits)
	if wait > 0 {
		read.WaitDeadline = timestamppb.New(now.Add(wait))
	}
	s.reopen()
	if s.Phase == streampb.STREAM_PHASE_PENDING {
		if createWait <= 0 || !parks {
			return nil, notFound()
		}
		read.CreateDeadline = timestamppb.New(now.Add(createWait))
	} else {
		from, err := s.StartOffset(in.Request)
		if err != nil {
			return nil, err
		}
		if err := s.keepWakeFloor(ctx, in, from); err != nil {
			return nil, err
		}
		resp, found, err := s.Page(ctx, in.Request, from)
		if err != nil {
			return nil, err
		}
		if found || !parks {
			s.finish(ctx, in.Limits)
			if in.Request.GetWakeOnly() {
				resp = s.wakeAnswer(from)
			}
			resp.ContinueAsNewSuggested = s.suggestsContinueAsNew(ctx, read.GetReaderRunId(),
				in.Limits)
			return resp, nil
		}
		read.StartOffset = from
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
	callbacks := []*commonpb.Callback{in.Callback}
	err := callback.Attach(ctx, read, in.RequestID, timestamppb.New(now), callbacks)
	if err != nil {
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

// wake answers a parked read when the Stream can. A look that finds only other Topics advances
// the read's offset, so the next look starts after those entries.
func (s *Stream) wake(
	ctx chasm.MutableContext,
	read *ParkedRead,
	limits Limits,
) (readOutcome, bool) {
	if read.GetCreateDeadline() != nil {
		from, err := s.StartOffset(read.GetRequest())
		if err != nil {
			return readOutcome{err: err}, true
		}
		read.CreateDeadline = nil
		read.StartOffset = from
		read.ScanOffset = from
	}
	// With no entry since the read's last look, a write doesn't rescan the parked read.
	if s.Phase == streampb.STREAM_PHASE_OPEN && read.GetScanOffset() > s.NewestOffset {
		return readOutcome{}, false
	}
	resp, found, err := s.Page(ctx, read.GetRequest(), read.GetScanOffset())
	if err != nil {
		return readOutcome{err: err}, true
	}
	if found && read.GetRequest().GetWakeOnly() {
		resp = s.wakeAnswer(read.GetStartOffset())
	}
	if found {
		resp.ContinueAsNewSuggested = s.suggestsContinueAsNew(ctx, read.GetReaderRunId(), limits)
		return readOutcome{resp: resp}, true
	}
	read.ScanOffset = max(read.GetScanOffset(), resp.GetNextOffset())
	return readOutcome{}, false
}

// resolveReads completes each waiting read for which decide is done.
func (s *Stream) resolveReads(
	ctx chasm.MutableContext,
	decide func(*ParkedRead) (readOutcome, bool),
) {
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
			if status != callbackspb.CALLBACK_STATUS_SUCCEEDED &&
				status != callbackspb.CALLBACK_STATUS_FAILED {
				delivered = false
				break
			}
		}
		if delivered {
			delete(s.Reads, key)
		}
	}
}

// parkWaits are the waits of a parked read, which is a wake_only read. It may wait for days,
// since it adds nothing to History while it waits.
func parkWaits(
	req *streamservice.ReadRecordsRequest,
	limits Limits,
) (wait time.Duration, createWait time.Duration) {
	if limits.MaxWakeWait <= 0 {
		return ReadTimeout(req)
	}
	return min(max(req.GetWait().AsDuration(), 0), limits.MaxWakeWait),
		min(max(req.GetCreateWait().AsDuration(), 0), limits.MaxWakeWait)
}

// ReadTimeout is how long a frontend read waits, bounded by the crate's limit.
func ReadTimeout(
	req *streamservice.ReadRecordsRequest,
) (wait time.Duration, createWait time.Duration) {
	wait = min(max(req.GetWait().AsDuration(), 0), MaxWait)
	createWait = min(max(req.GetCreateWait().AsDuration(), 0), MaxWait)
	return wait, createWait
}

// keepWakeFloor keeps the floor of a wake's Workflow in the transaction that takes the wake,
// since the wake's pages read from its start.
func (s *Stream) keepWakeFloor(ctx chasm.MutableContext, in ReadInput, from int64) error {
	if in.Reader == nil || !in.Request.GetWakeOnly() {
		return nil
	}
	return s.AddReader(ctx, *in.Reader, s.floorStart(in.Request, from), in.Limits)
}
