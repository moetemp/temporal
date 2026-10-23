package stream

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/streamservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type harness struct {
	t      *testing.T
	ctx    context.Context
	engine *chasmtest.Engine
	clock  *clock.EventTimeSource
	key    chasm.ExecutionKey
	limits Limits
}

func newHarness(t *testing.T) *harness {
	registry := chasm.NewRegistry(log.NewNoopLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(callback.NewNilLibrary()))
	config := ConfigProvider(dynamicconfig.NewNoopCollection())
	require.NoError(t, registry.Register(newLibrary(config, nil, nil, newTimerTaskHandler(config), nil, nil)))
	ts := clock.NewEventTimeSource()
	ts.Update(time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC))
	engine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(ts))
	return &harness{
		t:      t,
		ctx:    chasm.NewEngineContext(context.Background(), engine),
		engine: engine,
		clock:  ts,
		key:    chasm.ExecutionKey{NamespaceID: "ns", BusinessID: "chat-1"},
		limits: config.LimitsFor("ns"),
	}
}

func (h *harness) ref() chasm.ComponentRef {
	return chasm.NewComponentRef[*Stream](h.key)
}

func (h *harness) create(req *streamservice.CreateStreamRequest) (CreateOutput, error) {
	result, err := chasm.UpdateWithStartExecution(h.ctx, h.key, StartStream, (*Stream).Create,
		CreateInput{Request: req, Limits: h.limits},
		chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyRejectDuplicate, chasm.BusinessIDConflictPolicyUseExisting),
	)
	out := result.UpdateOutput
	out.Created = out.Created || result.Created
	return out, err
}

func (h *harness) append(producer *streamservice.Producer, sequence int64, topics ...string) (*streamservice.AppendRecordsResponse, error) {
	req := &streamservice.AppendRecordsRequest{Producer: producer, Sequence: sequence}
	for _, topic := range topics {
		req.Records = append(req.Records, &streamservice.AppendRecord{
			Topic: topic,
			Body:  &commonpb.Payload{Data: []byte(topic)},
		})
	}
	return h.appendRequest(req)
}

func (h *harness) appendRequest(req *streamservice.AppendRecordsRequest) (*streamservice.AppendRecordsResponse, error) {
	resp, _, err := chasm.UpdateComponent(h.ctx, h.ref(), (*Stream).Append, AppendInput{Request: req, Limits: h.limits})
	return resp, err
}

func (h *harness) close(result string) error {
	_, _, err := chasm.UpdateComponent(h.ctx, h.ref(), (*Stream).Close, CloseInput{
		Request: &streamservice.CloseStreamRequest{
			Producer: clientProducer(),
			Result:   &commonpb.Payload{Data: []byte(result)},
		},
		Limits: h.limits,
	})
	return err
}

func (h *harness) read(req *streamservice.ReadRecordsRequest) (*streamservice.ReadRecordsResponse, error) {
	return chasm.ReadComponent(h.ctx, h.ref(),
		func(s *Stream, ctx chasm.Context, req *streamservice.ReadRecordsRequest) (*streamservice.ReadRecordsResponse, error) {
			from, err := s.StartOffset(req)
			if err != nil {
				return nil, err
			}
			resp, _, err := s.Page(ctx, req, from)
			return resp, err
		}, req)
}

func (h *harness) readOrPark(requestID string, req *streamservice.ReadRecordsRequest) (*streamservice.ReadRecordsResponse, error) {
	result, err := chasm.UpdateWithStartExecution(h.ctx, h.key,
		StartForRead,
		(*Stream).ReadOrPark,
		ReadInput{
			Request:   req,
			RequestID: requestID,
			Callback: &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{
				Url: chasm.NexusCompletionHandlerURL,
			}}},
			Limits: h.limits,
		},
		chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyRejectDuplicate, chasm.BusinessIDConflictPolicyUseExisting),
	)
	return result.UpdateOutput, err
}

type parkedView struct {
	*streampb.ParkedReadState
	callbacks []callbackspb.CallbackStatus
}

func (h *harness) parked(requestID string) *parkedView {
	view, err := chasm.ReadComponent(h.ctx, h.ref(),
		func(s *Stream, ctx chasm.Context, id string) (*parkedView, error) {
			field, ok := s.Reads[id]
			if !ok {
				return nil, nil
			}
			read := field.Get(ctx)
			view := &parkedView{ParkedReadState: common.CloneProto(read.ParkedReadState)}
			for _, cb := range read.Callbacks {
				view.callbacks = append(view.callbacks, cb.Get(ctx).GetStatus())
			}
			return view, nil
		}, requestID)
	require.NoError(h.t, err)
	return view
}

func (h *harness) advance(d time.Duration) {
	h.clock.Update(h.clock.Now().Add(d))
	_, err := h.engine.FirePureTasks(h.ref(), h.clock.Now())
	require.NoError(h.t, err)
}

func clientProducer() *streamservice.Producer {
	return &streamservice.Producer{
		Kind:  streamservice.PRODUCER_KIND_CLIENT,
		Id:    "client:test:1",
		Epoch: &streamservice.ProducerEpoch{Attempt: 1},
	}
}

func activityProducer(attempt int32) *streamservice.Producer {
	return &streamservice.Producer{
		Kind:  streamservice.PRODUCER_KIND_ACTIVITY,
		Id:    "act:run:1:1",
		Epoch: &streamservice.ProducerEpoch{Attempt: attempt},
		RunId: "run",
	}
}

func topics(resp *streamservice.ReadRecordsResponse) []string {
	var out []string
	for _, record := range resp.GetRecords() {
		switch record.GetKind() {
		case streamservice.STREAM_RECORD_KIND_SUPERSEDED:
			out = append(out, "superseded")
		case streamservice.STREAM_RECORD_KIND_CLOSE:
			out = append(out, "close")
		default:
			out = append(out, record.GetTopic())
		}
	}
	return out
}

func TestCreateIsIdempotentForOneRetention(t *testing.T) {
	h := newHarness(t)
	out, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)
	require.Equal(t, CreateOutput{Created: true, Status: streamservice.STREAM_STATUS_OPEN}, out)

	out, err = h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)
	require.Equal(t, CreateOutput{Status: streamservice.STREAM_STATUS_OPEN}, out)

	_, err = h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1", Retention: durationpb.New(time.Hour)})
	require.ErrorContains(t, err, ReasonAlreadyExists)
}

func TestAppendAndRead(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)

	first, err := h.append(activityProducer(1), 1, "turns", "tool/search", "other")
	require.NoError(t, err)
	require.Equal(t, first.GetFirstOffset()+2, first.GetLastOffset())
	require.Equal(t, h.clock.Now().UnixMilli()<<seqBits, first.GetFirstOffset())

	all, err := h.read(&streamservice.ReadRecordsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"turns", "tool/search", "other"}, topics(all))
	require.Equal(t, first.GetLastOffset()+1, all.GetNextOffset())

	filtered, err := h.read(&streamservice.ReadRecordsRequest{Topics: []string{"turns"}, TopicPrefixes: []string{"tool/"}})
	require.NoError(t, err)
	require.Equal(t, []string{"turns", "tool/search"}, topics(filtered))

	page, err := h.read(&streamservice.ReadRecordsRequest{FromOffset: first.GetFirstOffset() + 1, MaxRecords: 1})
	require.NoError(t, err)
	require.Equal(t, []string{"tool/search"}, topics(page))
	require.Equal(t, first.GetFirstOffset()+2, page.GetNextOffset())

	end, err := h.read(&streamservice.ReadRecordsRequest{Start: streamservice.READ_START_END})
	require.NoError(t, err)
	require.Empty(t, end.GetRecords())

	_, err = h.read(&streamservice.ReadRecordsRequest{FromOffset: first.GetLastOffset() + 2})
	require.ErrorContains(t, err, ReasonOffset)
}

func TestProducerFence(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)

	first, err := h.append(activityProducer(1), 1, "a")
	require.NoError(t, err)
	resend, err := h.append(activityProducer(1), 1, "a")
	require.NoError(t, err)
	require.Equal(t, first, resend)

	_, err = h.append(activityProducer(1), 2, "b")
	require.NoError(t, err)
	_, err = h.append(activityProducer(1), 1, "a")
	require.ErrorContains(t, err, ReasonSequence)

	_, err = h.append(activityProducer(2), 1, "c")
	require.NoError(t, err)
	_, err = h.append(activityProducer(1), 3, "d")
	require.ErrorContains(t, err, ReasonFenced)

	all, err := h.read(&streamservice.ReadRecordsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "superseded", "c"}, topics(all))
	require.Equal(t, int32(2), all.GetRecords()[2].GetSupersession().GetEpoch().GetAttempt())
}

func TestCloseEndsEveryRead(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)
	_, err = h.append(activityProducer(1), 1, "a")
	require.NoError(t, err)
	require.NoError(t, h.close("done"))
	require.NoError(t, h.close("again"))

	_, err = h.append(activityProducer(1), 2, "b")
	require.ErrorContains(t, err, ReasonClosed)

	all, err := h.read(&streamservice.ReadRecordsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "close"}, topics(all))
	require.True(t, all.GetDone())
	require.Equal(t, "done", string(all.GetRecords()[1].GetCloseResult().GetData()))

	after, err := h.read(&streamservice.ReadRecordsRequest{FromOffset: all.GetNextOffset()})
	require.NoError(t, err)
	require.Equal(t, []string{"close"}, topics(after))
	require.True(t, after.GetDone())

	out, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)
	require.Equal(t, CreateOutput{Status: streamservice.STREAM_STATUS_CLOSED}, out)
}

func TestAutoCloseAfterNoAppend(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1", AutoClose: durationpb.New(time.Minute)})
	require.NoError(t, err)
	h.advance(40 * time.Second)
	_, err = h.append(activityProducer(1), 1, "a")
	require.NoError(t, err)
	h.advance(40 * time.Second)

	open, err := h.read(&streamservice.ReadRecordsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, topics(open))

	h.advance(30 * time.Second)
	closed, err := h.read(&streamservice.ReadRecordsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "close"}, topics(closed))
	require.Nil(t, closed.GetRecords()[1].GetCloseResult())
}

func TestRetentionDropsOldRecordsAndExpiresTheStream(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1", Retention: durationpb.New(time.Hour)})
	require.NoError(t, err)
	old, err := h.append(activityProducer(1), 1, "old")
	require.NoError(t, err)
	h.advance(40 * time.Minute)
	_, err = h.append(activityProducer(1), 2, "new")
	require.NoError(t, err)
	h.advance(30 * time.Minute)

	all, err := h.read(&streamservice.ReadRecordsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"new"}, topics(all))
	_, err = h.read(&streamservice.ReadRecordsRequest{FromOffset: old.GetFirstOffset()})
	require.ErrorContains(t, err, ReasonExpired)

	h.advance(time.Hour)
	_, err = h.read(&streamservice.ReadRecordsRequest{})
	require.ErrorContains(t, err, ReasonExpired)
	out, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1", Retention: durationpb.New(time.Hour)})
	require.Error(t, err)
	require.Equal(t, CreateOutput{}, out)
}

func TestParkedReadWakesOnAppend(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)

	resp, err := h.readOrPark("read-1", &streamservice.ReadRecordsRequest{Topics: []string{"turns"}})
	require.NoError(t, err)
	require.Nil(t, resp)
	require.False(t, h.parked("read-1").GetResolved())

	_, err = h.append(activityProducer(1), 1, "other")
	require.NoError(t, err)
	require.False(t, h.parked("read-1").GetResolved())

	_, err = h.append(activityProducer(1), 2, "turns")
	require.NoError(t, err)
	read := h.parked("read-1")
	require.True(t, read.GetResolved())
	require.Equal(t, []string{"turns"}, topics(read.GetResponse()))
	require.Equal(t, []callbackspb.CallbackStatus{callbackspb.CALLBACK_STATUS_SCHEDULED}, read.callbacks)

	again, err := h.readOrPark("read-1", &streamservice.ReadRecordsRequest{Topics: []string{"turns"}})
	require.NoError(t, err)
	require.Equal(t, []string{"turns"}, topics(again))
}

func TestParkedReadWakesOnClose(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)
	resp, err := h.readOrPark("read-1", &streamservice.ReadRecordsRequest{})
	require.NoError(t, err)
	require.Nil(t, resp)

	require.NoError(t, h.close("done"))
	read := h.parked("read-1")
	require.True(t, read.GetResolved())
	require.True(t, read.GetResponse().GetDone())
}

func TestReadWithRecordsAnswersAtOnce(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)
	_, err = h.append(activityProducer(1), 1, "a")
	require.NoError(t, err)

	resp, err := h.readOrPark("read-1", &streamservice.ReadRecordsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, topics(resp))
	require.Nil(t, h.parked("read-1"))
}

func TestReadWaitsForTheCreate(t *testing.T) {
	h := newHarness(t)
	_, err := h.readOrPark("read-1", &streamservice.ReadRecordsRequest{})
	require.ErrorContains(t, err, ReasonNotFound)

	resp, err := h.readOrPark("read-1", &streamservice.ReadRecordsRequest{CreateWait: durationpb.New(30 * time.Second)})
	require.NoError(t, err)
	require.Nil(t, resp)
	_, err = h.append(activityProducer(1), 1, "a")
	require.ErrorContains(t, err, ReasonNotFound)

	out, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)
	require.True(t, out.Created)
	require.False(t, h.parked("read-1").GetResolved())

	_, err = h.append(activityProducer(1), 1, "a")
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, topics(h.parked("read-1").GetResponse()))
}

func TestReadForTheCreateTimesOut(t *testing.T) {
	h := newHarness(t)
	resp, err := h.readOrPark("read-1", &streamservice.ReadRecordsRequest{CreateWait: durationpb.New(30 * time.Second)})
	require.NoError(t, err)
	require.Nil(t, resp)

	h.advance(31 * time.Second)
	read := h.parked("read-1")
	require.True(t, read.GetResolved())
	require.Equal(t, ReasonNotFound, read.GetFailure().GetApplicationFailureInfo().GetType())
}

// workflowProducer is the run's Producer in the Workflow Task that started at event started.
func workflowProducer(started int64) *streamservice.Producer {
	return &streamservice.Producer{
		Kind:  streamservice.PRODUCER_KIND_WORKFLOW,
		Id:    "wf:run",
		Epoch: &streamservice.ProducerEpoch{TaskStartedEventId: started, TaskAttempt: 1},
		RunId: "run",
	}
}

func localActivityProducer(started int64) *streamservice.Producer {
	return &streamservice.Producer{
		Kind:  streamservice.PRODUCER_KIND_LOCAL_ACTIVITY,
		Id:    "la:run:1",
		Epoch: &streamservice.ProducerEpoch{TaskStartedEventId: started, TaskAttempt: 1, Attempt: 1},
		RunId: "run",
	}
}

func records(topics ...string) []*streamservice.AppendRecord {
	var out []*streamservice.AppendRecord
	for _, topic := range topics {
		out = append(out, &streamservice.AppendRecord{Topic: topic, Body: &commonpb.Payload{Data: []byte(topic)}})
	}
	return out
}

func TestWorkflowFailedTaskIsWithdrawn(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)

	// Task 3 writes and its Local Activity writes, then task 3 fails. Task 7 names it failed.
	_, err = h.append(workflowProducer(3), 1, "a")
	require.NoError(t, err)
	_, err = h.append(localActivityProducer(3), 1, "la")
	require.NoError(t, err)
	_, err = h.appendRequest(&streamservice.AppendRecordsRequest{
		Producer:              workflowProducer(7),
		Sequence:              1,
		Records:               records("b"),
		PreviousCompletedTask: &streamservice.WorkflowTaskEpoch{StartedEventId: 2, Attempt: 1},
		FailedTasks:           []int64{3},
	})
	require.NoError(t, err)

	all, err := h.read(&streamservice.ReadRecordsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "la", "superseded", "b"}, topics(all))
	require.Equal(t, []int64{3}, all.GetRecords()[2].GetSupersession().GetFailedTasks())

	// A late Local Activity of the withdrawn task is fenced.
	_, err = h.append(localActivityProducer(3), 2, "late")
	require.ErrorContains(t, err, ReasonFenced)
}

func TestWithdrawRunFencesThePreviousRun(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)
	_, err = h.append(workflowProducer(3), 1, "a")
	require.NoError(t, err)

	retry := workflowProducer(3)
	retry.Id, retry.RunId = "wf:retry", "retry"
	_, err = h.appendRequest(&streamservice.AppendRecordsRequest{Producer: retry, Sequence: 1, Records: records("b"), WithdrawRun: "run"})
	require.NoError(t, err)
	_, err = h.append(workflowProducer(9), 1, "c")
	require.ErrorContains(t, err, ReasonFenced)

	all, err := h.read(&streamservice.ReadRecordsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "superseded", "b"}, topics(all))
	require.Equal(t, "run", all.GetRecords()[1].GetSupersession().GetWithdrawRun())
}

func TestWorkflowWithdrawalAfterTheClose(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)
	_, err = h.append(workflowProducer(3), 1, "a")
	require.NoError(t, err)
	require.NoError(t, h.close("done"))

	// The task that wrote failed. The next task settles with no Records after the close.
	_, err = h.appendRequest(&streamservice.AppendRecordsRequest{
		Producer:    workflowProducer(7),
		Sequence:    1,
		FailedTasks: []int64{3},
	})
	require.NoError(t, err)
	_, err = h.append(workflowProducer(7), 2, "b")
	require.ErrorContains(t, err, ReasonClosed)

	all, err := h.read(&streamservice.ReadRecordsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "superseded", "close"}, topics(all))
	require.True(t, all.GetDone())
	after, err := h.read(&streamservice.ReadRecordsRequest{FromOffset: all.GetNextOffset()})
	require.NoError(t, err)
	require.Equal(t, []string{"close"}, topics(after))
}

func TestErrorsCarryTheReason(t *testing.T) {
	err := NewError(ReasonFenced, "fenced")
	details := serviceerror.ToStatus(err).Details()
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
	require.Len(t, details, 1)
	require.Equal(t, ReasonFenced, details[0].(*streamservice.StreamErrorDetails).GetReason())
}

// A reader's floor keeps its Records past the Retention and the cap, and the Records below it
// trim. When the reader goes, the Retention trims the rest.
func TestReaderFloorKeepsItsRecords(t *testing.T) {
	h := newHarness(t)
	h.limits.MaxStoredBytes = 200
	_, err := h.create(&streamservice.CreateStreamRequest{
		StreamId: "chat-1", Retention: durationpb.New(time.Minute), Owner: workflowProducer(1),
	})
	require.NoError(t, err)
	_, err = h.append(activityProducer(1), 1, "a")
	require.NoError(t, err)
	b, err := h.append(activityProducer(1), 2, "b")
	require.NoError(t, err)
	setReaders := func(fn func(*Stream, chasm.MutableContext) error) {
		_, _, err := chasm.UpdateComponent(h.ctx, h.ref(), func(s *Stream, mctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			return struct{}{}, fn(s, mctx)
		}, struct{}{})
		require.NoError(t, err)
	}
	setReaders(func(s *Stream, mctx chasm.MutableContext) error {
		s.Nested = true
		return s.AddReader(mctx, ReaderRun{WorkflowID: "wf", RunID: "run-1"},
			b.GetFirstOffset(), h.limits)
	})

	big := &streamservice.AppendRecordsRequest{Producer: activityProducer(1), Sequence: 3, Records: []*streamservice.AppendRecord{
		{Topic: "c", Body: &commonpb.Payload{Data: make([]byte, 300)}},
	}}
	_, err = h.appendRequest(big)
	require.ErrorContains(t, err, ReasonTooLarge)

	h.advance(2 * time.Minute)
	all, err := h.read(&streamservice.ReadRecordsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"b"}, topics(all))

	setReaders(func(s *Stream, mctx chasm.MutableContext) error {
		s.dropReaders(mctx, []string{"run-1"})
		s.ApplyDue(mctx, h.limits)
		return nil
	})
	_, err = h.read(&streamservice.ReadRecordsRequest{FromOffset: b.GetFirstOffset()})
	require.ErrorContains(t, err, ReasonExpired)
}

// localActivityWrite is a write of the Local Activity call of the run, dispatched by task
// started, with the run's last completed task (0 before the first), as the crate's S110 test has.
func localActivityWrite(call string, started, completed int64, topic string) *streamservice.AppendRecordsRequest {
	return &streamservice.AppendRecordsRequest{
		Producer: &streamservice.Producer{
			Kind: streamservice.PRODUCER_KIND_LOCAL_ACTIVITY,
			Id:   "la:run:" + call,
			Epoch: &streamservice.ProducerEpoch{
				TaskStartedEventId: started, TaskAttempt: 1,
				TaskStartedTime: timestamppb.New(time.Unix(100+started, 0)), Attempt: 1,
			},
			RunId: "run",
		},
		Sequence:              1,
		Records:               records(topic),
		PreviousCompletedTask: &streamservice.WorkflowTaskEpoch{StartedEventId: completed},
	}
}

func TestLocalActivityOfATaskThatNeverCompletedIsWithdrawnByTheNextWrite(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)
	// Task 2 completed. Task 3 runs a Local Activity that writes, then task 3 fails.
	_, err = h.appendRequest(localActivityWrite("call-1", 2, 0, "done"))
	require.NoError(t, err)
	_, err = h.appendRequest(localActivityWrite("call-2", 3, 2, "orphan"))
	require.NoError(t, err)
	// Task 4 takes another path and writes once.
	other, err := h.appendRequest(localActivityWrite("call-3", 4, 2, "other"))
	require.NoError(t, err)

	read, err := h.read(&streamservice.ReadRecordsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"done", "orphan", "superseded", "other"}, topics(read))
	require.Equal(t, read.GetRecords()[3].GetOffset(), other.GetFirstOffset())
	require.Equal(t, &streamservice.Supersession{
		ProducerKind:          streamservice.PRODUCER_KIND_WORKFLOW,
		ProducerId:            "wf:run",
		Epoch:                 &streamservice.ProducerEpoch{TaskStartedEventId: 4, TaskAttempt: 1, TaskStartedTime: timestamppb.New(time.Unix(104, 0))},
		PreviousCompletedTask: &streamservice.WorkflowTaskEpoch{StartedEventId: 2},
		RunId:                 "run",
	}, read.GetRecords()[2].GetSupersession())

	// A late write of the orphaned call is fenced.
	late := localActivityWrite("call-2", 3, 2, "late")
	late.Sequence = 2
	_, err = h.appendRequest(late)
	require.ErrorContains(t, err, ReasonFenced)

	// A later write that knows task 4 completed withdraws nothing more.
	_, err = h.appendRequest(localActivityWrite("call-4", 7, 4, "later"))
	require.NoError(t, err)
	read, err = h.read(&streamservice.ReadRecordsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"done", "orphan", "superseded", "other", "later"}, topics(read))
}

func TestFailedTaskBeforeTheLastCompletedOneIsWithdrawnByItsID(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)
	_, err = h.appendRequest(localActivityWrite("call-1", 3, 2, "orphan"))
	require.NoError(t, err)
	// Task 3 failed and task 4 completed with no write here. Task 5 knows both.
	next := localActivityWrite("call-2", 5, 4, "next")
	next.FailedTasks = []int64{3}
	_, err = h.appendRequest(next)
	require.NoError(t, err)

	read, err := h.read(&streamservice.ReadRecordsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"orphan", "superseded", "next"}, topics(read))
	require.Equal(t, []int64{3}, read.GetRecords()[1].GetSupersession().GetFailedTasks())
}

// A run terminated while a task is open never completes that task. The chain's end withdraws the
// run's Records of tasks after its last completed one, before the close, so a reader drops them.
func TestChainEndWithdrawsTheTasksThatNeverCompleted(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{
		StreamId: "chat-1", Owner: workflowProducer(1), CloseWithRun: true,
	})
	require.NoError(t, err)
	_, err = h.appendRequest(localActivityWrite("call-1", 3, 0, "kept"))
	require.NoError(t, err)
	_, err = h.appendRequest(localActivityWrite("call-2", 7, 3, "open"))
	require.NoError(t, err)

	end := RunEnd{RunID: "run", LastCompleted: 3, Status: streamservice.CLOSE_STATUS_TERMINATED}
	_, _, err = chasm.UpdateComponent(h.ctx, h.ref(),
		func(s *Stream, mctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			s.EndChain(mctx, end, h.limits)
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)

	read, err := h.read(&streamservice.ReadRecordsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"kept", "open", "superseded", "close"}, topics(read))
	completed := read.GetRecords()[2].GetSupersession().GetPreviousCompletedTask()
	require.Equal(t, int64(3), completed.GetStartedEventId())
	require.Equal(t, streamservice.CLOSE_STATUS_TERMINATED, read.GetRecords()[3].GetCloseStatus())
}

// Only the owner's end terminates or times out a Stream, so a caller can't forge either status.
func TestCallerCantCloseAsTerminated(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)
	_, _, err = chasm.UpdateComponent(h.ctx, h.ref(), (*Stream).Close, CloseInput{
		Request: &streamservice.CloseStreamRequest{
			Producer: activityProducer(1), Status: streamservice.CLOSE_STATUS_TERMINATED,
		},
		Limits: h.limits,
	})
	require.ErrorContains(t, err, ReasonInvalid)

	require.NoError(t, h.close("done"))
	read, err := h.read(&streamservice.ReadRecordsRequest{})
	require.NoError(t, err)
	require.Equal(t, streamservice.CLOSE_STATUS_COMPLETED, read.GetRecords()[0].GetCloseStatus())
}

// A wake_only read parks for hours, so an idle Workflow reader adds no History each minute. Any
// other parked read still answers an empty page after 60 s.
func TestWakeParksPastTheLongPollCap(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)
	for id, wakeOnly := range map[string]bool{"wake": true, "page": false} {
		resp, err := h.readOrPark(id, &streamservice.ReadRecordsRequest{
			WakeOnly: wakeOnly, Wait: durationpb.New(2 * time.Hour),
		})
		require.NoError(t, err)
		require.Nil(t, resp)
	}

	h.advance(2 * time.Minute)
	require.False(t, h.parked("wake").GetResolved())
	require.True(t, h.parked("page").GetResolved())

	_, err = h.append(activityProducer(1), 1, "a")
	require.NoError(t, err)
	require.True(t, h.parked("wake").GetResolved())
	require.Empty(t, h.parked("wake").GetResponse().GetRecords())
}

// A page stops before the Record that would take it past max_bytes, and a larger Record comes
// alone, so a reader with a small cap still moves on.
func TestPageStopsAtMaxBytes(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)
	_, err = h.appendRequest(&streamservice.AppendRecordsRequest{
		Producer: activityProducer(1), Sequence: 1, Records: []*streamservice.AppendRecord{
			{Topic: "a", Body: &commonpb.Payload{Data: make([]byte, 100)}},
			{Topic: "b", Body: &commonpb.Payload{Data: make([]byte, 100)}},
			{Topic: "big", Body: &commonpb.Payload{Data: make([]byte, 1000)}},
		},
	})
	require.NoError(t, err)

	first, err := h.read(&streamservice.ReadRecordsRequest{MaxBytes: 300})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, topics(first))
	second, err := h.read(&streamservice.ReadRecordsRequest{
		FromOffset: first.GetNextOffset(), MaxBytes: 300,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"big"}, topics(second))
}

// A last read gives the newest Records that pass the filter, oldest first, and leaves out the
// Records of a withdrawn Workflow Task.
func TestLastReadsTheNewestRecords(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)
	_, err = h.append(activityProducer(1), 1, "turns", "tools", "turns")
	require.NoError(t, err)
	_, err = h.append(workflowProducer(3), 1, "turns")
	require.NoError(t, err)
	_, err = h.appendRequest(&streamservice.AppendRecordsRequest{
		Producer: workflowProducer(7), Sequence: 1, Records: records("tools"),
		PreviousCompletedTask: &streamservice.WorkflowTaskEpoch{StartedEventId: 2, Attempt: 1},
		FailedTasks:           []int64{3},
	})
	require.NoError(t, err)

	last := func(req *streamservice.ReadRecordsRequest) []string {
		resp, err := chasm.ReadComponent(h.ctx, h.ref(), (*Stream).Last, req)
		require.NoError(t, err)
		return topics(resp)
	}
	require.Equal(t, []string{"turns", "tools"}, last(&streamservice.ReadRecordsRequest{Last: 2}))
	turns := &streamservice.ReadRecordsRequest{Last: 3, Topics: []string{"turns"}}
	require.Equal(t, []string{"turns", "turns"}, last(turns))

	require.NoError(t, h.close("done"))
	require.Equal(t, []string{"tools", "close"}, last(&streamservice.ReadRecordsRequest{Last: 1}))
	wake := &streamservice.ReadRecordsRequest{Last: 1, WakeOnly: true}
	require.ErrorContains(t, checkLast(wake, false), ReasonInvalid)
}

// A resend with the last sequence and other content is not a duplicate. Dropping it would lose
// the batch with no error, so it fails as seq_divergent. The same content answers its offsets.
func TestResendWithOtherContentIsDivergent(t *testing.T) {
	h := newHarness(t)
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)
	first, err := h.append(clientProducer(), 1, "a")
	require.NoError(t, err)

	again, err := h.append(clientProducer(), 1, "a")
	require.NoError(t, err)
	require.Equal(t, first.GetFirstOffset(), again.GetFirstOffset())
	_, err = h.append(clientProducer(), 1, "b")
	require.ErrorContains(t, err, ReasonSeqDivergent)
}

// The digest is the crate's, so the shared test vectors cover the server too.
func TestBatchDigestVector(t *testing.T) {
	digest := BatchDigest([]*streamservice.AppendRecord{{
		Topic: "a",
		Body: &commonpb.Payload{
			Metadata: map[string][]byte{"encoding": []byte("json/plain")},
			Data:     []byte(`"x"`),
		},
	}})
	require.Equal(t, "817ca6adadba99bd9a23764d62be9b40d78f1be310682bb85f208699f9c1f621",
		hex.EncodeToString(digest))
}

// At the Producer cap a Local Activity's entry goes first, since a call writes only while it
// runs. A client's older entry keeps its dedupe state, so its resend still goes in once.
func TestLocalActivityProducerGoesFirstAtTheCap(t *testing.T) {
	h := newHarness(t)
	h.limits.MaxProducers = 2
	_, err := h.create(&streamservice.CreateStreamRequest{StreamId: "chat-1"})
	require.NoError(t, err)
	client, err := h.append(clientProducer(), 1, "client")
	require.NoError(t, err)
	_, err = h.append(localActivityProducer(3), 1, "call")
	require.NoError(t, err)
	_, err = h.append(activityProducer(1), 1, "tool")
	require.NoError(t, err)

	again, err := h.append(clientProducer(), 1, "client")
	require.NoError(t, err)
	require.Equal(t, client.GetFirstOffset(), again.GetFirstOffset())
	read, err := h.read(&streamservice.ReadRecordsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"client", "call", "tool"}, topics(read))
}
