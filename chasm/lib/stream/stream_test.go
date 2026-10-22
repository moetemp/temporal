package stream

import (
	"context"
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
	require.NoError(t, registry.Register(newLibrary(config, nil, nil, newTimerTaskHandler(config), nil)))
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
			Producer: activityProducer(1),
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
