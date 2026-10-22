package tests

import (
	"testing"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/streamservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/chasm/lib/stream"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	sdkconverter "go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type StreamTestSuite struct {
	parallelsuite.Suite[*StreamTestSuite]
}

func TestStreamTestSuite(t *testing.T) {
	parallelsuite.Run(t, &StreamTestSuite{})
}

func (s *StreamTestSuite) newEnv() *testcore.TestEnv {
	return testcore.NewEnv(s.T(),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(stream.Enabled, true),
	)
}

// streamCaller drives a Workflow by hand, since the SDK refuses the __temporal_system endpoint.
// Each step completes the open Workflow Task with commands and polls the next one.
type streamCaller struct {
	s         *StreamTestSuite
	env       *testcore.TestEnv
	taskQueue string
	token     []byte
	history   []*historypb.HistoryEvent
	run       client.WorkflowRun
}

func (s *StreamTestSuite) newCaller(env *testcore.TestEnv) *streamCaller {
	c := &streamCaller{s: s, env: env, taskQueue: testcore.RandomizeStr(s.T().Name())}
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), client.StartWorkflowOptions{TaskQueue: c.taskQueue}, "stream-caller")
	s.NoError(err)
	c.run = run
	s.T().Cleanup(func() {
		_ = env.SdkClient().TerminateWorkflow(s.Context(), run.GetID(), run.GetRunID(), "test cleanup")
	})
	c.poll()
	return c
}

func (c *streamCaller) poll() {
	resp, err := c.env.FrontendClient().PollWorkflowTaskQueue(c.s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: c.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: c.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	c.s.NoError(err)
	c.s.NotEmpty(resp.GetTaskToken())
	c.token = resp.GetTaskToken()
	c.history = resp.GetHistory().GetEvents()
}

// schedule completes the open task with one System Nexus operation per request and answers the
// scheduled event ids, in the order of the requests.
func (c *streamCaller) schedule(ops ...streamOp) []int64 {
	last := c.history[len(c.history)-1].GetEventId()
	commands := make([]*commandpb.Command, 0, len(ops))
	for _, op := range ops {
		commands = append(commands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  commonnexus.SystemEndpoint,
					Service:   stream.ServiceName,
					Operation: op.name,
					Input:     payloads.MustEncodeSingle(op.request),
				},
			},
		})
	}
	c.respond(commands...)
	var ids []int64
	for _, event := range c.history {
		if event.GetEventId() > last && event.GetNexusOperationScheduledEventAttributes() != nil {
			ids = append(ids, event.GetEventId())
		}
	}
	c.s.Len(ids, len(ops))
	return ids
}

func (c *streamCaller) respond(commands ...*commandpb.Command) {
	_, err := c.env.FrontendClient().RespondWorkflowTaskCompleted(c.s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: c.token,
		Commands:  commands,
	})
	c.s.NoError(err)
	c.poll()
}

// outcome waits for the end of the operation that scheduledID scheduled. It answers whether the
// operation started async first.
func (c *streamCaller) outcome(scheduledID int64, result proto.Message) (*failurepb.Failure, bool) {
	for {
		started := false
		for _, event := range c.history {
			if attrs := event.GetNexusOperationStartedEventAttributes(); attrs != nil && attrs.GetScheduledEventId() == scheduledID {
				started = true
			}
			if attrs := event.GetNexusOperationCompletedEventAttributes(); attrs != nil && attrs.GetScheduledEventId() == scheduledID {
				c.s.NoError(sdkconverter.PreferProtoDataConverter.FromPayloads(
					&commonpb.Payloads{Payloads: []*commonpb.Payload{attrs.GetResult()}}, result))
				return nil, started
			}
			if attrs := event.GetNexusOperationFailedEventAttributes(); attrs != nil && attrs.GetScheduledEventId() == scheduledID {
				return attrs.GetFailure(), started
			}
		}
		c.respond()
	}
}

func (c *streamCaller) call(op streamOp, result proto.Message) *failurepb.Failure {
	failure, _ := c.outcome(c.schedule(op)[0], result)
	return failure
}

type streamOp struct {
	name    string
	request proto.Message
}

func ref(id string) *streamservice.StreamRef {
	return &streamservice.StreamRef{Id: id}
}

func records(topics ...string) []*streamservice.AppendRecord {
	out := make([]*streamservice.AppendRecord, 0, len(topics))
	for _, topic := range topics {
		out = append(out, &streamservice.AppendRecord{Topic: topic, Body: payload.EncodeString(topic)})
	}
	return out
}

func kindsAndTopics(resp *streamservice.ReadRecordsResponse) []string {
	var out []string
	for _, record := range resp.GetRecords() {
		if record.GetKind() == streamservice.STREAM_RECORD_KIND_CLOSE {
			out = append(out, "close")
			continue
		}
		out = append(out, record.GetTopic())
	}
	return out
}

// causeType is the reason in the StreamErrorDetails under the Nexus operation failure.
func causeType(failure *failurepb.Failure) string {
	for f := failure; f != nil; f = f.GetCause() {
		if details := f.GetApplicationFailureInfo().GetDetails(); details != nil {
			var d streamservice.StreamErrorDetails
			if sdkconverter.PreferProtoDataConverter.FromPayloads(details, &d) == nil {
				return d.GetReason()
			}
		}
	}
	return ""
}

// workflowWriter is the Producer of a Workflow run that writes through the Core extension.
func workflowWriter(runID string, started int64) *streamservice.Producer {
	return &streamservice.Producer{
		Kind:  streamservice.PRODUCER_KIND_WORKFLOW,
		Id:    "wf:" + runID,
		Epoch: &streamservice.ProducerEpoch{TaskStartedEventId: started, TaskAttempt: 1},
		RunId: runID,
	}
}

func (s *StreamTestSuite) TestWorkflowReadAndDescribe() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	ctx := s.Context()
	ns := env.Namespace().String()
	id := testcore.RandomizeStr("chat")
	writer := workflowWriter("run-1", 3)

	_, err := sc.CreateStream(ctx, &streamservice.CreateStreamRequest{Namespace: ns, StreamId: id, Owner: writer})
	s.NoError(err)
	appended, err := sc.AppendRecords(ctx, &streamservice.AppendRecordsRequest{
		Namespace: ns, Stream: ref(id), Producer: writer, Sequence: 1, Records: records("turns", "tools"),
	})
	s.NoError(err)

	var page streamservice.ReadRecordsResponse
	failure, started := c.outcome(c.schedule(streamOp{"ReadRecords", &streamservice.ReadRecordsRequest{Stream: ref(id)}})[0], &page)
	s.Nil(failure)
	s.False(started)
	s.Equal([]string{"turns", "tools"}, kindsAndTopics(&page))

	var described streamservice.DescribeStreamResponse
	s.Nil(c.call(streamOp{"DescribeStream", &streamservice.DescribeStreamRequest{Stream: ref(id)}}, &described))
	s.Equal(appended.GetLastOffset(), described.GetLastOffset())

	_, err = sc.CloseStream(ctx, &streamservice.CloseStreamRequest{Namespace: ns, Stream: ref(id), Producer: writer, Result: payload.EncodeString("done")})
	s.NoError(err)
	var last streamservice.ReadRecordsResponse
	s.Nil(c.call(streamOp{"ReadRecords", &streamservice.ReadRecordsRequest{Stream: ref(id), FromOffset: page.GetNextOffset()}}, &last))
	s.Equal([]string{"close"}, kindsAndTopics(&last))
	s.True(last.GetDone())
}

func (s *StreamTestSuite) TestWorkflowReadWaitsForAnAppend() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	ns := env.Namespace().String()
	id := testcore.RandomizeStr("chat")
	_, err := sc.CreateStream(s.Context(), &streamservice.CreateStreamRequest{Namespace: ns, StreamId: id})
	s.NoError(err)

	read := c.schedule(streamOp{"ReadRecords", &streamservice.ReadRecordsRequest{Stream: ref(id), Topics: []string{"turns"}}})[0]
	for i, topic := range []string{"other", "turns"} {
		_, err = sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
			Namespace: ns, Stream: ref(id), Producer: clientProducer(), Sequence: int64(i + 1), Records: records(topic),
		})
		s.NoError(err)
	}

	var page streamservice.ReadRecordsResponse
	failure, started := c.outcome(read, &page)
	s.Nil(failure)
	s.True(started)
	s.Equal([]string{"turns"}, kindsAndTopics(&page))
}

func (s *StreamTestSuite) TestWorkflowReadEndsOnAutoClose() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	id := testcore.RandomizeStr("chat")
	_, err := sc.CreateStream(s.Context(), &streamservice.CreateStreamRequest{
		Namespace: env.Namespace().String(), StreamId: id, AutoClose: durationpb.New(2 * time.Second),
	})
	s.NoError(err)

	var page streamservice.ReadRecordsResponse
	failure, started := c.outcome(c.schedule(streamOp{"ReadRecords", &streamservice.ReadRecordsRequest{Stream: ref(id)}})[0], &page)
	s.Nil(failure)
	s.True(started)
	s.Equal([]string{"close"}, kindsAndTopics(&page))
	s.True(page.GetDone())
}

func (s *StreamTestSuite) TestWorkflowReadOfAMissingStreamFails() {
	env := s.newEnv()
	c := s.newCaller(env)
	failure := c.call(streamOp{"ReadRecords", &streamservice.ReadRecordsRequest{Stream: ref(testcore.RandomizeStr("missing"))}},
		&streamservice.ReadRecordsResponse{})
	s.Equal(stream.ReasonNotFound, causeType(failure))
}

func (s *StreamTestSuite) streamClient(env *testcore.TestEnv) streamservice.StreamServiceClient {
	conn, err := grpc.NewClient(env.FrontendGRPCAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.NoError(err)
	s.T().Cleanup(func() { _ = conn.Close() })
	return streamservice.NewStreamServiceClient(conn)
}

func clientProducer() *streamservice.Producer {
	return &streamservice.Producer{
		Kind:  streamservice.PRODUCER_KIND_CLIENT,
		Id:    "client:test:1",
		Epoch: &streamservice.ProducerEpoch{Attempt: 1},
	}
}

func (s *StreamTestSuite) TestClientCreateAppendReadClose() {
	env := s.newEnv()
	sc := s.streamClient(env)
	ctx := s.Context()
	ns := env.Namespace().String()
	id := testcore.RandomizeStr("chat")

	created, err := sc.CreateStream(ctx, &streamservice.CreateStreamRequest{Namespace: ns, StreamId: id})
	s.NoError(err)
	s.True(created.GetCreated())
	generated, err := sc.CreateStream(ctx, &streamservice.CreateStreamRequest{Namespace: ns})
	s.NoError(err)
	s.NotEmpty(generated.GetStream().GetId())

	appended, err := sc.AppendRecords(ctx, &streamservice.AppendRecordsRequest{
		Namespace: ns, Stream: ref(id), Producer: clientProducer(), Sequence: 1, Records: records("turns", "tool/search"),
	})
	s.NoError(err)
	resent, err := sc.AppendRecords(ctx, &streamservice.AppendRecordsRequest{
		Namespace: ns, Stream: ref(id), Producer: clientProducer(), Sequence: 1, Records: records("turns", "tool/search"),
	})
	s.NoError(err)
	s.Equal(appended.GetFirstOffset(), resent.GetFirstOffset())

	page, err := sc.ReadRecords(ctx, &streamservice.ReadRecordsRequest{Namespace: ns, Stream: ref(id), TopicPrefixes: []string{"tool/"}})
	s.NoError(err)
	s.Equal([]string{"tool/search"}, kindsAndTopics(page))
	s.Equal(appended.GetLastOffset()+1, page.GetNextOffset())

	described, err := sc.DescribeStream(ctx, &streamservice.DescribeStreamRequest{Namespace: ns, Stream: ref(id)})
	s.NoError(err)
	s.Equal(streamservice.STREAM_STATUS_OPEN, described.GetStatus())
	s.Equal(appended.GetLastOffset(), described.GetLastOffset())

	_, err = sc.CloseStream(ctx, &streamservice.CloseStreamRequest{Namespace: ns, Stream: ref(id), Producer: clientProducer()})
	s.NoError(err)
	last, err := sc.ReadRecords(ctx, &streamservice.ReadRecordsRequest{Namespace: ns, Stream: ref(id), FromOffset: page.GetNextOffset()})
	s.NoError(err)
	s.Equal([]string{"close"}, kindsAndTopics(last))
	s.True(last.GetDone())

	_, err = sc.AppendRecords(ctx, &streamservice.AppendRecordsRequest{
		Namespace: ns, Stream: ref(id), Producer: clientProducer(), Sequence: 2, Records: records("late"),
	})
	st := status.Convert(err)
	s.Equal(codes.FailedPrecondition, st.Code())
	s.Len(st.Details(), 1)
	s.Equal(stream.ReasonClosed, st.Details()[0].(*streamservice.StreamErrorDetails).GetReason())
}

func (s *StreamTestSuite) TestClientReadWaitsForAnAppend() {
	env := s.newEnv()
	sc := s.streamClient(env)
	ctx := s.Context()
	ns := env.Namespace().String()
	id := testcore.RandomizeStr("chat")
	_, err := sc.CreateStream(ctx, &streamservice.CreateStreamRequest{Namespace: ns, StreamId: id})
	s.NoError(err)

	type result struct {
		resp *streamservice.ReadRecordsResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := sc.ReadRecords(ctx, &streamservice.ReadRecordsRequest{
			Namespace: ns, Stream: ref(id), Start: streamservice.READ_START_END, Wait: durationpb.New(30 * time.Second),
		})
		done <- result{resp, err}
	}()
	// The read may start after an append, so each attempt appends a new batch.
	var sequence int64
	s.Await(func(s *StreamTestSuite) {
		sequence++
		_, err := sc.AppendRecords(ctx, &streamservice.AppendRecordsRequest{
			Namespace: ns, Stream: ref(id), Producer: clientProducer(), Sequence: sequence, Records: records("turns"),
		})
		s.NoError(err)
		select {
		case r := <-done:
			s.NoError(r.err)
			s.Equal([]string{"turns"}, kindsAndTopics(r.resp))
		case <-time.After(time.Second):
			s.Fail("the read did not end")
		}
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *StreamTestSuite) TestClientReadWaitsForTheCreate() {
	env := s.newEnv()
	sc := s.streamClient(env)
	ctx := s.Context()
	ns := env.Namespace().String()
	id := testcore.RandomizeStr("chat")

	_, err := sc.ReadRecords(ctx, &streamservice.ReadRecordsRequest{Namespace: ns, Stream: ref(id)})
	s.Equal(codes.NotFound, status.Code(err))

	done := make(chan error, 1)
	go func() {
		_, err := sc.ReadRecords(ctx, &streamservice.ReadRecordsRequest{
			Namespace: ns, Stream: ref(id), CreateWait: durationpb.New(30 * time.Second),
		})
		done <- err
	}()
	_, err = sc.CreateStream(ctx, &streamservice.CreateStreamRequest{Namespace: ns, StreamId: id})
	s.NoError(err)
	s.NoError(<-done)
}

func (s *StreamTestSuite) TestClientReadEndsOnAutoClose() {
	env := s.newEnv()
	sc := s.streamClient(env)
	ctx := s.Context()
	ns := env.Namespace().String()
	id := testcore.RandomizeStr("chat")
	_, err := sc.CreateStream(ctx, &streamservice.CreateStreamRequest{Namespace: ns, StreamId: id, AutoClose: durationpb.New(time.Second)})
	s.NoError(err)

	page, err := sc.ReadRecords(ctx, &streamservice.ReadRecordsRequest{Namespace: ns, Stream: ref(id), Wait: durationpb.New(30 * time.Second)})
	s.NoError(err)
	s.Equal([]string{"close"}, kindsAndTopics(page))
	s.True(page.GetDone())

	described, err := sc.DescribeStream(ctx, &streamservice.DescribeStreamRequest{Namespace: ns, Stream: ref(id)})
	s.NoError(err)
	s.Equal(streamservice.STREAM_STATUS_CLOSED, described.GetStatus())
}

func (s *StreamTestSuite) TestWorkflowReadWaitsForTheCreate() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	ns := env.Namespace().String()
	id := testcore.RandomizeStr("chat")

	read := c.schedule(streamOp{"ReadRecords", &streamservice.ReadRecordsRequest{
		Stream:     ref(id),
		CreateWait: durationpb.New(30 * time.Second),
	}})[0]
	_, err := sc.CreateStream(s.Context(), &streamservice.CreateStreamRequest{Namespace: ns, StreamId: id})
	s.NoError(err)
	_, err = sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
		Namespace: ns, Stream: ref(id), Producer: clientProducer(), Sequence: 1, Records: records("turns"),
	})
	s.NoError(err)

	var page streamservice.ReadRecordsResponse
	failure, started := c.outcome(read, &page)
	s.Nil(failure)
	s.True(started)
	s.Equal([]string{"turns"}, kindsAndTopics(&page))
}

// closeWithRun creates a Stream that closes with the caller's run chain.
func (s *StreamTestSuite) closeWithRun(sc streamservice.StreamServiceClient, env *testcore.TestEnv, c *streamCaller) *streamservice.StreamRef {
	id := testcore.RandomizeStr("chat")
	_, err := sc.CreateStream(s.Context(), &streamservice.CreateStreamRequest{
		Namespace:    env.Namespace().String(),
		StreamId:     id,
		Owner:        workflowWriter(c.run.GetRunID(), 3),
		Ref:          &streamservice.StreamRef{Id: id, WorkflowId: c.run.GetID(), FirstExecutionRunId: c.run.GetRunID()},
		CloseWithRun: true,
	})
	s.NoError(err)
	return &streamservice.StreamRef{Id: id, WorkflowId: c.run.GetID(), FirstExecutionRunId: c.run.GetRunID()}
}

func (s *StreamTestSuite) readClose(sc streamservice.StreamServiceClient, env *testcore.TestEnv, owned *streamservice.StreamRef) *streamservice.StreamRecord {
	page, err := sc.ReadRecords(s.Context(), &streamservice.ReadRecordsRequest{
		Namespace: env.Namespace().String(), Stream: owned, Wait: durationpb.New(20 * time.Second),
	})
	s.NoError(err)
	s.Equal([]string{"close"}, kindsAndTopics(page))
	return page.GetRecords()[0]
}

func (s *StreamTestSuite) TestStreamClosesWithTheOwnerResult() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	owned := s.closeWithRun(sc, env, c)

	_, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: c.token,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("answer"),
				},
			},
		}},
	})
	s.NoError(err)

	s.Equal(payload.EncodeString("answer").GetData(), s.readClose(sc, env, owned).GetCloseResult().GetData())
}

func (s *StreamTestSuite) TestStreamClosesWithATerminatedOwner() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	owned := s.closeWithRun(sc, env, c)

	s.NoError(env.SdkClient().TerminateWorkflow(s.Context(), c.run.GetID(), c.run.GetRunID(), "stop"))

	failure := s.readClose(sc, env, owned).GetCloseFailure()
	s.NotNil(failure.GetTerminatedFailureInfo())
}

func (s *StreamTestSuite) TestDeferredWorkflowBatchIsWithdrawnByAFailedTask() {
	env := s.newEnv()
	sc := s.streamClient(env)
	ctx := s.Context()
	ns := env.Namespace().String()
	id := testcore.RandomizeStr("chat")
	_, err := sc.CreateStream(ctx, &streamservice.CreateStreamRequest{Namespace: ns, StreamId: id})
	s.NoError(err)

	// Task 3 writes before a Local Activity, with no task facts yet, then fails.
	_, err = sc.AppendRecords(ctx, &streamservice.AppendRecordsRequest{
		Namespace: ns, Stream: ref(id), Producer: workflowWriter("run-1", 3), Sequence: 1,
		Records: records("a"), DeferWithdrawal: true,
	})
	s.NoError(err)
	_, err = sc.AppendRecords(ctx, &streamservice.AppendRecordsRequest{
		Namespace: ns, Stream: ref(id), Producer: workflowWriter("run-1", 7), Sequence: 1, Records: records("b"),
		PreviousCompletedTask: &streamservice.WorkflowTaskEpoch{StartedEventId: 2, Attempt: 1},
		FailedTasks:           []int64{3},
	})
	s.NoError(err)

	page, err := sc.ReadRecords(ctx, &streamservice.ReadRecordsRequest{Namespace: ns, Stream: ref(id)})
	s.NoError(err)
	s.Equal([]string{"a", "", "b"}, kindsAndTopics(page))
	s.Equal(streamservice.STREAM_RECORD_KIND_SUPERSEDED, page.GetRecords()[1].GetKind())
	s.Equal([]int64{3}, page.GetRecords()[1].GetSupersession().GetFailedTasks())
}

func (s *StreamTestSuite) TestWorkflowReadEndsAtItsWait() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	ns := env.Namespace().String()
	id := testcore.RandomizeStr("chat")
	_, err := sc.CreateStream(s.Context(), &streamservice.CreateStreamRequest{Namespace: ns, StreamId: id})
	s.NoError(err)
	appended, err := sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
		Namespace: ns, Stream: ref(id), Producer: clientProducer(), Sequence: 1, Records: records("other"),
	})
	s.NoError(err)

	var page streamservice.ReadRecordsResponse
	failure, started := c.outcome(c.schedule(streamOp{"ReadRecords", &streamservice.ReadRecordsRequest{
		Stream: ref(id), Topics: []string{"turns"}, Wait: durationpb.New(time.Second),
	}})[0], &page)
	s.Nil(failure)
	s.True(started)
	s.Empty(page.GetRecords())
	s.False(page.GetDone())
	s.Equal(appended.GetLastOffset()+1, page.GetNextOffset())
}

// An append to a Workflow Stream completes the Workflow's parked read in the Workflow's
// transaction, since the Stream nests in the Workflow.
func (s *StreamTestSuite) TestNestedStreamWakesItsWorkflow() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	owned := s.closeWithRun(sc, env, c)

	read := c.schedule(streamOp{"ReadRecords", &streamservice.ReadRecordsRequest{Stream: owned}})[0]
	_, err := sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
		Namespace: env.Namespace().String(), Stream: owned,
		Producer: workflowWriter(c.run.GetRunID(), 3), Sequence: 1, Records: records("turns"),
	})
	s.NoError(err)

	var page streamservice.ReadRecordsResponse
	failure, started := c.outcome(read, &page)
	s.Nil(failure)
	s.True(started)
	s.Equal([]string{"turns"}, kindsAndTopics(&page))

	// The Stream id names no execution of its own.
	_, err = sc.DescribeStream(s.Context(), &streamservice.DescribeStreamRequest{Namespace: env.Namespace().String(), Stream: ref(owned.GetId())})
	s.Equal(codes.NotFound, status.Code(err))
}

func (s *StreamTestSuite) TestStreamClosesWithTheLastRunOfTheChain() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	owned := s.closeWithRun(sc, env, c)

	// Run 1 continues as new. The Stream stays open in run 1's tree.
	c.respond(&commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
			ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: "stream-caller"},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: c.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			},
		},
	})
	described, err := sc.DescribeStream(s.Context(), &streamservice.DescribeStreamRequest{
		Namespace: env.Namespace().String(), Stream: owned,
	})
	s.NoError(err)
	s.Equal(streamservice.STREAM_STATUS_OPEN, described.GetStatus())

	// Run 2 completes, which ends the chain.
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: c.token,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("run-2"),
				},
			},
		}},
	})
	s.NoError(err)
	s.Equal(payload.EncodeString("run-2").GetData(), s.readClose(sc, env, owned).GetCloseResult().GetData())
}
