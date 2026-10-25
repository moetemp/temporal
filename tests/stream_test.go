package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
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

func (s *StreamTestSuite) newEnv(opts ...testcore.TestOption) *testcore.TestEnv {
	return testcore.NewEnv(s.T(), append([]testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(stream.Enabled, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableDirectNexusCalls, true),
	}, opts...)...)
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
	// The run of the open Workflow Task, which a continue-as-new or a cron run changes.
	current string
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
	c.current = resp.GetWorkflowExecution().GetRunId()
}

// schedule completes the open task with one System Nexus operation per request and answers the
// scheduled event ids, in the order of the requests.
func (c *streamCaller) schedule(ops ...streamOp) []int64 {
	last := c.history[len(c.history)-1].GetEventId()
	commands := make([]*commandpb.Command, 0, len(ops))
	for _, op := range ops {
		commands = append(commands, nexusCommand(op))
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

// nexusCommand schedules op as a System Nexus operation.
func nexusCommand(op streamOp) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
		Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
			ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
				Endpoint:  commonnexus.SystemEndpoint,
				Service:   stream.ServiceName,
				Operation: op.name,
				Input:     payloads.MustEncodeSingle(op.request),
			},
		},
	}
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

	_, err := sc.CreateStream(ctx, &streamservice.CreateStreamRequest{Namespace: ns, StreamId: id})
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

	_, err = sc.CloseStream(ctx, &streamservice.CloseStreamRequest{Namespace: ns, Stream: ref(id), Producer: clientProducer(), Result: payload.EncodeString("done")})
	s.NoError(err)
	var last streamservice.ReadRecordsResponse
	s.Nil(c.call(streamOp{"ReadRecords", &streamservice.ReadRecordsRequest{Stream: ref(id), FromOffset: page.GetNextOffset()}}, &last))
	s.Equal([]string{"close"}, kindsAndTopics(&last))
	s.True(last.GetDone())
}

// A Workflow's wake parks until a Record passes its filter, and answers with no Records.
func (s *StreamTestSuite) TestWorkflowWakeWaitsForAnAppend() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	ns := env.Namespace().String()
	id := testcore.RandomizeStr("chat")
	_, err := sc.CreateStream(s.Context(), &streamservice.CreateStreamRequest{Namespace: ns, StreamId: id})
	s.NoError(err)

	read := c.schedule(streamOp{"ReadRecords", &streamservice.ReadRecordsRequest{
		Stream: ref(id), Topics: []string{"turns"}, WakeOnly: true,
	}})[0]
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
	s.Empty(page.GetRecords())
	s.False(page.GetDone())
}

func (s *StreamTestSuite) TestWorkflowWakeEndsOnClose() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	ns := env.Namespace().String()
	id := testcore.RandomizeStr("chat")
	_, err := sc.CreateStream(s.Context(), &streamservice.CreateStreamRequest{Namespace: ns, StreamId: id})
	s.NoError(err)

	read := c.schedule(streamOp{"ReadRecords", &streamservice.ReadRecordsRequest{
		Stream: ref(id), WakeOnly: true,
	}})[0]
	_, err = sc.CloseStream(s.Context(), &streamservice.CloseStreamRequest{Namespace: ns, Stream: ref(id), Producer: clientProducer()})
	s.NoError(err)

	var page streamservice.ReadRecordsResponse
	failure, started := c.outcome(read, &page)
	s.Nil(failure)
	s.True(started)
	s.Empty(page.GetRecords())
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

func (s *StreamTestSuite) TestWorkflowWakeWaitsForTheCreate() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	ns := env.Namespace().String()
	id := testcore.RandomizeStr("chat")

	read := c.schedule(streamOp{"ReadRecords", &streamservice.ReadRecordsRequest{
		Stream:     ref(id),
		WakeOnly:   true,
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
	s.Empty(page.GetRecords())
}

// A wake whose wait for the create ends first leaves the Workflow Stream's node abandoned. The
// node has no execution of its own to replace, so a later wake parks again and the create opens
// it.
func (s *StreamTestSuite) TestAbandonedWorkflowStreamCanStillBeCreated() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	ns := env.Namespace().String()
	id := testcore.RandomizeStr("chat")
	owned := &streamservice.StreamRef{
		Id: id, WorkflowId: c.run.GetID(), FirstExecutionRunId: c.run.GetRunID(),
	}

	failure := c.call(streamOp{"ReadRecords", &streamservice.ReadRecordsRequest{
		Stream: owned, WakeOnly: true,
		CreateWait: durationpb.New(time.Second), Wait: durationpb.New(time.Hour),
	}}, &streamservice.ReadRecordsResponse{})
	s.Equal(stream.ReasonNotFound, causeType(failure))

	wake := c.schedule(streamOp{"ReadRecords", &streamservice.ReadRecordsRequest{
		Stream: owned, WakeOnly: true, CreateWait: durationpb.New(30 * time.Second),
	}})[0]
	_, err := sc.CreateStream(s.Context(), &streamservice.CreateStreamRequest{
		Namespace: ns, StreamId: id, Owner: workflowWriter(c.current, 3), Ref: owned,
		CloseWithRun: true,
	})
	s.NoError(err)
	_, err = sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
		Namespace: ns, Stream: owned, Producer: clientProducer(), Sequence: 1,
		Records: records("turns"),
	})
	s.NoError(err)
	failure, started := c.outcome(wake, &streamservice.ReadRecordsResponse{})
	s.Nil(failure)
	s.True(started)
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

	closed := s.readClose(sc, env, owned)
	s.Equal(payload.EncodeString("answer").GetData(), closed.GetCloseResult().GetData())
	s.Equal(streamservice.CLOSE_STATUS_COMPLETED, closed.GetCloseStatus())
}

func (s *StreamTestSuite) TestStreamClosesWithATerminatedOwner() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	owned := s.closeWithRun(sc, env, c)

	s.NoError(env.SdkClient().TerminateWorkflow(s.Context(), c.run.GetID(), c.run.GetRunID(), "stop"))

	closed := s.readClose(sc, env, owned)
	s.NotNil(closed.GetCloseFailure().GetTerminatedFailureInfo())
	s.Equal(streamservice.CLOSE_STATUS_TERMINATED, closed.GetCloseStatus())
}

// closeOp closes a Workflow Stream through System Nexus, as the calling run.
func closeOp(owned *streamservice.StreamRef, result string) streamOp {
	return streamOp{"CloseStream", &streamservice.CloseStreamRequest{Stream: owned, Result: payload.EncodeString(result)}}
}

func (s *StreamTestSuite) TestWorkflowCloseCommandClosesItsStream() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	owned := s.closeWithRun(sc, env, c)

	s.Nil(c.call(closeOp(owned, "answer"), &streamservice.CloseStreamResponse{}))

	closed := s.readClose(sc, env, owned)
	s.Equal(payload.EncodeString("answer").GetData(), closed.GetCloseResult().GetData())
	s.Equal(streamservice.CLOSE_STATUS_COMPLETED, closed.GetCloseStatus())
	s.Equal("wf:"+stream.EncodeIDPart(c.run.GetRunID()), closed.GetProducer().GetId())
}

func (s *StreamTestSuite) TestCloseCommandOfAnotherChainIsRefused() {
	env := s.newEnv()
	sc := s.streamClient(env)
	owned := s.closeWithRun(sc, env, s.newCaller(env))

	failure := s.newCaller(env).call(closeOp(owned, "answer"), &streamservice.CloseStreamResponse{})
	s.Equal(stream.ReasonNotOwner, causeType(failure))

	described, err := sc.DescribeStream(s.Context(), &streamservice.DescribeStreamRequest{Namespace: env.Namespace().String(), Stream: owned})
	s.NoError(err)
	s.Equal(streamservice.STREAM_STATUS_OPEN, described.GetStatus())
}

// A direct System Nexus call waits for no Workflow Task to commit, so it can't close the Stream,
// even with the link of the owner run.
func (s *StreamTestSuite) TestDirectCloseCallIsRefused() {
	env := s.newEnv()
	sc := s.streamClient(env)
	ns := env.Namespace().String()
	c := s.newCaller(env)
	owned := s.closeWithRun(sc, env, c)

	resp, err := env.FrontendClient().StartNexusOperation(s.Context(), &workflowservice.StartNexusOperationRequest{
		Namespace: ns,
		Endpoint:  commonnexus.SystemEndpoint,
		Service:   stream.ServiceName,
		Operation: "CloseStream",
		Input:     payloads.MustEncodeSingle(closeOp(owned, "answer").request),
		Links:     []*commonpb.Link{readerLink(env, c.run)},
	})
	s.NoError(err)
	s.Equal(stream.ReasonInvalid, causeType(resp.GetOperationFailure().GetFailure()))

	described, err := sc.DescribeStream(s.Context(), &streamservice.DescribeStreamRequest{Namespace: ns, Stream: owned})
	s.NoError(err)
	s.Equal(streamservice.STREAM_STATUS_OPEN, described.GetStatus())
}

// A bad command fails the Workflow Task, so the server drops the close command that came with it.
// The retried task's close command closes the Stream once.
func (s *StreamTestSuite) TestCloseCommandOfAFailedWorkflowTaskIsDropped() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	owned := s.closeWithRun(sc, env, c)

	_, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: c.token,
		Commands: []*commandpb.Command{
			nexusCommand(closeOp(owned, "failed task")),
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{},
				},
			},
		},
	})
	s.Error(err)
	c.poll()

	s.Nil(c.call(closeOp(owned, "retried task"), &streamservice.CloseStreamResponse{}))
	page, err := sc.ReadRecords(s.Context(), &streamservice.ReadRecordsRequest{Namespace: env.Namespace().String(), Stream: owned})
	s.NoError(err)
	s.Equal([]string{"close"}, kindsAndTopics(page))
	s.Equal(payload.EncodeString("retried task").GetData(), page.GetRecords()[0].GetCloseResult().GetData())
}

// A cron run's end isn't the chain's end, so the next cron run writes on. The terminate of a later
// run ends the chain and closes the Stream with its status.
func (s *StreamTestSuite) TestCronRunEndKeepsTheStreamOpen() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := &streamCaller{s: s, env: env, taskQueue: testcore.RandomizeStr(s.T().Name())}
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), client.StartWorkflowOptions{
		TaskQueue: c.taskQueue, CronSchedule: "@every 1s",
	}, "stream-caller")
	s.NoError(err)
	c.run = run
	s.T().Cleanup(func() {
		_ = env.SdkClient().TerminateWorkflow(s.Context(), run.GetID(), "", "test cleanup")
	})
	c.poll()
	owned := s.closeWithRun(sc, env, c)

	// Run 1 completes and the next cron run starts.
	c.respond(&commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.
				CompleteWorkflowExecutionCommandAttributes{},
		},
	})
	appended, err := sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
		Namespace: env.Namespace().String(), Stream: owned,
		Producer: workflowWriter(c.current, 3), Sequence: 1, Records: records("next"),
	})
	s.NoError(err)

	s.NoError(env.SdkClient().TerminateWorkflow(s.Context(), run.GetID(), "", "stop"))
	page, err := sc.ReadRecords(s.Context(), &streamservice.ReadRecordsRequest{
		Namespace: env.Namespace().String(), Stream: owned,
		FromOffset: appended.GetLastOffset() + 1, Wait: durationpb.New(20 * time.Second),
	})
	s.NoError(err)
	// Run 2's task never completed, so the chain's end withdraws its Record before the close.
	s.Equal([]string{"", "close"}, kindsAndTopics(page))
	s.Equal(streamservice.STREAM_RECORD_KIND_SUPERSEDED, page.GetRecords()[0].GetKind())
	s.Equal(streamservice.CLOSE_STATUS_TERMINATED, page.GetRecords()[1].GetCloseStatus())
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

// A Workflow read that isn't a wake never parks. It answers what the Stream has at once, an
// empty page here, so no parked read keeps a page.
func (s *StreamTestSuite) TestWorkflowReadThatIsNotAWakeAnswersAtOnce() {
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
		Stream: ref(id), Topics: []string{"turns"}, Wait: durationpb.New(time.Minute),
	}})[0], &page)
	s.Nil(failure)
	s.False(started)
	s.Empty(page.GetRecords())
	s.False(page.GetDone())
	s.Equal(appended.GetLastOffset()+1, page.GetNextOffset())
}

// One large append must not copy its batch into every waiting read, which would push the
// execution past the mutable state limit and expire the Stream.
func (s *StreamTestSuite) TestLargeAppendWithManyReadersStaysUnderTheStateLimit() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	ns := env.Namespace().String()
	id := testcore.RandomizeStr("chat")
	_, err := sc.CreateStream(s.Context(), &streamservice.CreateStreamRequest{Namespace: ns, StreamId: id})
	s.NoError(err)

	ops := make([]streamOp, 0, 5)
	for range 5 {
		ops = append(ops, streamOp{"ReadRecords", &streamservice.ReadRecordsRequest{
			Stream: ref(id), Start: streamservice.READ_START_END, Wait: durationpb.New(time.Minute),
		}})
	}
	c.schedule(ops...)

	big := make([]byte, 1800*1024)
	_, err = sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
		Namespace: ns, Stream: ref(id), Producer: clientProducer(), Sequence: 1,
		Records: []*streamservice.AppendRecord{{Topic: "big", Body: &commonpb.Payload{Data: big}}},
	})
	s.NoError(err)
	// The size check runs in the next transition, such as a wake's callback task.
	time.Sleep(2 * time.Second) //nolint:forbidigo // The callback tasks run on real time.
	described, err := sc.DescribeStream(s.Context(),
		&streamservice.DescribeStreamRequest{Namespace: ns, Stream: ref(id)})
	s.NoError(err)
	s.Equal(streamservice.STREAM_STATUS_OPEN, described.GetStatus())
}

// An append to a Workflow Stream completes the Workflow's parked read in the Workflow's
// transaction, since the Stream nests in the Workflow.
func (s *StreamTestSuite) TestNestedStreamWakesItsWorkflow() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	owned := s.closeWithRun(sc, env, c)

	read := c.schedule(streamOp{"ReadRecords", &streamservice.ReadRecordsRequest{
		Stream: owned, WakeOnly: true,
	}})[0]
	_, err := sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
		Namespace: env.Namespace().String(), Stream: owned,
		Producer: workflowWriter(c.run.GetRunID(), 3), Sequence: 1, Records: records("turns"),
	})
	s.NoError(err)

	var page streamservice.ReadRecordsResponse
	failure, started := c.outcome(read, &page)
	s.Nil(failure)
	s.True(started)
	s.Empty(page.GetRecords())

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
	closed := s.readClose(sc, env, owned)
	s.Equal(payload.EncodeString("run-2").GetData(), closed.GetCloseResult().GetData())
	s.Equal(streamservice.CLOSE_STATUS_COMPLETED, closed.GetCloseStatus())
}

// A reset to an earlier run of a continued-as-new chain terminates the current run, but the
// chain goes on in the reset run, so its Streams stay open.
func (s *StreamTestSuite) TestResetToAnEarlierRunKeepsTheStreamOpen() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	ns := env.Namespace().String()
	owned := s.closeWithRun(sc, env, c)
	first := c.run.GetRunID()
	c.respond(&commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
			ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.
				ContinueAsNewWorkflowExecutionCommandAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: "stream-caller"},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: c.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
			},
		},
	})
	s.NotEqual(first, c.current)

	reset, err := env.FrontendClient().ResetWorkflowExecution(s.Context(),
		&workflowservice.ResetWorkflowExecutionRequest{
			Namespace:                 ns,
			WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: c.run.GetID(), RunId: first},
			Reason:                    "test",
			WorkflowTaskFinishEventId: 4,
			RequestId:                 uuid.NewString(),
		})
	s.NoError(err)
	s.T().Cleanup(func() {
		_ = env.SdkClient().TerminateWorkflow(s.Context(), c.run.GetID(), reset.GetRunId(), "test cleanup")
	})

	// The chain's end would close the Stream through a task on the terminated run.
	time.Sleep(2 * time.Second) //nolint:forbidigo // The close task runs on real time.
	described, err := sc.DescribeStream(s.Context(),
		&streamservice.DescribeStreamRequest{Namespace: ns, Stream: owned})
	s.NoError(err)
	s.Equal(streamservice.STREAM_STATUS_OPEN, described.GetStatus())

	// The reset run's end is the chain's end.
	s.NoError(env.SdkClient().TerminateWorkflow(s.Context(), c.run.GetID(), reset.GetRunId(), "stop"))
	s.Equal(streamservice.CLOSE_STATUS_TERMINATED, s.readClose(sc, env, owned).GetCloseStatus())
}

// directRead reads through the direct System Nexus call, which adds no History event.
// A link names the reading Workflow run.
func (s *StreamTestSuite) directRead(
	env *testcore.TestEnv,
	req *streamservice.ReadRecordsRequest,
	links ...*commonpb.Link,
) (*streamservice.ReadRecordsResponse, *failurepb.Failure) {
	resp, err := env.FrontendClient().StartNexusOperation(s.Context(), &workflowservice.StartNexusOperationRequest{
		Namespace: env.Namespace().String(),
		Endpoint:  commonnexus.SystemEndpoint,
		Service:   stream.ServiceName,
		Operation: "ReadRecords",
		Input:     payloads.MustEncodeSingle(req),
		Links:     links,
	})
	s.NoError(err)
	if failure := resp.GetOperationFailure().GetFailure(); failure != nil {
		return nil, failure
	}
	var page streamservice.ReadRecordsResponse
	s.NoError(sdkconverter.PreferProtoDataConverter.FromPayloads(
		&commonpb.Payloads{Payloads: []*commonpb.Payload{resp.GetSyncSuccess().GetPayload()}}, &page))
	return &page, nil
}

func (s *StreamTestSuite) historyLength(env *testcore.TestEnv, c *streamCaller) int {
	resp, err := env.FrontendClient().GetWorkflowExecutionHistory(s.Context(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: c.run.GetID(), RunId: c.run.GetRunID()},
	})
	s.NoError(err)
	return len(resp.GetHistory().GetEvents())
}

// A Workflow wakes with offsets only and reads its pages directly. The pages add no History
// event, and a read bounded by to_offset answers the same page after more appends, as replay
// needs.
func (s *StreamTestSuite) TestWorkflowReadsDirectlyWithNoRecordsInHistory() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	owned := s.closeWithRun(sc, env, c)
	ns := env.Namespace().String()
	appendTopics := func(sequence int64, topics ...string) {
		_, err := sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
			Namespace: ns, Stream: owned, Producer: workflowWriter(c.run.GetRunID(), 3), Sequence: sequence, Records: records(topics...),
		})
		s.NoError(err)
	}

	wake := c.schedule(streamOp{"ReadRecords", &streamservice.ReadRecordsRequest{Stream: owned, WakeOnly: true}})[0]
	appendTopics(1, "a", "b")
	var woke streamservice.ReadRecordsResponse
	failure, started := c.outcome(wake, &woke)
	s.Nil(failure)
	s.True(started)
	s.Empty(woke.GetRecords())
	s.Equal(int64(0), woke.GetNextOffset())

	before := s.historyLength(env, c)
	page, failure := s.directRead(env, &streamservice.ReadRecordsRequest{Stream: owned, FromOffset: woke.GetNextOffset()})
	s.Nil(failure)
	s.Equal([]string{"a", "b"}, kindsAndTopics(page))
	s.Equal(before, s.historyLength(env, c))

	appendTopics(2, "c")
	again, failure := s.directRead(env, &streamservice.ReadRecordsRequest{Stream: owned, ToOffset: page.GetNextOffset()})
	s.Nil(failure)
	s.Equal(page.GetRecords(), again.GetRecords())
	s.Equal(page.GetNextOffset(), again.GetNextOffset())

	// A direct read with wait long-polls, as the frontend read does.
	waited, failure := s.directRead(env, &streamservice.ReadRecordsRequest{
		Stream: owned, Start: streamservice.READ_START_END, Wait: durationpb.New(500 * time.Millisecond),
	})
	s.Nil(failure)
	s.Empty(waited.GetRecords())

	_, failure = s.directRead(env, &streamservice.ReadRecordsRequest{Stream: ref(testcore.RandomizeStr("missing"))})
	s.Equal(stream.ReasonNotFound, causeType(failure))
}

// The first run of a chain keeps the chain's Streams after it closes, so the namespace retention
// can't delete them while a later run still writes.
func (s *StreamTestSuite) TestChainOutlivesTheRetentionOfItsFirstRun() {
	env := testcore.NewEnv(s.T(),
		testcore.WithDedicatedCluster(),
		testcore.WithDynamicConfig(stream.Enabled, true),
		testcore.WithDynamicConfig(dynamicconfig.NamespaceMinRetentionGlobal, time.Duration(0)),
		testcore.WithDynamicConfig(dynamicconfig.NamespaceMinRetentionLocal, time.Duration(0)),
		testcore.WithDynamicConfig(dynamicconfig.RetentionTimerJitterDuration, time.Duration(0)),
	)
	_, err := env.FrontendClient().UpdateNamespace(s.Context(), &workflowservice.UpdateNamespaceRequest{
		Namespace: env.Namespace().String(),
		Config:    &namespacepb.NamespaceConfig{WorkflowExecutionRetentionTtl: durationpb.New(time.Second)},
	})
	s.NoError(err)
	// Wait for the namespace cache to see the new retention.
	<-time.After(2 * testcore.NamespaceCacheRefreshInterval)

	sc := s.streamClient(env)
	c := s.newCaller(env)
	owned := s.closeWithRun(sc, env, c)
	c.respond(&commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
			ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: "stream-caller"},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: c.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			},
		},
	})
	// Several retentions of the first run pass while run 2 is open.
	<-time.After(4 * time.Second)

	_, err = sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
		Namespace: env.Namespace().String(), Stream: owned,
		Producer: workflowWriter(c.current, 3), Sequence: 1, Records: records("late"),
	})
	s.NoError(err)
	page, err := sc.ReadRecords(s.Context(), &streamservice.ReadRecordsRequest{Namespace: env.Namespace().String(), Stream: owned})
	s.NoError(err)
	s.Equal([]string{"late"}, kindsAndTopics(page))
}

// An append that names a Workflow Stream by its bare id, or a Stream that doesn't exist, fails.
// It never writes somewhere a reader of the Workflow Stream can't see.
func (s *StreamTestSuite) TestAppendToAStreamThatIsNotThereFails() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	owned := s.closeWithRun(sc, env, c)
	ns := env.Namespace().String()

	for _, target := range []string{owned.GetId(), "missing-" + owned.GetId()} {
		_, err := sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
			Namespace: ns, Stream: ref(target),
			Producer: workflowWriter(c.run.GetRunID(), 3), Sequence: 1, Records: records("lost"),
		})
		st := status.Convert(err)
		s.Equal(codes.NotFound, st.Code(), target)
		reason := st.Details()[0].(*streamservice.StreamErrorDetails).GetReason()
		s.Equal(stream.ReasonNotFound, reason, target)
	}

	page, err := sc.ReadRecords(s.Context(),
		&streamservice.ReadRecordsRequest{Namespace: ns, Stream: owned})
	s.NoError(err)
	s.Empty(page.GetRecords())
}

// A Workflow Stream that no Workflow reads trims past the size cap, so its writes go on.
func (s *StreamTestSuite) TestWorkflowStreamWithNoReaderTrimsPastTheCap() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	owned := s.closeWithRun(sc, env, c)

	var first int64
	for sequence := int64(1); sequence <= 5; sequence++ {
		resp, err := sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
			Namespace: env.Namespace().String(), Stream: owned, Producer: workflowWriter(c.run.GetRunID(), 3),
			Sequence: sequence, Records: []*streamservice.AppendRecord{
				{Topic: "big", Body: &commonpb.Payload{Data: make([]byte, 1024*1024)}},
			},
		})
		s.NoError(err)
		if first == 0 {
			first = resp.GetFirstOffset()
		}
	}

	_, err := sc.ReadRecords(s.Context(), &streamservice.ReadRecordsRequest{
		Namespace: env.Namespace().String(), Stream: owned, FromOffset: first,
	})
	s.Equal(stream.ReasonExpired, status.Convert(err).Details()[0].(*streamservice.StreamErrorDetails).GetReason())
}

// The range that an open Workflow run read stays past the Retention, so its replay reads the same
// page. When the run closes, the Retention trims it.
func (s *StreamTestSuite) TestWorkflowReaderKeepsItsRangePastTheRetention() {
	env := s.newEnv(testcore.WithDynamicConfig(stream.ReaderCheckInterval, time.Second))
	sc := s.streamClient(env)
	c := s.newCaller(env)
	ns := env.Namespace().String()
	id := testcore.RandomizeStr("chat")
	owned := &streamservice.StreamRef{Id: id, WorkflowId: c.run.GetID(), FirstExecutionRunId: c.run.GetRunID()}
	_, err := sc.CreateStream(s.Context(), &streamservice.CreateStreamRequest{
		Namespace: ns, StreamId: id, Ref: owned, Retention: durationpb.New(time.Second),
		Owner: workflowWriter(c.run.GetRunID(), 3),
	})
	s.NoError(err)
	appended, err := sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
		Namespace: ns, Stream: owned, Producer: clientProducer(), Sequence: 1, Records: records("a"),
	})
	s.NoError(err)

	readerRun := s.newCaller(env).run
	reader := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{WorkflowEvent: &commonpb.Link_WorkflowEvent{
		Namespace: ns, WorkflowId: readerRun.GetID(), RunId: readerRun.GetRunID(),
	}}}
	page, failure := s.directRead(env, &streamservice.ReadRecordsRequest{Stream: owned}, reader)
	s.Nil(failure)
	s.Equal([]string{"a"}, kindsAndTopics(page))

	// Past the Retention, and past a reader check, the replay reads the same page.
	time.Sleep(3 * time.Second) //nolint:forbidigo // The Retention and the check run on real time.
	again, failure := s.directRead(env, &streamservice.ReadRecordsRequest{Stream: owned, ToOffset: page.GetNextOffset()})
	s.Nil(failure)
	s.Equal(page.GetRecords(), again.GetRecords())
	s.Equal(page.GetNextOffset(), again.GetNextOffset())

	s.NoError(env.SdkClient().TerminateWorkflow(s.Context(), readerRun.GetID(), readerRun.GetRunID(), "reader done"))
	s.AwaitTrue(func() bool {
		_, failure := s.directRead(env, &streamservice.ReadRecordsRequest{Stream: owned, FromOffset: appended.GetFirstOffset()})
		return failure != nil && causeType(failure) == stream.ReasonExpired
	}, 10*time.Second, 200*time.Millisecond)
}

// A read of a range that the size cap dropped fails with stream_expired, not with the framework's
// missing execution.
func (s *StreamTestSuite) TestReadOfADroppedRangeFailsAsExpired() {
	env := s.newEnv()
	sc := s.streamClient(env)
	ns := env.Namespace().String()
	id := testcore.RandomizeStr("chat")
	_, err := sc.CreateStream(s.Context(), &streamservice.CreateStreamRequest{Namespace: ns, StreamId: id})
	s.NoError(err)

	var first int64
	for sequence := int64(1); sequence <= 3; sequence++ {
		resp, err := sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
			Namespace: ns, Stream: ref(id), Producer: clientProducer(), Sequence: sequence,
			Records: []*streamservice.AppendRecord{{Topic: "big", Body: &commonpb.Payload{Data: make([]byte, 1536*1024)}}},
		})
		s.NoError(err)
		if first == 0 {
			first = resp.GetFirstOffset()
		}
	}

	_, err = sc.ReadRecords(s.Context(), &streamservice.ReadRecordsRequest{Namespace: ns, Stream: ref(id), FromOffset: first})
	st := status.Convert(err)
	s.Equal(codes.NotFound, st.Code())
	s.Equal(stream.ReasonExpired, st.Details()[0].(*streamservice.StreamErrorDetails).GetReason())

	// A direct page answers the reason in its operation failure.
	_, failure := s.directRead(env, &streamservice.ReadRecordsRequest{Stream: ref(id), FromOffset: first})
	s.Equal(stream.ReasonExpired, causeType(failure))
}

// Each Stream error of a direct page keeps its reason, so the SDK raises the Stream error and
// doesn't retry it.
func (s *StreamTestSuite) TestDirectReadErrorsKeepTheirReason() {
	env := s.newEnv()
	sc := s.streamClient(env)
	id := testcore.RandomizeStr("chat")
	_, err := sc.CreateStream(s.Context(), &streamservice.CreateStreamRequest{Namespace: env.Namespace().String(), StreamId: id})
	s.NoError(err)

	for reason, req := range map[string]*streamservice.ReadRecordsRequest{
		stream.ReasonNotFound: {Stream: ref("missing-" + id)},
		stream.ReasonOffset:   {Stream: ref(id), FromOffset: 1 << 62},
		stream.ReasonInvalid:  {Stream: &streamservice.StreamRef{Id: id, WorkflowId: "wf"}},
	} {
		_, failure := s.directRead(env, req)
		s.Equal(reason, causeType(failure), reason)
	}
}

// createWithRetention creates a Workflow Stream with a 1 s Retention, and appends one Record of a
// client.
func (s *StreamTestSuite) createWithRetention(
	sc streamservice.StreamServiceClient,
	env *testcore.TestEnv,
	c *streamCaller,
) (*streamservice.StreamRef, *streamservice.AppendRecordsResponse) {
	id := testcore.RandomizeStr("chat")
	owned := &streamservice.StreamRef{Id: id, WorkflowId: c.run.GetID(), FirstExecutionRunId: c.run.GetRunID()}
	_, err := sc.CreateStream(s.Context(), &streamservice.CreateStreamRequest{
		Namespace: env.Namespace().String(), StreamId: id, Ref: owned, Retention: durationpb.New(time.Second),
		Owner: workflowWriter(c.run.GetRunID(), 3),
	})
	s.NoError(err)
	appended, err := sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
		Namespace: env.Namespace().String(), Stream: owned, Producer: clientProducer(), Sequence: 1, Records: records("a"),
	})
	s.NoError(err)
	return owned, appended
}

// A wake keeps the floor of the Workflow that the server's Nexus call names, in the transaction
// that answers it, so the wake's pages read past the Retention.
func (s *StreamTestSuite) TestWorkflowWakeKeepsItsFloor() {
	env := s.newEnv()
	sc := s.streamClient(env)
	owned, _ := s.createWithRetention(sc, env, s.newCaller(env))

	reader := s.newCaller(env)
	var woke streamservice.ReadRecordsResponse
	failure, _ := reader.outcome(reader.schedule(streamOp{"ReadRecords", &streamservice.ReadRecordsRequest{
		Stream: owned, WakeOnly: true,
	}})[0], &woke)
	s.Nil(failure)

	time.Sleep(3 * time.Second) //nolint:forbidigo // The Retention runs on real time.
	page, failure := s.directRead(env, &streamservice.ReadRecordsRequest{Stream: owned, FromOffset: woke.GetNextOffset()})
	s.Nil(failure)
	s.Equal([]string{"a"}, kindsAndTopics(page))
}

// A link to a run that doesn't exist keeps no floor, so a forged link can't stop the trim.
func (s *StreamTestSuite) TestReadLinkToNoRunKeepsNoFloor() {
	env := s.newEnv()
	sc := s.streamClient(env)
	owned, appended := s.createWithRetention(sc, env, s.newCaller(env))

	forged := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{WorkflowEvent: &commonpb.Link_WorkflowEvent{
		Namespace: env.Namespace().String(), WorkflowId: "nobody", RunId: uuid.NewString(),
	}}}
	page, failure := s.directRead(env, &streamservice.ReadRecordsRequest{Stream: owned}, forged)
	s.Nil(failure)
	s.Equal([]string{"a"}, kindsAndTopics(page))

	time.Sleep(3 * time.Second) //nolint:forbidigo // The Retention runs on real time.
	_, failure = s.directRead(env, &streamservice.ReadRecordsRequest{Stream: owned, FromOffset: appended.GetFirstOffset()})
	s.Equal(stream.ReasonExpired, causeType(failure))
}

// A direct page past the reader cap fails with ResourceExhausted, which the caller backs off on.
func (s *StreamTestSuite) TestDirectReadPastTheReaderCapIsResourceExhausted() {
	env := s.newEnv(testcore.WithDynamicConfig(stream.MaxWorkflowReaders, 0))
	sc := s.streamClient(env)
	owned, _ := s.createWithRetention(sc, env, s.newCaller(env))
	reader := s.newCaller(env).run

	_, err := env.FrontendClient().StartNexusOperation(s.Context(), &workflowservice.StartNexusOperationRequest{
		Namespace: env.Namespace().String(),
		Endpoint:  commonnexus.SystemEndpoint,
		Service:   stream.ServiceName,
		Operation: "ReadRecords",
		Input:     payloads.MustEncodeSingle(&streamservice.ReadRecordsRequest{Stream: owned}),
		Links: []*commonpb.Link{{Variant: &commonpb.Link_WorkflowEvent_{WorkflowEvent: &commonpb.Link_WorkflowEvent{
			Namespace: env.Namespace().String(), WorkflowId: reader.GetID(), RunId: reader.GetRunID(),
		}}}},
	})
	var exhausted *serviceerror.ResourceExhausted
	s.ErrorAs(err, &exhausted)
	s.Contains(exhausted.Error(), "most Workflow readers")
}

// readerLink gives a Workflow run as the reader of a direct page.
func readerLink(env *testcore.TestEnv, run client.WorkflowRun) *commonpb.Link {
	return &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{
			Namespace: env.Namespace().String(), WorkflowId: run.GetID(), RunId: run.GetRunID(),
		},
	}}
}

// A reader whose floor keeps more than half of the cap gets continue_as_new_suggested, since the
// writer fails when the cap fills and only the reader's continue-as-new frees it.
func (s *StreamTestSuite) TestReaderThatKeepsHalfTheCapIsToldToContinueAsNew() {
	env := s.newEnv(testcore.WithDynamicConfig(stream.MaxStoredBytes, 4096))
	sc := s.streamClient(env)
	owned, _ := s.createWithRetention(sc, env, s.newCaller(env))
	reader := readerLink(env, s.newCaller(env).run)

	page, failure := s.directRead(env, &streamservice.ReadRecordsRequest{Stream: owned}, reader)
	s.Nil(failure)
	s.False(page.GetContinueAsNewSuggested())

	_, err := sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
		Namespace: env.Namespace().String(), Stream: owned, Producer: clientProducer(), Sequence: 2,
		Records: []*streamservice.AppendRecord{
			{Topic: "big", Body: &commonpb.Payload{Data: make([]byte, 2500)}},
		},
	})
	s.NoError(err)
	next := &streamservice.ReadRecordsRequest{Stream: owned, FromOffset: page.GetNextOffset()}
	page, failure = s.directRead(env, next, reader)
	s.Nil(failure)
	s.True(page.GetContinueAsNewSuggested())
}

// A Workflow reader of a client's Stream keeps its floor too. An append that the size cap would
// make drop the reader's range fails, and the reader's replay reads the same page.
func (s *StreamTestSuite) TestWorkflowReaderOfAClientStreamKeepsItsRangePastTheCap() {
	env := s.newEnv(testcore.WithDynamicConfig(stream.MaxStoredBytes, 4096))
	sc := s.streamClient(env)
	ns := env.Namespace().String()
	id := testcore.RandomizeStr("chat")
	_, err := sc.CreateStream(s.Context(), &streamservice.CreateStreamRequest{Namespace: ns, StreamId: id})
	s.NoError(err)
	_, err = sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
		Namespace: ns, Stream: ref(id), Producer: clientProducer(), Sequence: 1, Records: records("a"),
	})
	s.NoError(err)

	reader := readerLink(env, s.newCaller(env).run)
	page, failure := s.directRead(env, &streamservice.ReadRecordsRequest{Stream: ref(id)}, reader)
	s.Nil(failure)
	s.Equal([]string{"a"}, kindsAndTopics(page))

	bigAppend := func(sequence int64) error {
		_, err := sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
			Namespace: ns, Stream: ref(id), Producer: clientProducer(), Sequence: sequence,
			Records: []*streamservice.AppendRecord{
				{Topic: "big", Body: &commonpb.Payload{Data: make([]byte, 2500)}},
			},
		})
		return err
	}
	s.NoError(bigAppend(2))
	s.Equal(stream.ReasonTooLarge, reasonOf(bigAppend(3)))

	again, failure := s.directRead(env, &streamservice.ReadRecordsRequest{
		Stream: ref(id), ToOffset: page.GetNextOffset(),
	})
	s.Nil(failure)
	s.Equal(page.GetRecords(), again.GetRecords())
	next, failure := s.directRead(env, &streamservice.ReadRecordsRequest{
		Stream: ref(id), FromOffset: page.GetNextOffset(),
	}, reader)
	s.Nil(failure)
	s.True(next.GetContinueAsNewSuggested())
}

// A reset run's first live page gives the lowest offset its copied reads read, so its floor
// keeps them and its replay finds them past the Retention.
func (s *StreamTestSuite) TestFloorFromKeepsTheReadsOfAResetRun() {
	env := s.newEnv()
	sc := s.streamClient(env)
	owned, first := s.createWithRetention(sc, env, s.newCaller(env))
	second, err := sc.AppendRecords(s.Context(), &streamservice.AppendRecordsRequest{
		Namespace: env.Namespace().String(), Stream: owned, Producer: clientProducer(), Sequence: 2,
		Records: records("b"),
	})
	s.NoError(err)

	reader := readerLink(env, s.newCaller(env).run)
	page, failure := s.directRead(env, &streamservice.ReadRecordsRequest{
		Stream: owned, FromOffset: second.GetFirstOffset(), FloorFrom: first.GetFirstOffset(),
	}, reader)
	s.Nil(failure)
	s.Equal([]string{"b"}, kindsAndTopics(page))

	time.Sleep(3 * time.Second) //nolint:forbidigo // The Retention runs on real time.
	copied, failure := s.directRead(env, &streamservice.ReadRecordsRequest{
		Stream: owned, FromOffset: first.GetFirstOffset(), ToOffset: second.GetFirstOffset(),
	})
	s.Nil(failure)
	s.Equal([]string{"a"}, kindsAndTopics(copied))
}

// reasonOf is the reason in the StreamErrorDetails of a gRPC error.
func reasonOf(err error) string {
	for _, detail := range status.Convert(err).Details() {
		if details, ok := detail.(*streamservice.StreamErrorDetails); ok {
			return details.GetReason()
		}
	}
	return ""
}

// localActivityWriter is the Producer of a Local Activity call of a run.
func localActivityWriter(runID string, started int64) *streamservice.Producer {
	return &streamservice.Producer{
		Kind: streamservice.PRODUCER_KIND_LOCAL_ACTIVITY,
		Id:   "la:" + runID + ":1",
		Epoch: &streamservice.ProducerEpoch{
			TaskStartedEventId: started, TaskAttempt: 1, Attempt: 1,
		},
		RunId: runID,
	}
}

// A caller with write access can't close a Workflow Stream, write as a run outside its chain,
// fence a run it doesn't follow, or put a Stream into another Workflow's first run. A Local
// Activity of the chain can't close it either, since it writes before its task commits. A
// Workflow owns no Stream outside its chain.
func (s *StreamTestSuite) TestForgedOwnerCallsAreRefused() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	owned := s.closeWithRun(sc, env, c)
	ctx := s.Context()
	ns := env.Namespace().String()
	run := c.run.GetRunID()

	_, err := sc.CloseStream(ctx, &streamservice.CloseStreamRequest{
		Namespace: ns, Stream: owned, Producer: clientProducer(),
		Result: payload.EncodeString("forged"),
	})
	s.Equal(stream.ReasonNotOwner, reasonOf(err), "a client's close")

	_, err = sc.CloseStream(ctx, &streamservice.CloseStreamRequest{
		Namespace: ns, Stream: owned, Producer: localActivityWriter(run, 3),
	})
	s.Equal(stream.ReasonNotOwner, reasonOf(err), "a Local Activity's close")

	stranger := s.newCaller(env).run.GetRunID()
	_, err = sc.AppendRecords(ctx, &streamservice.AppendRecordsRequest{
		Namespace: ns, Stream: owned, Producer: localActivityWriter(stranger, 3), Sequence: 1,
		Records: records("forged"),
	})
	s.Equal(stream.ReasonNotOwner, reasonOf(err), "a run outside the chain")

	_, err = sc.AppendRecords(ctx, &streamservice.AppendRecordsRequest{
		Namespace: ns, Stream: owned, Producer: localActivityWriter(run, 3), Sequence: 1,
		Records: records("forged"), WithdrawRun: run,
	})
	s.Equal(stream.ReasonNotOwner, reasonOf(err), "a withdraw_run of a run it doesn't follow")

	id := testcore.RandomizeStr("chat")
	_, err = sc.CreateStream(ctx, &streamservice.CreateStreamRequest{
		Namespace: ns, StreamId: id, Owner: workflowWriter(run, 3),
	})
	s.Equal(stream.ReasonInvalid, reasonOf(err), "a Workflow owner with no Workflow ref")
	inChain := &streamservice.StreamRef{Id: id, WorkflowId: c.run.GetID(), FirstExecutionRunId: run}
	for name, owner := range map[string]*streamservice.Producer{
		"a create from another chain": workflowWriter(stranger, 3),
		"a create with no owner":      nil,
	} {
		_, err = sc.CreateStream(ctx, &streamservice.CreateStreamRequest{
			Namespace: ns, StreamId: id, Ref: inChain, Owner: owner,
		})
		s.Equal(stream.ReasonNotOwner, reasonOf(err), name)
	}

	page, err := sc.ReadRecords(ctx,
		&streamservice.ReadRecordsRequest{Namespace: ns, Stream: owned})
	s.NoError(err)
	s.Empty(page.GetRecords())
	s.False(page.GetDone())
}

// An Activity writes with its task token. A token of an earlier attempt is refused, so a zombie
// attempt can't write as the Activity once its retry started.
func (s *StreamTestSuite) TestStaleActivityAttemptIsRefused() {
	env := s.newEnv()
	sc := s.streamClient(env)
	c := s.newCaller(env)
	ctx := s.Context()
	ns := env.Namespace().String()
	id := testcore.RandomizeStr("chat")
	taskQueue := &taskqueuepb.TaskQueue{Name: c.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	_, err := sc.CreateStream(ctx, &streamservice.CreateStreamRequest{Namespace: ns, StreamId: id})
	s.NoError(err)

	schedule := &commandpb.ScheduleActivityTaskCommandAttributes{
		ActivityId:          "tool",
		ActivityType:        &commonpb.ActivityType{Name: "tool"},
		TaskQueue:           taskQueue,
		StartToCloseTimeout: durationpb.New(time.Minute),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(100 * time.Millisecond),
		},
	}
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx,
		&workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: c.token,
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: schedule,
				},
			}},
		})
	s.NoError(err)
	pollAttempt := func() *workflowservice.PollActivityTaskQueueResponse {
		resp, err := env.FrontendClient().PollActivityTaskQueue(ctx,
			&workflowservice.PollActivityTaskQueueRequest{
				Namespace: ns, TaskQueue: taskQueue, Identity: "test",
			})
		s.NoError(err)
		s.NotEmpty(resp.GetTaskToken())
		return resp
	}
	write := func(task *workflowservice.PollActivityTaskQueueResponse, sequence int64) error {
		_, err := sc.AppendRecords(ctx, &streamservice.AppendRecordsRequest{
			Namespace: ns, Stream: ref(id), Sequence: sequence, Records: records("tool"),
			TaskToken: task.GetTaskToken(),
			Producer: &streamservice.Producer{
				Kind:  streamservice.PRODUCER_KIND_ACTIVITY,
				Id:    "act:" + c.run.GetRunID() + ":tool:5",
				RunId: c.run.GetRunID(),
				Epoch: &streamservice.ProducerEpoch{
					AttemptScheduledTime: task.GetCurrentAttemptScheduledTime(),
					Attempt:              task.GetAttempt(),
				},
			},
		})
		return err
	}

	first := pollAttempt()
	s.NoError(write(first, 1))
	_, err = env.FrontendClient().RespondActivityTaskFailed(ctx,
		&workflowservice.RespondActivityTaskFailedRequest{
			Namespace: ns, TaskToken: first.GetTaskToken(), Identity: "test",
			Failure: &failurepb.Failure{
				Message: "retry",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{},
				},
			},
		})
	s.NoError(err)
	second := pollAttempt()

	s.Equal(stream.ReasonNotOwner, reasonOf(write(first, 2)))
	s.NoError(write(second, 1))
}

// A Standalone Activity owns its Stream. Only its current attempt creates and closes it, and its
// end closes it in the same transaction, with its status and result.
func (s *StreamTestSuite) TestStandaloneActivityOwnsItsStream() {
	env := s.newEnv()
	sc := s.streamClient(env)
	ctx := s.Context()
	ns := env.Namespace().String()
	activityID := "tool-" + uuid.NewString()
	taskQueue := &taskqueuepb.TaskQueue{
		Name: "tools-" + uuid.NewString(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	started, err := env.FrontendClient().StartActivityExecution(ctx,
		&workflowservice.StartActivityExecutionRequest{
			Namespace:           ns,
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "tool"},
			Identity:            "test",
			TaskQueue:           taskQueue,
			StartToCloseTimeout: durationpb.New(time.Minute),
		})
	s.NoError(err)
	task, err := env.FrontendClient().PollActivityTaskQueue(ctx,
		&workflowservice.PollActivityTaskQueueRequest{
			Namespace: ns, TaskQueue: taskQueue, Identity: "test",
		})
	s.NoError(err)

	id := testcore.RandomizeStr("tokens")
	owned := &streamservice.StreamRef{
		Id: id, ActivityId: activityID, ActivityRunId: started.GetRunId(),
	}
	owner := &streamservice.Producer{
		Kind: streamservice.PRODUCER_KIND_STANDALONE_ACTIVITY,
		Id:   "sa:" + activityID + ":" + started.GetRunId(),
		Epoch: &streamservice.ProducerEpoch{
			AttemptScheduledTime: task.GetCurrentAttemptScheduledTime(),
			Attempt:              task.GetAttempt(),
		},
	}
	create := &streamservice.CreateStreamRequest{
		Namespace: ns, StreamId: id, Ref: owned, Owner: owner,
	}
	_, err = sc.CreateStream(ctx, create)
	s.Equal(stream.ReasonNotOwner, reasonOf(err), "a create with no task token")
	create.TaskToken = task.GetTaskToken()
	_, err = sc.CreateStream(ctx, create)
	s.NoError(err)
	_, err = sc.AppendRecords(ctx, &streamservice.AppendRecordsRequest{
		Namespace: ns, Stream: owned, Producer: owner, Sequence: 1, Records: records("tokens"),
		TaskToken: task.GetTaskToken(),
	})
	s.NoError(err)
	_, err = sc.CloseStream(ctx,
		&streamservice.CloseStreamRequest{Namespace: ns, Stream: owned, Producer: clientProducer()})
	s.Equal(stream.ReasonNotOwner, reasonOf(err), "a client's close")

	_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx,
		&workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: ns, TaskToken: task.GetTaskToken(), Identity: "test",
			Result: payloads.EncodeString("done"),
		})
	s.NoError(err)
	page, err := sc.ReadRecords(ctx,
		&streamservice.ReadRecordsRequest{Namespace: ns, Stream: owned})
	s.NoError(err)
	s.Equal([]string{"tokens", "close"}, kindsAndTopics(page))
	closed := page.GetRecords()[1]
	s.Equal(streamservice.CLOSE_STATUS_COMPLETED, closed.GetCloseStatus())
	s.Equal(payload.EncodeString("done").GetData(), closed.GetCloseResult().GetData())
}
