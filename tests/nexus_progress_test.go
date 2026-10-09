package tests

import (
	"bytes"
	"context"
	"net/http"
	"slices"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	notificationpb "go.temporal.io/api/notification/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/encoding/protojson"
)

// TestNexusOperationProgress drives a burst of progress deliveries through the completion endpoint
// and checks that the CHASM caller folds them onto Workflow Task scheduled events with no event of
// their own, drops progress after completion, and replays. The HSM caller refuses progress.
func (s *NexusWorkflowTestSuite) TestNexusOperationProgress(chasmEnabled bool) {
	env := s.newTestEnv(chasmEnabled, testcore.WithDynamicConfig(chasmnexus.EnableProgress, true))
	ctx := s.Context()
	taskQueue := testcore.RandomizeStr(s.T().Name())

	var callbackToken, callbackURL string
	h := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			callbackToken = options.CallbackHeader.Get(commonnexus.CallbackTokenHeader)
			callbackURL = options.CallbackURL
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "test"}, nil
		},
	}
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	callerWF := func(ctx workflow.Context) (string, error) {
		c := workflow.NewNexusClient(endpointName, "service")
		var result string
		if err := c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{}).Get(ctx, &result); err != nil {
			return "", err
		}
		workflow.GetSignalChannel(ctx, "finish").Receive(ctx, nil)
		return result, nil
	}

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, callerWF)
	s.NoError(err)
	w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(callerWF)
	s.NoError(w.Start())
	defer w.Stop()

	wfExec := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}
	history := func() []*historypb.HistoryEvent {
		return env.GetHistory(env.Namespace().String(), wfExec)
	}
	// Idle means the Workflow is blocked on the operation or the Signal with no task in flight.
	waitIdle := func(check func(s *NexusWorkflowTestSuite, hist []*historypb.HistoryEvent)) []*historypb.HistoryEvent {
		var hist []*historypb.HistoryEvent
		s.Await(func(s *NexusWorkflowTestSuite) {
			desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
			s.NoError(err)
			s.Nil(desc.GetPendingWorkflowTask())
			hist = history()
			s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, hist[len(hist)-1].GetEventType())
			check(s, hist)
		}, 10*time.Second, 50*time.Millisecond)
		return hist
	}
	sendProgress := func(counter int64) int {
		body, err := protojson.Marshal(&notificationpb.Notification{
			Position: []byte{byte(counter)},
			Counter:  counter,
		})
		s.NoError(err)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, callbackURL, bytes.NewReader(body))
		s.NoError(err)
		req.Header.Set("Nexus-Operation-State", string(nexus.OperationStateRunning))
		req.Header.Set(commonnexus.CallbackTokenHeader, callbackToken)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		s.NoError(err)
		s.NoError(resp.Body.Close())
		return resp.StatusCode
	}

	beforeBurst := waitIdle(func(s *NexusWorkflowTestSuite, hist []*historypb.HistoryEvent) {
		s.RequireHistoryEvent(hist, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED)
	})

	if !chasmEnabled {
		s.Equal(http.StatusBadRequest, sendProgress(1), "an HSM caller refuses progress, so the handler stops sending it")
	} else {
		for counter := int64(1); counter <= 3; counter++ {
			s.Equal(http.StatusOK, sendProgress(counter))
		}
		// A stale counter is accepted and dropped.
		s.Equal(http.StatusOK, sendProgress(2))

		afterBurst := waitIdle(func(s *NexusWorkflowTestSuite, hist []*historypb.HistoryEvent) {
			s.Equal(int64(3), latestProgressCounter(hist[len(beforeBurst):]))
		})
		burst := afterBurst[len(beforeBurst):]
		var scheduled int
		var counters []int64
		for _, event := range burst {
			s.Contains([]enumspb.EventType{
				enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
			}, event.GetEventType(), "progress must add no event of its own")
			if attrs := event.GetWorkflowTaskScheduledEventAttributes(); attrs != nil {
				scheduled++
				for _, n := range attrs.GetNotifications() {
					s.Equal(chasmworkflow.NexusProgressChannel(s.scheduledEventID(beforeBurst)), n.GetChannel())
					counters = append(counters, n.GetCounter())
				}
			}
		}
		s.T().Logf("burst: %d events, %d Workflow Tasks, counters on scheduled events %v", len(burst), scheduled, counters)
		s.GreaterOrEqual(scheduled, 1)
		s.LessOrEqual(scheduled, 2, "one burst costs at most a task in flight plus the one that carries the rest")
		s.True(slices.IsSorted(counters), "counters on successive scheduled events only grow: %v", counters)
	}

	s.NoError(s.sendNexusCompletionRequest(ctx, callbackURL, nexusrpc.CompleteOperationOptions{
		Result: testcore.MustToPayload(s.T(), "result"),
		Header: nexus.Header{commonnexus.CallbackTokenHeader: callbackToken},
	}))
	afterCompletion := waitIdle(func(s *NexusWorkflowTestSuite, hist []*historypb.HistoryEvent) {
		s.RequireHistoryEvent(hist, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED)
	})

	if chasmEnabled {
		// The operation component is gone with its completion, so the delivery cannot reach it.
		s.Equal(http.StatusNotFound, sendProgress(4))
		s.Len(history(), len(afterCompletion), "progress after completion must not touch the Workflow")
	}

	s.NoError(env.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "finish", nil))
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("result", result)

	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(callerWF)
	s.NoError(replayer.ReplayWorkflowHistory(nil, &historypb.History{Events: history()}))

	if chasmEnabled {
		resp, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: env.Namespace().String(),
			Execution: wfExec,
		})
		s.NoError(err)
		s.Equal(int64(3), latestProgressCounter(resp.GetHistory().GetEvents()), "the progress is part of History")
	}
}

func (s *NexusWorkflowTestSuite) scheduledEventID(hist []*historypb.HistoryEvent) int64 {
	return s.RequireHistoryEvent(hist, enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED).GetEventId()
}

func latestProgressCounter(hist []*historypb.HistoryEvent) int64 {
	var latest int64
	for _, event := range hist {
		for _, n := range event.GetWorkflowTaskScheduledEventAttributes().GetNotifications() {
			latest = max(latest, n.GetCounter())
		}
	}
	return latest
}
