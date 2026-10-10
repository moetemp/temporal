package tests

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/tests/testcore"
)

// postNexusProgress posts a progress delivery to an operation's callback URL and answers the
// status code.
func postNexusProgress(ctx context.Context, callbackURL, callbackToken, body string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, callbackURL, strings.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Nexus-Operation-State", string(nexus.OperationStateRunning))
	req.Header.Set(commonnexus.CallbackTokenHeader, callbackToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	return resp.StatusCode, resp.Body.Close()
}

// TestNexusOperationProgressIntake drives progress deliveries through the completion endpoint.
// Every refusal is a 400, which tells the handler to stop sending progress; an accepted delivery
// rides the next Workflow Task scheduled event; progress before the start response is dropped; and
// a closed operation answers 404.
func (s *NexusWorkflowTestSuite) TestNexusOperationProgressIntake(chasmEnabled bool) {
	for _, enabled := range []bool{false, true} {
		env := s.newTestEnv(chasmEnabled, testcore.WithDynamicConfig(chasmnexus.EnableProgress, enabled))
		ctx := s.Context()
		taskQueue := testcore.RandomizeStr(s.T().Name())

		var callbackToken, callbackURL string
		earlyStatus := make(chan int, 1)
		h := nexustest.Handler{
			OnStartOperation: func(
				ctx context.Context,
				service, operation string,
				input *nexus.LazyValue,
				options nexus.StartOperationOptions,
			) (nexus.HandlerStartOperationResult[any], error) {
				callbackToken = options.CallbackHeader.Get(commonnexus.CallbackTokenHeader)
				callbackURL = options.CallbackURL
				// Progress that races ahead of this start response.
				status, err := postNexusProgress(ctx, callbackURL, callbackToken, `{"counter": 1}`)
				if err != nil {
					return nil, err
				}
				earlyStatus <- status
				return &nexus.HandlerStartOperationResultAsync{OperationToken: "test"}, nil
			},
		}
		endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

		callerWF := func(ctx workflow.Context) (string, error) {
			c := workflow.NewNexusClient(endpointName, "service")
			var result string
			err := c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{}).Get(ctx, &result)
			return result, err
		}
		run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, callerWF)
		s.NoError(err)
		w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
		w.RegisterWorkflow(callerWF)
		s.NoError(w.Start())

		wfExec := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}
		var started []*historypb.HistoryEvent
		s.Await(func(s *NexusWorkflowTestSuite) {
			desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
			s.NoError(err)
			s.Nil(desc.GetPendingWorkflowTask())
			started = env.GetHistory(env.Namespace().String(), wfExec)
			s.RequireHistoryEvent(started, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED)
			s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, started[len(started)-1].GetEventType())
		}, 10*time.Second, 50*time.Millisecond)

		progress := func(body string) int {
			status, err := postNexusProgress(ctx, callbackURL, callbackToken, body)
			s.NoError(err)
			return status
		}
		accepts := enabled && chasmEnabled
		if accepts {
			s.Equal(http.StatusOK, <-earlyStatus, "progress before the start response is dropped, not refused")
		} else {
			s.Equal(http.StatusBadRequest, <-earlyStatus)
		}
		// Only the real start response started the operation.
		var startedEvents int
		for _, event := range started {
			if event.GetEventType() == enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED {
				startedEvents++
			}
		}
		s.Equal(1, startedEvents)

		if !accepts {
			// The flag is off, or the caller is on the HSM path: progress is refused for good.
			s.Equal(http.StatusBadRequest, progress(`{"counter": 2}`))
		} else {
			for _, body := range []string{
				`nope`,
				`{"position": "p"}`,
				`{"counter": 0}`,
				`{"counter": 2, "metadata": {"k": "` + strings.Repeat("x", 2048) + `"}}`,
			} {
				s.Equal(http.StatusBadRequest, progress(body), body)
			}
			s.Equal(http.StatusOK, progress(`{"position": "cursor-2", "counter": "2", "metadata": {"topic": "t"}}`))
			// An accepted delivery rides the next Workflow Task's scheduled event.
			scheduledEventID := s.RequireHistoryEvent(started, enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED).GetEventId()
			s.Await(func(s *NexusWorkflowTestSuite) {
				carried := carriedNexusProgress(env.GetHistory(env.Namespace().String(), wfExec)[len(started):])
				s.Len(carried, 1)
				s.Equal(scheduledEventID, carried[0].GetScheduledEventId())
				s.Equal(int64(2), carried[0].GetCounter())
				s.Equal("cursor-2", carried[0].GetPosition())
				s.Equal("t", carried[0].GetMetadata()["topic"])
			}, 10*time.Second, 50*time.Millisecond)
		}

		s.NoError(s.sendNexusCompletionRequest(ctx, callbackURL, nexusrpc.CompleteOperationOptions{
			Result: testcore.MustToPayload(s.T(), "result"),
			Header: nexus.Header{commonnexus.CallbackTokenHeader: callbackToken},
		}))
		var result string
		s.NoError(run.Get(ctx, &result))
		s.Equal("result", result)
		if accepts {
			s.Equal(http.StatusNotFound, progress(`{"counter": 3}`), "a closed operation answers 404")
		}
		w.Stop()
	}
}

// carriedNexusProgress lists the Nexus operation progress the scheduled events in hist carry.
func carriedNexusProgress(hist []*historypb.HistoryEvent) []*nexuspb.NexusOperationProgress {
	var carried []*nexuspb.NexusOperationProgress
	for _, event := range hist {
		carried = append(carried, event.GetWorkflowTaskScheduledEventAttributes().GetNexusOperationProgress()...)
	}
	return carried
}

// TestNexusOperationProgress checks how progress reaches a CHASM caller Workflow: on Workflow Task
// scheduled events with no event of its own, folded into a scheduled task that has not started,
// dropped after completion, and replayed. The HSM caller refuses progress.
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
	newWorker := func() worker.Worker {
		w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
		w.RegisterWorkflow(callerWF)
		s.NoError(w.Start())
		return w
	}
	w := newWorker()

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
	progress := func(counter int) int {
		status, err := postNexusProgress(ctx, callbackURL, callbackToken, fmt.Sprintf(`{"position": "p%d", "counter": %d}`, counter, counter))
		s.NoError(err)
		return status
	}
	onlyWorkflowTaskEvents := func(hist []*historypb.HistoryEvent) {
		for _, event := range hist {
			s.Contains([]enumspb.EventType{
				enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
			}, event.GetEventType(), "progress must add no event of its own")
		}
	}

	beforeBurst := waitIdle(func(s *NexusWorkflowTestSuite, hist []*historypb.HistoryEvent) {
		s.RequireHistoryEvent(hist, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED)
	})
	if !chasmEnabled {
		s.Equal(http.StatusBadRequest, progress(1), "an HSM caller refuses progress, so the handler stops sending it")
		w.Stop()
		return
	}
	scheduledEventID := s.scheduledEventID(beforeBurst)

	// With no worker polling, the task the first delivery schedules stays unstarted, and later
	// deliveries fold into it instead of asking for tasks of their own.
	w.Stop()
	s.Equal(http.StatusOK, progress(1))
	s.Await(func(s *NexusWorkflowTestSuite) {
		s.Len(carriedNexusProgress(history()[len(beforeBurst):]), 1)
	}, 10*time.Second, 50*time.Millisecond)
	s.Equal(http.StatusOK, progress(2))
	s.Equal(http.StatusOK, progress(3))
	s.Equal(http.StatusOK, progress(2), "a stale counter is accepted and dropped")
	w = newWorker()
	afterFold := waitIdle(func(s *NexusWorkflowTestSuite, hist []*historypb.HistoryEvent) {
		s.Greater(len(hist), len(beforeBurst))
	})
	folded := afterFold[len(beforeBurst):]
	onlyWorkflowTaskEvents(folded)
	carried := carriedNexusProgress(folded)
	s.Len(carried, 1, "one task carries the whole burst")
	s.Equal(scheduledEventID, carried[0].GetScheduledEventId())
	s.Equal(int64(1), carried[0].GetCounter(), "the task that was already scheduled carries what it had")
	var scheduledTasks int
	for _, event := range folded {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
			scheduledTasks++
		}
	}
	s.Equal(1, scheduledTasks)

	// After that task started, newer progress needs a task of its own.
	s.Equal(http.StatusOK, progress(4))
	afterNext := waitIdle(func(s *NexusWorkflowTestSuite, hist []*historypb.HistoryEvent) {
		s.Len(carriedNexusProgress(hist[len(afterFold):]), 1)
	})
	onlyWorkflowTaskEvents(afterNext[len(afterFold):])
	s.Equal(int64(4), carriedNexusProgress(afterNext[len(afterFold):])[0].GetCounter())
	s.Equal(http.StatusOK, progress(3), "a counter below a folded one is stale")

	s.NoError(s.sendNexusCompletionRequest(ctx, callbackURL, nexusrpc.CompleteOperationOptions{
		Result: testcore.MustToPayload(s.T(), "result"),
		Header: nexus.Header{commonnexus.CallbackTokenHeader: callbackToken},
	}))
	afterCompletion := waitIdle(func(s *NexusWorkflowTestSuite, hist []*historypb.HistoryEvent) {
		s.RequireHistoryEvent(hist, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED)
	})
	s.Equal(http.StatusNotFound, progress(5), "the operation is gone with its completion")
	s.Len(history(), len(afterCompletion), "progress after completion must not touch the Workflow")

	s.NoError(env.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "finish", nil))
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("result", result)
	w.Stop()

	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(callerWF)
	s.NoError(replayer.ReplayWorkflowHistory(nil, &historypb.History{Events: history()}))

	resp, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: wfExec,
	})
	s.NoError(err)
	s.Len(carriedNexusProgress(resp.GetHistory().GetEvents()), 2, "the progress is part of History")
}

// TestNexusOperationProgressSkipsHeartbeatTasks checks that progress arriving during a long local
// activity stays off the tasks a worker forces to heartbeat it, and rides the next normal task.
func (s *NexusWorkflowTestSuite) TestNexusOperationProgressSkipsHeartbeatTasks(chasmEnabled bool) {
	if !chasmEnabled {
		// An HSM caller refuses progress; TestNexusOperationProgress covers it.
		return
	}
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

	laStarted := make(chan struct{})
	var laStartedOnce sync.Once
	releaseLA := make(chan struct{})
	longLocalActivity := func(ctx context.Context) error {
		laStartedOnce.Do(func() { close(laStarted) })
		select {
		case <-releaseLA:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	callerWF := func(ctx workflow.Context) (string, error) {
		c := workflow.NewNexusClient(endpointName, "service")
		fut := c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{})
		if err := fut.GetNexusOperationExecution().Get(ctx, nil); err != nil {
			return "", err
		}
		laCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: time.Minute})
		if err := workflow.ExecuteLocalActivity(laCtx, longLocalActivity).Get(ctx, nil); err != nil {
			return "", err
		}
		var result string
		err := fut.Get(ctx, &result)
		return result, err
	}
	w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(callerWF)
	w.RegisterActivity(longLocalActivity)
	s.NoError(w.Start())
	defer w.Stop()

	// A short task timeout makes the worker heartbeat the local activity often.
	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: time.Second,
	}, callerWF)
	s.NoError(err)
	wfExec := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}
	history := func() []*historypb.HistoryEvent {
		return env.GetHistory(env.Namespace().String(), wfExec)
	}
	select {
	case <-laStarted:
	case <-ctx.Done():
		s.FailNow("the local activity did not start")
	}

	beforeProgress := history()
	status, err := postNexusProgress(ctx, callbackURL, callbackToken, `{"position": "p1", "counter": 1}`)
	s.NoError(err)
	s.Equal(http.StatusOK, status)
	countScheduled := func(hist []*historypb.HistoryEvent) int {
		var n int
		for _, event := range hist {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
				n++
			}
		}
		return n
	}
	var duringLA []*historypb.HistoryEvent
	s.Await(func(s *NexusWorkflowTestSuite) {
		duringLA = history()[len(beforeProgress):]
		s.GreaterOrEqual(countScheduled(duringLA), 2, "the worker keeps heartbeating the local activity")
	}, 10*time.Second, 50*time.Millisecond)
	s.Empty(carriedNexusProgress(duringLA), "no heartbeat task carries progress")

	close(releaseLA)
	var afterLA []*historypb.HistoryEvent
	s.Await(func(s *NexusWorkflowTestSuite) {
		afterLA = history()[len(beforeProgress):]
		s.Len(carriedNexusProgress(afterLA), 1)
	}, 10*time.Second, 50*time.Millisecond)
	markerIdx := -1
	carrierIdx := -1
	for i, event := range afterLA {
		if event.GetEventType() == enumspb.EVENT_TYPE_MARKER_RECORDED {
			markerIdx = i
		}
		if len(event.GetWorkflowTaskScheduledEventAttributes().GetNexusOperationProgress()) > 0 {
			carrierIdx = i
		}
	}
	s.Positive(markerIdx, "the local activity's result is recorded")
	s.Greater(carrierIdx, markerIdx, "the first normal task after the heartbeats carries the progress")
	s.Equal(int64(1), carriedNexusProgress(afterLA)[0].GetCounter())

	s.NoError(s.sendNexusCompletionRequest(ctx, callbackURL, nexusrpc.CompleteOperationOptions{
		Result: testcore.MustToPayload(s.T(), "result"),
		Header: nexus.Header{commonnexus.CallbackTokenHeader: callbackToken},
	}))
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("result", result)

	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(callerWF)
	s.NoError(replayer.ReplayWorkflowHistory(nil, &historypb.History{Events: history()}))
}

// TestNexusOperationProgressSkipsHeartbeatRetries checks that the retry of a failed heartbeat task
// holds progress like the task it retries, and that the next normal task carries it. A Signal
// arrives before the retry starts, so the retry gets a scheduled event of its own.
func (s *NexusWorkflowTestSuite) TestNexusOperationProgressSkipsHeartbeatRetries(chasmEnabled bool) {
	if !chasmEnabled {
		// An HSM caller refuses progress; TestNexusOperationProgress covers it.
		return
	}
	env := s.newTestEnv(chasmEnabled, testcore.WithDynamicConfig(chasmnexus.EnableProgress, true))
	ctx := s.Context()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	ns := env.Namespace().String()

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

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, "workflow")
	s.NoError(err)
	frontend := env.FrontendClient()
	poll := func() *workflowservice.PollWorkflowTaskQueueResponse {
		resp, err := frontend.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: ns,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "test",
		})
		s.NoError(err)
		s.NotEmpty(resp.GetTaskToken())
		return resp
	}
	respond := func(task *workflowservice.PollWorkflowTaskQueueResponse, forceCreate bool, commands ...*commandpb.Command) {
		_, err := frontend.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace:                  ns,
			Identity:                   "test",
			TaskToken:                  task.GetTaskToken(),
			Commands:                   commands,
			ForceCreateNewWorkflowTask: forceCreate,
		})
		s.NoError(err)
	}
	// lastScheduled is the scheduled event of the task a poll answered.
	lastScheduled := func(task *workflowservice.PollWorkflowTaskQueueResponse) *historypb.HistoryEvent {
		events := task.GetHistory().GetEvents()
		for i := len(events) - 1; i >= 0; i-- {
			if events[i].GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
				return events[i]
			}
		}
		s.FailNow("the task has no scheduled event")
		return nil
	}

	respond(poll(), false, &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
		Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
			ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
				Endpoint:  endpointName,
				Service:   "service",
				Operation: "operation",
				Input:     testcore.MustToPayload(s.T(), "input"),
			},
		},
	})
	startedTask := poll()
	s.RequireHistoryEvent(startedTask.GetHistory().GetEvents(), enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED)

	// The worker forces a heartbeat task, takes it, gets progress meanwhile, and fails it.
	respond(startedTask, true)
	heartbeatTask := poll()
	s.Empty(lastScheduled(heartbeatTask).GetWorkflowTaskScheduledEventAttributes().GetNexusOperationProgress())
	status, err := postNexusProgress(ctx, callbackURL, callbackToken, `{"position": "p1", "counter": 1}`)
	s.NoError(err)
	s.Equal(http.StatusOK, status)
	_, err = frontend.RespondWorkflowTaskFailed(ctx, &workflowservice.RespondWorkflowTaskFailedRequest{
		Namespace: ns,
		Identity:  "test",
		TaskToken: heartbeatTask.GetTaskToken(),
		Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
	})
	s.NoError(err)
	s.NoError(env.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "wake", nil))

	retryTask := poll()
	s.RequireHistoryEvent(retryTask.GetHistory().GetEvents(), enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
	s.Empty(lastScheduled(retryTask).GetWorkflowTaskScheduledEventAttributes().GetNexusOperationProgress(),
		"the retry of a heartbeat task carries no progress")
	respond(retryTask, false)

	normalTask := poll()
	carried := lastScheduled(normalTask).GetWorkflowTaskScheduledEventAttributes().GetNexusOperationProgress()
	s.Len(carried, 1, "the next normal task carries the progress")
	s.Equal(int64(1), carried[0].GetCounter())
	respond(normalTask, false, &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
		},
	})
	s.NoError(run.Get(ctx, nil))
}

func (s *NexusWorkflowTestSuite) scheduledEventID(hist []*historypb.HistoryEvent) int64 {
	return s.RequireHistoryEvent(hist, enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED).GetEventId()
}
