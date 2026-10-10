package tests

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/testing/updateutils"
	"go.temporal.io/server/tests/testcore"
)

// postNexusProgress posts a progress delivery to an operation's callback URL and answers the
// status code.
func postNexusProgress(ctx context.Context, callbackURL, callbackToken, body string) (int, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		callbackURL,
		strings.NewReader(body),
	)
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
		env := s.newTestEnv(
			chasmEnabled,
			testcore.WithDynamicConfig(chasmnexus.EnableProgress, enabled),
		)
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
			err := c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{}).
				Get(ctx, &result)
			return result, err
		}
		run, err := env.SdkClient().
			ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, callerWF)
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
			s.Equal(
				enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				started[len(started)-1].GetEventType(),
			)
		}, 10*time.Second, 50*time.Millisecond)

		progress := func(body string) int {
			status, err := postNexusProgress(ctx, callbackURL, callbackToken, body)
			s.NoError(err)
			return status
		}
		accepts := enabled && chasmEnabled
		if accepts {
			s.Equal(
				http.StatusOK,
				<-earlyStatus,
				"progress before the start response is dropped, not refused",
			)
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
			accepted := `{"position": "cursor-2", "counter": "2", "metadata": {"topic": "t"}}`
			s.Equal(http.StatusOK, progress(accepted))
			// An accepted delivery rides the next Workflow Task's scheduled event.
			scheduledEventID := s.RequireHistoryEvent(
				started, enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED).GetEventId()
			s.Await(func(s *NexusWorkflowTestSuite) {
				hist := env.GetHistory(env.Namespace().String(), wfExec)
				carried := carriedNexusProgress(hist[len(started):])
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
			s.Equal(
				http.StatusNotFound,
				progress(`{"counter": 3}`),
				"a closed operation answers 404",
			)
		}
		w.Stop()
	}
}

func scheduleNexusOperationCmd(t *testing.T, endpoint string) *commandpb.Command {
	attrs := &commandpb.ScheduleNexusOperationCommandAttributes{
		Endpoint:  endpoint,
		Service:   "service",
		Operation: "operation",
		Input:     testcore.MustToPayload(t, "input"),
	}
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
		Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
			ScheduleNexusOperationCommandAttributes: attrs,
		},
	}
}

// carriedNexusProgress lists the Nexus operation progress the scheduled events in hist carry.
func carriedNexusProgress(hist []*historypb.HistoryEvent) []*nexuspb.NexusOperationProgress {
	var carried []*nexuspb.NexusOperationProgress
	for _, event := range hist {
		carried = append(
			carried,
			event.GetWorkflowTaskScheduledEventAttributes().GetNexusOperationProgress()...)
	}
	return carried
}

// TestNexusOperationProgress checks how progress reaches a CHASM caller Workflow: on Workflow Task
// scheduled events with no event of its own, folded into a scheduled task that has not started and
// then carried by one follow-up task so the highest counter arrives, dropped after completion, and
// replayed. The HSM caller refuses progress.
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
		fut := c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{})
		if err := fut.Get(ctx, &result); err != nil {
			return "", err
		}
		workflow.GetSignalChannel(ctx, "finish").Receive(ctx, nil)
		return result, nil
	}
	run, err := env.SdkClient().
		ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, callerWF)
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
	waitIdle := func(
		check func(s *NexusWorkflowTestSuite, hist []*historypb.HistoryEvent),
	) []*historypb.HistoryEvent {
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
		status, err := postNexusProgress(
			ctx,
			callbackURL,
			callbackToken,
			fmt.Sprintf(`{"position": "p%d", "counter": %d}`, counter, counter),
		)
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
		s.Equal(
			http.StatusBadRequest,
			progress(1),
			"an HSM caller refuses progress, so the handler stops sending it",
		)
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
		carried := carriedNexusProgress(hist[len(beforeBurst):])
		s.NotEmpty(carried)
		s.Equal(
			int64(3),
			carried[len(carried)-1].GetCounter(),
			"the Workflow sees the highest counter",
		)
	})
	folded := afterFold[len(beforeBurst):]
	onlyWorkflowTaskEvents(folded)
	carried := carriedNexusProgress(folded)
	s.Len(carried, 2)
	s.Equal(scheduledEventID, carried[0].GetScheduledEventId())
	s.Equal(
		int64(1),
		carried[0].GetCounter(),
		"the task that was already scheduled carries what it had",
	)
	s.Equal(int64(3), carried[1].GetCounter(), "one follow-up task carries what folded into it")
	var scheduledTasks int
	for _, event := range folded {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
			scheduledTasks++
		}
	}
	s.Equal(2, scheduledTasks, "the burst costs at most one extra task")

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

	resp, err := env.FrontendClient().
		GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: env.Namespace().String(),
			Execution: wfExec,
		})
	s.NoError(err)
	s.Len(carriedNexusProgress(resp.GetHistory().GetEvents()), 3, "the progress is part of History")
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
		laCtx := workflow.WithLocalActivityOptions(
			ctx,
			workflow.LocalActivityOptions{StartToCloseTimeout: time.Minute},
		)
		la := workflow.ExecuteLocalActivity(laCtx, longLocalActivity)
		if err := la.Get(ctx, nil); err != nil {
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
	status, err := postNexusProgress(
		ctx,
		callbackURL,
		callbackToken,
		`{"position": "p1", "counter": 1}`,
	)
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
		s.GreaterOrEqual(
			countScheduled(duringLA),
			2,
			"the worker keeps heartbeating the local activity",
		)
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
	s.Greater(
		carrierIdx,
		markerIdx,
		"the first normal task after the heartbeats carries the progress",
	)
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
func (s *NexusWorkflowTestSuite) TestNexusOperationProgressSkipsHeartbeatRetries(
	chasmEnabled bool,
) {
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

	run, err := env.SdkClient().
		ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, "workflow")
	s.NoError(err)
	frontend := env.FrontendClient()
	poll := func() *workflowservice.PollWorkflowTaskQueueResponse {
		resp, err := frontend.PollWorkflowTaskQueue(
			ctx,
			&workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: ns,
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueue,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				Identity: "test",
			},
		)
		s.NoError(err)
		s.NotEmpty(resp.GetTaskToken())
		return resp
	}
	respond := func(
		task *workflowservice.PollWorkflowTaskQueueResponse,
		forceCreate bool,
		commands ...*commandpb.Command,
	) {
		_, err := frontend.RespondWorkflowTaskCompleted(
			ctx,
			&workflowservice.RespondWorkflowTaskCompletedRequest{
				Namespace:                  ns,
				Identity:                   "test",
				TaskToken:                  task.GetTaskToken(),
				Commands:                   commands,
				ForceCreateNewWorkflowTask: forceCreate,
			},
		)
		s.NoError(err)
	}
	// lastScheduled is the scheduled event of the task a poll answered.
	lastScheduled := func(
		task *workflowservice.PollWorkflowTaskQueueResponse,
	) *historypb.HistoryEvent {
		events := task.GetHistory().GetEvents()
		for i := len(events) - 1; i >= 0; i-- {
			if events[i].GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
				return events[i]
			}
		}
		s.FailNow("the task has no scheduled event")
		return nil
	}

	respond(poll(), false, scheduleNexusOperationCmd(s.T(), endpointName))
	startedTask := poll()
	s.RequireHistoryEvent(
		startedTask.GetHistory().GetEvents(),
		enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
	)

	// The worker forces a heartbeat task, takes it, gets progress meanwhile, and fails it.
	respond(startedTask, true)
	heartbeatTask := poll()
	s.Empty(
		lastScheduled(
			heartbeatTask,
		).GetWorkflowTaskScheduledEventAttributes().
			GetNexusOperationProgress(),
	)
	status, err := postNexusProgress(
		ctx,
		callbackURL,
		callbackToken,
		`{"position": "p1", "counter": 1}`,
	)
	s.NoError(err)
	s.Equal(http.StatusOK, status)
	_, err = frontend.RespondWorkflowTaskFailed(
		ctx,
		&workflowservice.RespondWorkflowTaskFailedRequest{
			Namespace: ns,
			Identity:  "test",
			TaskToken: heartbeatTask.GetTaskToken(),
			Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
		},
	)
	s.NoError(err)
	s.NoError(env.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "wake", nil))

	retryTask := poll()
	s.RequireHistoryEvent(
		retryTask.GetHistory().GetEvents(),
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
	)
	s.Empty(
		lastScheduled(
			retryTask,
		).GetWorkflowTaskScheduledEventAttributes().
			GetNexusOperationProgress(),
		"the retry of a heartbeat task carries no progress",
	)
	respond(retryTask, false)

	normalTask := poll()
	carried := lastScheduled(
		normalTask,
	).GetWorkflowTaskScheduledEventAttributes().
		GetNexusOperationProgress()
	s.Len(carried, 1, "the next normal task carries the progress")
	s.Equal(int64(1), carried[0].GetCounter())
	respond(normalTask, false, completeWorkflowCmd())
	s.NoError(run.Get(ctx, nil))
}

// TestNexusOperationProgressRidesSpeculativeTasks checks that progress reaches the Workflow when it
// arrives while a speculative task for an Update is pending, and the Update is then rejected. The
// progress transaction turns the speculative task into a normal one: one that has not started
// carries the progress, and one already started is kept, so a follow-up task carries it.
func (s *NexusWorkflowTestSuite) TestNexusOperationProgressRidesSpeculativeTasks(
	chasmEnabled bool,
) {
	if !chasmEnabled {
		// An HSM caller refuses progress; TestNexusOperationProgress covers it.
		return
	}
	env := s.newTestEnv(chasmEnabled, testcore.WithDynamicConfig(chasmnexus.EnableProgress, true))
	ctx := s.Context()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	ns := env.Namespace().String()
	tv := testvars.New(s.T())
	updates := updateutils.New(s.T())

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

	run, err := env.SdkClient().
		ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, "workflow")
	s.NoError(err)
	wfExec := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}
	frontend := env.FrontendClient()
	poll := func() *workflowservice.PollWorkflowTaskQueueResponse {
		resp, err := frontend.PollWorkflowTaskQueue(
			ctx,
			&workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: ns,
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueue,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				Identity: "test",
			},
		)
		s.NoError(err)
		s.NotEmpty(resp.GetTaskToken())
		return resp
	}
	respond := func(
		task *workflowservice.PollWorkflowTaskQueueResponse,
		commands ...*commandpb.Command,
	) {
		_, err := frontend.RespondWorkflowTaskCompleted(
			ctx,
			&workflowservice.RespondWorkflowTaskCompletedRequest{
				Namespace: ns,
				Identity:  "test",
				TaskToken: task.GetTaskToken(),
				Commands:  commands,
			},
		)
		s.NoError(err)
	}
	rejectUpdate := func(task *workflowservice.PollWorkflowTaskQueueResponse) {
		s.Len(task.GetMessages(), 1, "the task carries the Update request")
		_, err := frontend.RespondWorkflowTaskCompleted(
			ctx,
			&workflowservice.RespondWorkflowTaskCompletedRequest{
				Namespace: ns,
				Identity:  "test",
				TaskToken: task.GetTaskToken(),
				Messages:  updates.UpdateRejectMessages(tv, task.GetMessages()[0]),
			},
		)
		s.NoError(err)
	}
	sendUpdate := func(updateID string) <-chan *updatepb.Outcome {
		done := make(chan *updatepb.Outcome, 1)
		go func() {
			resp, err := frontend.UpdateWorkflowExecution(
				ctx,
				&workflowservice.UpdateWorkflowExecutionRequest{
					Namespace:         ns,
					WorkflowExecution: wfExec,
					Request: &updatepb.Request{
						Meta:  &updatepb.Meta{UpdateId: updateID},
						Input: &updatepb.Input{Name: "update"},
					},
					WaitPolicy: &updatepb.WaitPolicy{
						LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
					},
				},
			)
			s.NoError(err)
			done <- resp.GetOutcome()
		}()
		return done
	}
	// carried is the progress on the scheduled event of the task a poll answered.
	carried := func(
		task *workflowservice.PollWorkflowTaskQueueResponse,
	) []*nexuspb.NexusOperationProgress {
		events := task.GetHistory().GetEvents()
		for i := len(events) - 1; i >= 0; i-- {
			if events[i].GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
				return events[i].GetWorkflowTaskScheduledEventAttributes().
					GetNexusOperationProgress()
			}
		}
		return nil
	}
	postProgress := func(counter int) {
		status, err := postNexusProgress(
			ctx,
			callbackURL,
			callbackToken,
			fmt.Sprintf(`{"position": "p%d", "counter": %d}`, counter, counter),
		)
		s.NoError(err)
		s.Equal(http.StatusOK, status)
	}

	respond(poll(), scheduleNexusOperationCmd(s.T(), endpointName))
	startedTask := poll()
	s.RequireHistoryEvent(
		startedTask.GetHistory().GetEvents(),
		enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
	)
	respond(startedTask)

	// An Update schedules a speculative task, and progress arrives before any worker takes it.
	firstUpdate := sendUpdate("unstarted")
	s.Await(func(s *NexusWorkflowTestSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
		s.NoError(err)
		s.NotNil(desc.GetPendingWorkflowTask())
	}, 10*time.Second, 50*time.Millisecond)
	postProgress(1)
	unstartedTask := poll()
	s.Len(carried(unstartedTask), 1, "the task that was speculative carries the progress")
	s.Equal(int64(1), carried(unstartedTask)[0].GetCounter())
	rejectUpdate(unstartedTask)
	s.NotNil((<-firstUpdate).GetFailure(), "the Update was rejected")

	// This time the worker has already started the speculative task when progress arrives.
	secondUpdate := sendUpdate("started")
	startedSpeculative := poll()
	s.Len(startedSpeculative.GetMessages(), 1)
	postProgress(2)
	rejectUpdate(startedSpeculative)
	s.NotNil((<-secondUpdate).GetFailure(), "the Update was rejected")
	followUp := poll()
	s.Len(carried(followUp), 1, "a follow-up task carries the progress the started task could not")
	s.Equal(int64(2), carried(followUp)[0].GetCounter())
	respond(followUp, completeWorkflowCmd())
	s.NoError(run.Get(ctx, nil))
}

func (s *NexusWorkflowTestSuite) scheduledEventID(hist []*historypb.HistoryEvent) int64 {
	return s.RequireHistoryEvent(hist, enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED).GetEventId()
}

// rawProgressCaller drives a caller Workflow with raw Workflow Task polls, so a test controls
// when each task starts and how it ends.
type rawProgressCaller struct {
	s             *NexusWorkflowTestSuite
	env           *NexusTestEnv
	ctx           context.Context
	taskQueue     string
	run           client.WorkflowRun
	callbackToken string
	callbackURL   string
}

func (s *NexusWorkflowTestSuite) newRawProgressCaller(
	opts ...testcore.TestOption,
) *rawProgressCaller {
	opts = append(opts, testcore.WithDynamicConfig(chasmnexus.EnableProgress, true))
	c := &rawProgressCaller{
		s:         s,
		env:       s.newTestEnv(true, opts...),
		ctx:       s.Context(),
		taskQueue: testcore.RandomizeStr(s.T().Name()),
	}
	h := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			c.callbackToken = options.CallbackHeader.Get(commonnexus.CallbackTokenHeader)
			c.callbackURL = options.CallbackURL
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "test"}, nil
		},
	}
	endpointName := c.env.createRandomExternalNexusServer(c.ctx, s.T(), h)
	run, err := c.env.SdkClient().ExecuteWorkflow(
		c.ctx, client.StartWorkflowOptions{TaskQueue: c.taskQueue}, "workflow")
	s.NoError(err)
	c.run = run
	c.respond(c.poll(), scheduleNexusOperationCmd(s.T(), endpointName))
	started := c.poll()
	s.RequireHistoryEvent(
		started.GetHistory().GetEvents(),
		enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
	)
	c.respond(started)
	return c
}

func (c *rawProgressCaller) poll() *workflowservice.PollWorkflowTaskQueueResponse {
	resp, err := c.env.FrontendClient().
		PollWorkflowTaskQueue(c.ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: c.env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: c.taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		})
	c.s.NoError(err)
	c.s.NotEmpty(resp.GetTaskToken())
	return resp
}

func (c *rawProgressCaller) respond(
	task *workflowservice.PollWorkflowTaskQueueResponse,
	commands ...*commandpb.Command,
) {
	_, err := c.env.FrontendClient().
		RespondWorkflowTaskCompleted(c.ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: c.env.Namespace().String(),
			Identity:  "test",
			TaskToken: task.GetTaskToken(),
			Commands:  commands,
		})
	c.s.NoError(err)
}

func (c *rawProgressCaller) complete(task *workflowservice.PollWorkflowTaskQueueResponse) {
	c.respond(task, completeWorkflowCmd())
	c.s.NoError(c.run.Get(c.ctx, nil))
}

func (c *rawProgressCaller) postProgress(counter int) {
	body := fmt.Sprintf(`{"position": "p%d", "counter": %d}`, counter, counter)
	status, err := postNexusProgress(c.ctx, c.callbackURL, c.callbackToken, body)
	c.s.NoError(err)
	c.s.Equal(http.StatusOK, status)
}

// lastScheduled is the scheduled event of the task a poll answered.
func lastScheduled(task *workflowservice.PollWorkflowTaskQueueResponse) *historypb.HistoryEvent {
	events := task.GetHistory().GetEvents()
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
			return events[i]
		}
	}
	return nil
}

// TestNexusOperationProgressMinInterval checks that progress schedules a task of its own no sooner
// than nexusoperation.progressMinInterval after the last task that carried progress, so a long
// stream can't fill the caller's History.
func (s *NexusWorkflowTestSuite) TestNexusOperationProgressMinInterval(chasmEnabled bool) {
	if !chasmEnabled {
		// An HSM caller refuses progress; TestNexusOperationProgress covers it.
		return
	}
	const interval = 2 * time.Second
	c := s.newRawProgressCaller(
		testcore.WithDynamicConfig(chasmnexus.ProgressMinInterval, interval),
	)

	c.postProgress(1)
	first := c.poll()
	s.Equal(int64(1), lastScheduled(first).GetWorkflowTaskScheduledEventAttributes().
		GetNexusOperationProgress()[0].GetCounter())
	c.respond(first)

	c.postProgress(2)
	c.postProgress(3)
	second := c.poll()
	carried := lastScheduled(
		second,
	).GetWorkflowTaskScheduledEventAttributes().
		GetNexusOperationProgress()
	s.Len(carried, 1)
	s.Equal(int64(3), carried[0].GetCounter(), "progress that waited folds into one task")
	gap := lastScheduled(
		second,
	).GetEventTime().
		AsTime().
		Sub(lastScheduled(first).GetEventTime().AsTime())
	s.GreaterOrEqual(
		gap,
		interval,
		"progress waits out the interval since the last task that carried it",
	)
	c.complete(second)
}

// TestNexusOperationProgressOnFailedTask checks where progress is after the task that carried it
// fails. It stays on that task's scheduled event, which the retry's History includes, and the
// retry's own scheduled event doesn't repeat it. So a worker reads progress from every scheduled
// event since its last completed task.
func (s *NexusWorkflowTestSuite) TestNexusOperationProgressOnFailedTask(chasmEnabled bool) {
	if !chasmEnabled {
		// An HSM caller refuses progress; TestNexusOperationProgress covers it.
		return
	}
	c := s.newRawProgressCaller()

	c.postProgress(1)
	failing := c.poll()
	s.Len(
		lastScheduled(
			failing,
		).GetWorkflowTaskScheduledEventAttributes().
			GetNexusOperationProgress(),
		1,
	)
	_, err := c.env.FrontendClient().
		RespondWorkflowTaskFailed(c.ctx, &workflowservice.RespondWorkflowTaskFailedRequest{
			Namespace: c.env.Namespace().String(),
			Identity:  "test",
			TaskToken: failing.GetTaskToken(),
			Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
		})
	s.NoError(err)

	retry := c.poll()
	s.Equal(int32(2), retry.GetAttempt())
	s.Empty(
		lastScheduled(retry).GetWorkflowTaskScheduledEventAttributes().GetNexusOperationProgress(),
		"the retry doesn't repeat what the failed task carried",
	)
	carried := carriedNexusProgress(retry.GetHistory().GetEvents())
	s.Len(carried, 1, "the failed task's scheduled event still carries the progress")
	s.Equal(int64(1), carried[0].GetCounter())
	c.complete(retry)
}
