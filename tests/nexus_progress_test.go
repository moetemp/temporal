package tests

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
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
// changes nothing in the caller's History; progress before the start response is dropped; and a
// closed operation answers 404.
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
			// Nothing folds progress yet, so an accepted delivery leaves History as it was.
			require.Never(s.T(), func() bool {
				return len(env.GetHistory(env.Namespace().String(), wfExec)) != len(started)
			}, 300*time.Millisecond, 50*time.Millisecond)
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
