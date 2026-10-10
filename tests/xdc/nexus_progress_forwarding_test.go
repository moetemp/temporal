package xdc

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	cnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
)

// NexusProgressForwardingSuite runs CHASM caller Workflows, the only ones that take Nexus
// operation progress, across two clusters.
type NexusProgressForwardingSuite struct {
	xdcBaseSuite
}

func TestNexusProgressForwardingSuite(t *testing.T) {
	t.Parallel()
	s := &NexusProgressForwardingSuite{}
	s.enableTransitionHistory = true
	suite.Run(t, s)
}

func (s *NexusProgressForwardingSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS.Key(): 1000,
		dynamicconfig.RefreshNexusEndpointsMinWait.Key():                               1 * time.Millisecond,
		dynamicconfig.EnableChasm.Key():                                                true,
		dynamicconfig.EnableCHASMCallbacks.Key():                                       true,
		chasmnexus.Enabled.Key():                                                       true,
		chasmnexus.EnableChasmWorkflowOperations.Key():                                 true,
		chasmnexus.ChasmWorkflowOperationsRolloutPercent.Key():                         100,
		chasmnexus.EnableProgress.Key():                                                true,
	}
	s.setupSuite()
}

func (s *NexusProgressForwardingSuite) SetupTest() {
	s.setupTest()
}

func (s *NexusProgressForwardingSuite) TearDownSuite() {
	s.tearDownSuite()
}

// TestProgressForwardedFromStandbyToActive posts progress to the standby cluster, which forwards it
// to the active one instead of refusing it, so the caller's next Workflow Task carries it.
func (s *NexusProgressForwardingSuite) TestProgressForwardedFromStandbyToActive() {
	// The callback URL points at the standby cluster, so every delivery lands there first.
	standbyCallbackURL := "http://" + s.clusters[1].Host().FrontendHTTPAddress() + "/namespaces/{{.NamespaceName}}/nexus/callback"
	s.clusters[0].OverrideDynamicConfig(s.T(), chasmnexus.CallbackURLTemplate, standbyCallbackURL)
	s.clusters[1].OverrideDynamicConfig(s.T(), chasmnexus.CallbackURLTemplate, standbyCallbackURL)

	ctx := testcore.NewContext()
	ns := s.createGlobalNamespace()
	taskQueue := fmt.Sprintf("%v-%v", "test-task-queue", uuid.New())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	var callbackToken, callbackURL string
	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			callbackToken = options.CallbackHeader.Get(cnexus.CallbackTokenHeader)
			callbackURL = options.CallbackURL
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "test"}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(s.T(), listenAddr, h)
	createEndpointReq := &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{Url: "http://" + listenAddr},
				},
			},
		},
	}
	for _, cluster := range s.clusters {
		_, err := cluster.OperatorClient().CreateNexusEndpoint(ctx, createEndpointReq)
		s.NoError(err)
	}

	activeSDKClient, err := client.Dial(client.Options{
		HostPort:  s.clusters[0].Host().FrontendGRPCAddress(),
		Namespace: ns,
		Logger:    log.NewSdkLogger(s.logger),
	})
	s.NoError(err)
	run, err := activeSDKClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, "workflow")
	s.NoError(err)

	active := s.clusters[0].FrontendClient()
	poll := func() *workflowservice.PollWorkflowTaskQueueResponse {
		resp, err := active.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: ns,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "test",
		})
		s.NoError(err)
		return resp
	}
	respond := func(resp *workflowservice.PollWorkflowTaskQueueResponse, commands ...*commandpb.Command) {
		_, err := active.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: resp.TaskToken,
			Commands:  commands,
		})
		s.NoError(err)
	}

	respond(poll(), &commandpb.Command{
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
	scheduledIdx := slices.IndexFunc(startedTask.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationScheduledEventAttributes() != nil
	})
	s.Positive(scheduledIdx)
	s.Positive(slices.IndexFunc(startedTask.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	}))
	respond(startedTask)

	// The standby has to know the operation before it can forward progress for it.
	await.RequireTrue(s.T(), func() bool {
		resp, err := s.clusters[1].FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()},
		})
		return err == nil && len(resp.PendingNexusOperations) > 0
	}, 5*time.Second, 200*time.Millisecond)

	c := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{})
	err = c.CompleteOperation(ctx, callbackURL, nexusrpc.CompleteOperationOptions{
		Result:   &nexus.Content{Header: nexus.Header{"type": "application/json"}, Data: []byte(`{"position": "p1", "counter": 1}`)},
		Progress: true,
		Header:   nexus.Header{cnexus.CallbackTokenHeader: callbackToken},
	})
	s.NoError(err, "the standby forwards progress rather than refusing it")

	progressTask := poll()
	var carried []*nexuspb.NexusOperationProgress
	for _, event := range progressTask.History.Events {
		carried = append(carried, event.GetWorkflowTaskScheduledEventAttributes().GetNexusOperationProgress()...)
	}
	s.Len(carried, 1)
	s.Equal(startedTask.History.Events[scheduledIdx].GetEventId(), carried[0].GetScheduledEventId())
	s.Equal(int64(1), carried[0].GetCounter())
	s.Equal("p1", carried[0].GetPosition())
	respond(progressTask, &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
		},
	})
	s.NoError(run.Get(ctx, nil))
}
