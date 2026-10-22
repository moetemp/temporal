package stream

import (
	"context"
	"errors"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/resource"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// armOwnerCheck schedules the next check of the owner's run chain.
func (s *Stream) armOwnerCheck(ctx chasm.MutableContext, interval time.Duration) {
	at := ctx.Now(s).Add(interval)
	s.OwnerCheckTime = timestamppb.New(at)
	ctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: at}, &streampb.StreamOwnerCheckTask{
		ScheduledTime: s.OwnerCheckTime,
	})
}

type ownerCheckTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*streampb.StreamOwnerCheckTask]

	config            *Config
	historyClient     resource.HistoryClient
	namespaceRegistry namespace.Registry
}

func newOwnerCheckTaskHandler(
	config *Config,
	historyClient resource.HistoryClient,
	namespaceRegistry namespace.Registry,
) *ownerCheckTaskHandler {
	return &ownerCheckTaskHandler{config: config, historyClient: historyClient, namespaceRegistry: namespaceRegistry}
}

func (h *ownerCheckTaskHandler) Validate(
	_ chasm.Context,
	s *Stream,
	_ chasm.TaskInvocation,
	task *streampb.StreamOwnerCheckTask,
) (bool, error) {
	return s.Phase == streampb.STREAM_PHASE_OPEN &&
		s.GetOwnerCheckTime().AsTime().Equal(task.GetScheduledTime().AsTime()), nil
}

// ownerEnd is what a check learned about the owner's run chain.
type ownerEnd struct {
	ended   bool
	result  *commonpb.Payload
	failure *failurepb.Failure
}

// Execute describes the owner's chain. When it closed, the Stream closes with the result or the
// failure of its last run. Otherwise the next check is armed.
func (h *ownerCheckTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *streampb.StreamOwnerCheckTask,
) error {
	state, err := chasm.ReadComponent(ctx, ref,
		func(s *Stream, _ chasm.Context, _ struct{}) (*streampb.StreamState, error) {
			return common.CloneProto(s.StreamState), nil
		}, struct{}{})
	if err != nil {
		return err
	}
	ns, err := h.namespaceRegistry.GetNamespaceByID(namespace.ID(ref.NamespaceID))
	if err != nil {
		return err
	}
	end, err := h.ownerEnded(ctx, ns, state)
	if err != nil {
		return err
	}
	limits := h.config.LimitsFor(ns.Name().String())
	_, _, err = chasm.UpdateComponent(ctx, ref,
		func(s *Stream, mctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			if s.Phase != streampb.STREAM_PHASE_OPEN || !s.GetOwnerCheckTime().AsTime().Equal(task.GetScheduledTime().AsTime()) {
				return struct{}{}, nil
			}
			if end.ended {
				s.close(mctx, s.GetOwner(), end.result, end.failure)
				s.finish(mctx, limits)
				return struct{}{}, nil
			}
			s.armOwnerCheck(mctx, h.config.OwnerCheckInterval(ns.Name().String()))
			return struct{}{}, nil
		}, struct{}{})
	return err
}

// ownerEnded describes the current run of the owner's Workflow id. A chain that ended through a
// retry, a cron run or Continue-as-New has a running current run, so only the chain's last run
// closes the Stream. A missing Workflow, or a current run of another chain, ends it with a
// failure.
func (h *ownerCheckTaskHandler) ownerEnded(
	ctx context.Context,
	ns *namespace.Namespace,
	state *streampb.StreamState,
) (ownerEnd, error) {
	end := ownerEnd{}
	gone := ownerEnd{ended: true, failure: &failurepb.Failure{
		Message: "The owner Workflow ended without closing the Stream",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			NonRetryable: true,
		}},
	}}
	current, err := h.describe(ctx, ns, state.GetOwnerWorkflowId())
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return gone, nil
	}
	if err != nil {
		return ownerEnd{}, err
	}
	info := current.GetWorkflowExecutionInfo()
	if info.GetFirstRunId() != state.GetOwnerFirstRunId() {
		return gone, nil
	}
	switch info.GetStatus() {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED:
		return end, nil
	default:
	}
	event, err := h.closeEvent(ctx, ns, info.GetExecution())
	if err != nil {
		return ownerEnd{}, err
	}
	end.ended = true
	switch {
	case event.GetWorkflowExecutionCompletedEventAttributes() != nil:
		if payloads := event.GetWorkflowExecutionCompletedEventAttributes().GetResult().GetPayloads(); len(payloads) > 0 {
			end.result = payloads[0]
		}
	case event.GetWorkflowExecutionFailedEventAttributes() != nil:
		end.failure = event.GetWorkflowExecutionFailedEventAttributes().GetFailure()
	case event.GetWorkflowExecutionCanceledEventAttributes() != nil:
		end.failure = &failurepb.Failure{
			Message: "The owner Workflow was canceled",
			FailureInfo: &failurepb.Failure_CanceledFailureInfo{CanceledFailureInfo: &failurepb.CanceledFailureInfo{
				Details: event.GetWorkflowExecutionCanceledEventAttributes().GetDetails(),
			}},
		}
	case event.GetWorkflowExecutionTerminatedEventAttributes() != nil:
		end.failure = &failurepb.Failure{
			Message:     event.GetWorkflowExecutionTerminatedEventAttributes().GetReason(),
			FailureInfo: &failurepb.Failure_TerminatedFailureInfo{TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{}},
		}
	case event.GetWorkflowExecutionTimedOutEventAttributes() != nil:
		end.failure = &failurepb.Failure{
			Message: "The owner Workflow timed out",
			FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			}},
		}
	default:
		return gone, nil
	}
	return end, nil
}

func (h *ownerCheckTaskHandler) describe(
	ctx context.Context,
	ns *namespace.Namespace,
	workflowID string,
) (*historyservice.DescribeWorkflowExecutionResponse, error) {
	return h.historyClient.DescribeWorkflowExecution(ctx, &historyservice.DescribeWorkflowExecutionRequest{
		NamespaceId: ns.ID().String(),
		Request: &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns.Name().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		},
	})
}

// closeEvent reads the event that closed a run.
func (h *ownerCheckTaskHandler) closeEvent(
	ctx context.Context,
	ns *namespace.Namespace,
	execution *commonpb.WorkflowExecution,
) (*historypb.HistoryEvent, error) {
	resp, err := h.historyClient.GetWorkflowExecutionHistory(ctx, &historyservice.GetWorkflowExecutionHistoryRequest{
		NamespaceId: ns.ID().String(),
		Request: &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:              ns.Name().String(),
			Execution:              execution,
			HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
			// The frontend sets a page size before it calls history, and history needs one.
			MaximumPageSize: 1000,
		},
	})
	if err != nil {
		return nil, err
	}
	events := resp.GetResponse().GetHistory().GetEvents()
	if len(events) == 0 {
		events = resp.GetHistory().GetEvents()
	}
	if len(events) == 0 {
		return nil, serviceerror.NewUnavailable("the close event of the owner run is not readable yet")
	}
	return events[len(events)-1], nil
}
