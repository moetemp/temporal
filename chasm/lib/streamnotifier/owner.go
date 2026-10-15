package streamnotifier

import (
	"context"
	"errors"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/resource"
	"google.golang.org/protobuf/proto"
)

// workflowOwnerEndedFailure is the failure message the callers get when the owner Workflow is
// gone, or its ID is in another chain, without having closed the stream.
const workflowOwnerEndedFailure = "the owner Workflow ended without closing the stream"

// activityOwnerEndedFailure is the failure message the callers get when the owner Activity run is
// gone, or a newer run holds its ID, without having closed the stream.
const activityOwnerEndedFailure = "the owner Activity ended without closing the stream"

// ownerFailedType is the failure type the callers get when the owner failed and the owner check
// has no failure of its own to hand them.
const ownerFailedType = "StreamOwnerFailed"

// maxOwnerFailureBytes matches the largest close that Core sends, so an implicit close is no
// larger than an explicit one.
const maxOwnerFailureBytes = 3 * 512 * 1024

// ownerEnd is what the owner check learned about the stream's owner.
type ownerEnd struct {
	ended bool
	// The failure the stream closes with, or nil for an empty result.
	failure *failurepb.Failure
	// Set when the run ended through a final Workflow Task, whose Worker closes the stream.
	workerCloses bool
	// When the run closed, if History said.
	closeTime *time.Time
	// Set when no run of the owner ID exists, so the owner may not have started yet.
	notFound bool
}

// ownerDescriber answers the owner check and the first-run check for each owner kind.
type ownerDescriber struct {
	historyClient  resource.HistoryClient
	activityClient activitypb.ActivityServiceClient
}

func newOwnerDescriber(
	historyClient resource.HistoryClient,
	activityClient activitypb.ActivityServiceClient,
) *ownerDescriber {
	return &ownerDescriber{historyClient: historyClient, activityClient: activityClient}
}

// ended reports whether the stream's owner ended, and the failure its stream closes with.
func (d *ownerDescriber) ended(
	ctx context.Context,
	namespaceID, namespaceName string,
	ref *streampb.StreamReference,
) (ownerEnd, error) {
	if ref.GetOwnerKind() == enumspb.STREAM_OWNER_KIND_ACTIVITY {
		return d.activityEnded(ctx, namespaceID, namespaceName, ref)
	}
	return d.workflowEnded(ctx, namespaceID, namespaceName, ref)
}

// checkFirstRun refuses a reference whose run ID does not key a notifier of its owner. Such a
// reference would key a second notifier for the stream, and the owner check would take the owner
// for ended and close that notifier while the stream runs. An owner that does not exist is left to
// the owner check.
func (d *ownerDescriber) checkFirstRun(
	ctx context.Context,
	namespaceID, namespaceName string,
	ref *streampb.StreamReference,
) error {
	if ref.GetOwnerKind() == enumspb.STREAM_OWNER_KIND_ACTIVITY {
		return d.checkActivityRun(ctx, namespaceID, namespaceName, ref)
	}
	return d.checkWorkflowFirstRun(ctx, namespaceID, namespaceName, ref)
}

func (d *ownerDescriber) describeWorkflow(
	ctx context.Context,
	namespaceID, namespaceName, workflowID, runID string,
) (*historyservice.DescribeWorkflowExecutionResponse, error) {
	return d.historyClient.DescribeWorkflowExecution(
		ctx,
		&historyservice.DescribeWorkflowExecutionRequest{
			NamespaceId: namespaceID,
			Request: &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: namespaceName,
				Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
			},
		},
	)
}

// workflowEnded reports whether the owner Workflow's run chain is closed, and the failure its
// stream closes with, as a Workflow-run operation completes for the same close status: none for a
// completed owner, so its callers get an empty result. A Workflow that does not exist (or whose
// retention passed) counts as ended without closing the stream, and so does a current run of
// another chain that reused the Workflow ID.
func (d *ownerDescriber) workflowEnded(
	ctx context.Context,
	namespaceID, namespaceName string,
	stream *streampb.StreamReference,
) (ownerEnd, error) {
	resp, err := d.describeWorkflow(ctx, namespaceID, namespaceName, stream.GetWorkflowId(), "")
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return ownerEnd{
			ended:    true,
			failure:  streamFailure(workflowOwnerEndedFailure),
			notFound: true,
		}, nil
	}
	if err != nil {
		return ownerEnd{}, err
	}
	// The described run is the chain's current one, so a retry, a cron run or Continue-as-New has
	// already moved past a closed earlier run.
	if firstRunID := stream.GetRunId(); firstRunID != "" &&
		resp.GetWorkflowExecutionInfo().GetFirstRunId() != firstRunID {
		return ownerEnd{ended: true, failure: streamFailure(workflowOwnerEndedFailure)}, nil
	}
	info := resp.GetWorkflowExecutionInfo()
	var closeTime *time.Time
	if info.GetCloseTime() != nil {
		t := info.GetCloseTime().AsTime()
		closeTime = &t
	}
	// A completed, failed or canceled run ended through a final Workflow Task, whose Worker
	// closes the stream itself.
	byWorker := func(failure *failurepb.Failure) ownerEnd {
		return ownerEnd{ended: true, failure: failure, workerCloses: true, closeTime: closeTime}
	}
	switch info.GetStatus() {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED:
		return ownerEnd{}, nil
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return byWorker(nil), nil
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		// The check reads no History, so it has the status but not the run's failure.
		return byWorker(ownerFailedFailure("the owner Workflow failed")), nil
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return byWorker(canceledFailure()), nil
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return ownerEnd{ended: true, failure: terminatedFailure()}, nil
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return ownerEnd{ended: true, failure: timedOutFailure()}, nil
	default:
		return ownerEnd{ended: true, failure: streamFailure(workflowOwnerEndedFailure)}, nil
	}
}

func (d *ownerDescriber) checkWorkflowFirstRun(
	ctx context.Context,
	namespaceID, namespaceName string,
	ref *streampb.StreamReference,
) error {
	resp, err := d.describeWorkflow(ctx, namespaceID, namespaceName, ref.GetWorkflowId(), "")
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return nil
	}
	if err != nil {
		return err
	}
	if resp.GetWorkflowExecutionInfo().GetFirstRunId() == ref.GetRunId() {
		return nil
	}
	// A later chain may hold the Workflow ID now. An earlier chain keeps its own notifier, so the
	// first run of any chain is taken.
	resp, err = d.describeWorkflow(
		ctx,
		namespaceID,
		namespaceName,
		ref.GetWorkflowId(),
		ref.GetRunId(),
	)
	if err == nil && resp.GetWorkflowExecutionInfo().GetFirstRunId() == ref.GetRunId() {
		return nil
	}
	if err != nil && common.IsRetryableRPCError(err) {
		return err
	}
	return serviceerror.NewInvalidArgumentf(
		"stream_ref.run_id %q is not the first run of the owner's run chain",
		ref.GetRunId(),
	)
}

func (d *ownerDescriber) describeActivity(
	ctx context.Context,
	namespaceID, namespaceName, activityID, runID string,
) (*workflowservice.DescribeActivityExecutionResponse, error) {
	resp, err := d.activityClient.DescribeActivityExecution(
		ctx,
		&activitypb.DescribeActivityExecutionRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.DescribeActivityExecutionRequest{
				Namespace:      namespaceName,
				ActivityId:     activityID,
				RunId:          runID,
				IncludeOutcome: true,
			},
		},
	)
	return resp.GetFrontendResponse(), err
}

// activityEnded reports whether the owner Activity run is closed, and the failure its stream
// closes with. One run is the owner, so retries, backoff, pause and reset keep the stream open. No
// Worker closes the stream after the run closed, since the Activity's own close comes before its
// completion, so the check closes it at once with the run's own outcome.
func (d *ownerDescriber) activityEnded(
	ctx context.Context,
	namespaceID, namespaceName string,
	stream *streampb.StreamReference,
) (ownerEnd, error) {
	// An empty run ID answers the newest run, so a reused Activity ID shows here.
	resp, err := d.describeActivity(ctx, namespaceID, namespaceName, stream.GetActivityId(), "")
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return ownerEnd{
			ended:    true,
			failure:  streamFailure(activityOwnerEndedFailure),
			notFound: true,
		}, nil
	}
	if err != nil {
		return ownerEnd{}, err
	}
	if resp.GetRunId() != stream.GetRunId() {
		return ownerEnd{ended: true, failure: streamFailure(activityOwnerEndedFailure)}, nil
	}
	failure := resp.GetOutcome().GetFailure()
	switch resp.GetInfo().GetStatus() {
	case enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, enumspb.ACTIVITY_EXECUTION_STATUS_PAUSED:
		return ownerEnd{}, nil
	case enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED:
		return ownerEnd{ended: true}, nil
	case enumspb.ACTIVITY_EXECUTION_STATUS_FAILED:
		return activityEnd(failure, ownerFailedFailure("the owner Activity failed")), nil
	case enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED:
		return activityEnd(failure, canceledFailure()), nil
	case enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED:
		return activityEnd(failure, terminatedFailure()), nil
	case enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
		return activityEnd(failure, timedOutFailure()), nil
	default:
		return ownerEnd{ended: true, failure: streamFailure(activityOwnerEndedFailure)}, nil
	}
}

// activityEnd closes with the run's own failure, or with fallback when the run has none.
func activityEnd(failure, fallback *failurepb.Failure) ownerEnd {
	if failure == nil {
		return ownerEnd{ended: true, failure: fallback}
	}
	return ownerEnd{ended: true, failure: boundFailure(failure)}
}

// checkActivityRun takes any run of the Activity ID, since each run keys its own notifier.
func (d *ownerDescriber) checkActivityRun(
	ctx context.Context,
	namespaceID, namespaceName string,
	ref *streampb.StreamReference,
) error {
	resp, err := d.describeActivity(ctx, namespaceID, namespaceName, ref.GetActivityId(), "")
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return nil
	}
	if err != nil {
		return err
	}
	if resp.GetRunId() == ref.GetRunId() {
		return nil
	}
	_, err = d.describeActivity(
		ctx,
		namespaceID,
		namespaceName,
		ref.GetActivityId(),
		ref.GetRunId(),
	)
	if err == nil {
		return nil
	}
	if common.IsRetryableRPCError(err) {
		return err
	}
	return serviceerror.NewInvalidArgumentf(
		"stream_ref.run_id %q is not a run of the owner Activity",
		ref.GetRunId(),
	)
}

// boundFailure keeps the notifier's state small. A failure above the close limit keeps only its
// message and its kind, with the type of an application failure.
func boundFailure(failure *failurepb.Failure) *failurepb.Failure {
	if proto.Size(failure) <= maxOwnerFailureBytes {
		return failure
	}
	bounded := &failurepb.Failure{Message: failure.GetMessage(), Source: failure.GetSource()}
	switch info := failure.GetFailureInfo().(type) {
	case *failurepb.Failure_ApplicationFailureInfo:
		bounded.FailureInfo = &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type:         info.ApplicationFailureInfo.GetType(),
				NonRetryable: info.ApplicationFailureInfo.GetNonRetryable(),
			},
		}
	case *failurepb.Failure_CanceledFailureInfo:
		bounded.FailureInfo = &failurepb.Failure_CanceledFailureInfo{
			CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
		}
	case *failurepb.Failure_TerminatedFailureInfo:
		bounded.FailureInfo = &failurepb.Failure_TerminatedFailureInfo{
			TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{},
		}
	case *failurepb.Failure_TimeoutFailureInfo:
		bounded.FailureInfo = &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: info.TimeoutFailureInfo.GetTimeoutType(),
			},
		}
	default:
		bounded = ownerFailedFailure(failure.GetMessage())
	}
	if proto.Size(bounded) > maxOwnerFailureBytes {
		bounded.Message = activityOwnerEndedFailure
	}
	return bounded
}

func ownerFailedFailure(message string) *failurepb.Failure {
	failure := streamFailure(message)
	failure.GetApplicationFailureInfo().Type = ownerFailedType
	return failure
}

func canceledFailure() *failurepb.Failure {
	return &failurepb.Failure{
		Message: "operation canceled",
		FailureInfo: &failurepb.Failure_CanceledFailureInfo{
			CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
		},
	}
}

func terminatedFailure() *failurepb.Failure {
	return &failurepb.Failure{
		Message: "operation terminated",
		FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
			TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{},
		},
	}
}

func timedOutFailure() *failurepb.Failure {
	return &failurepb.Failure{
		Message: "operation exceeded internal timeout",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{},
		},
	}
}
