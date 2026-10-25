package stream

import (
	"context"
	"errors"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/streamservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/tasktoken"
)

// The checks of a Stream's owner and of the Producers that claim a Workflow run or an Activity
// attempt. A caller with write access could otherwise close any Stream, write as a Workflow,
// fence a run, or put a Stream into a Workflow's first run, which then outlives its retention.

func notOwner(format string, args ...any) error {
	return NewError(ReasonNotOwner, format, args...)
}

// ownerKind is the kind of the owner a create gave. A create with no owner is a client's.
func ownerKind(owner *streamservice.Producer) streamservice.ProducerKind {
	if kind := owner.GetKind(); kind != streamservice.PRODUCER_KIND_UNSPECIFIED {
		return kind
	}
	return streamservice.PRODUCER_KIND_CLIENT
}

// checkOwner refuses a create whose owner can't own a Stream. A Workflow Stream nests in its
// chain's first run, so only that chain creates one. A Standalone Activity's Stream nests in the
// Activity, so only the Activity creates one, and its host checks the task token.
func checkOwner(owner *streamservice.Producer, t target) error {
	switch kind := ownerKind(owner); {
	case t.workflow() && kind != streamservice.PRODUCER_KIND_WORKFLOW:
		return notOwner("Only the Workflow's run chain creates a Workflow Stream")
	case t.activity() && kind != streamservice.PRODUCER_KIND_STANDALONE_ACTIVITY:
		return notOwner("Only the Standalone Activity creates its Stream")
	case kind == streamservice.PRODUCER_KIND_STANDALONE_ACTIVITY && !t.activity():
		return invalid("A Standalone Activity owns only a Stream in its ref")
	case kind == streamservice.PRODUCER_KIND_WORKFLOW ||
		kind == streamservice.PRODUCER_KIND_CLIENT ||
		kind == streamservice.PRODUCER_KIND_STANDALONE_ACTIVITY:
		return nil
	default:
		return invalid("A Stream's owner is a Workflow, a client or a Standalone Activity, not %v",
			kind)
	}
}

// checkCloser refuses a close that the Stream's owner doesn't allow. A Workflow Stream closes
// only as a run of its chain, and a client's Stream only from a client. The server's own close
// with the run chain comes from no caller.
func (s *Stream) checkCloser(producer *streamservice.Producer) error {
	kind := producer.GetKind()
	owner := ownerKind(s.GetOwner())
	switch owner {
	case streamservice.PRODUCER_KIND_WORKFLOW:
		// A Local Activity writes before its Workflow Task commits, and a failed task can't
		// take a close back, so only the run's close Activity or command closes.
		if kind == streamservice.PRODUCER_KIND_WORKFLOW {
			return nil
		}
	case streamservice.PRODUCER_KIND_CLIENT:
		if kind == streamservice.PRODUCER_KIND_CLIENT {
			return nil
		}
	case streamservice.PRODUCER_KIND_STANDALONE_ACTIVITY:
		// The host checks the Activity's task token.
		if kind == owner && producer.GetId() == s.GetOwner().GetId() {
			return nil
		}
	default:
	}
	return notOwner("A %v Producer can't close a Stream that a %v owns", kind, owner)
}

// ofRun says whether a Producer kind writes from a Workflow run, in its code or its Local
// Activities.
func ofRun(kind streamservice.ProducerKind) bool {
	return kind == streamservice.PRODUCER_KIND_WORKFLOW ||
		kind == streamservice.PRODUCER_KIND_LOCAL_ACTIVITY
}

// checkRunID refuses a Producer whose id doesn't carry its run, so a write can't claim a run in
// one field and show another in its id.
func checkRunID(producer *streamservice.Producer) error {
	run := encodePart(producer.GetRunId())
	id := producer.GetId()
	switch producer.GetKind() {
	case streamservice.PRODUCER_KIND_WORKFLOW:
		if producer.GetRunId() != "" && id == "wf:"+run {
			return nil
		}
	case streamservice.PRODUCER_KIND_LOCAL_ACTIVITY:
		if producer.GetRunId() != "" && strings.HasPrefix(id, "la:"+run+":") {
			return nil
		}
	case streamservice.PRODUCER_KIND_ACTIVITY:
		if producer.GetRunId() != "" && strings.HasPrefix(id, "act:"+run+":") {
			return nil
		}
	default:
		return nil
	}
	return notOwner("The Producer id %q doesn't carry its run %q", id, producer.GetRunId())
}

// chainRun says whether runID is an open run of the chain that a Workflow Stream nests in. The
// run is on the Stream's shard, since the chain has one Workflow id.
func (h *handler) chainRun(ctx context.Context, t target, runID string) (bool, error) {
	resp, err := h.historyClient.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId: t.key.NamespaceID,
		Execution:   &commonpb.WorkflowExecution{WorkflowId: t.key.BusinessID, RunId: runID},
	})
	if missingRun(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return resp.GetFirstExecutionRunId() == t.key.RunID &&
		resp.GetWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, nil
}

// nextRun says whether next is the run that follows run in the chain, so only a retry run
// withdraws its previous run.
func (h *handler) nextRun(ctx context.Context, t target, run, next string) (bool, error) {
	req := &historyservice.DescribeMutableStateRequest{
		NamespaceId:     t.key.NamespaceID,
		Execution:       &commonpb.WorkflowExecution{WorkflowId: t.key.BusinessID, RunId: run},
		SkipForceReload: true,
	}
	resp, err := h.historyClient.DescribeMutableState(ctx, req)
	if missingRun(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	info := resp.GetDatabaseMutableState().GetExecutionInfo()
	return info.GetFirstExecutionRunId() == t.key.RunID && info.GetNewExecutionRunId() == next, nil
}

// checkChain refuses a write of a Workflow Stream whose Workflow or Local Activity Producer isn't
// an open run of the chain, or whose withdraw_run isn't the run before the writer's.
func (h *handler) checkChain(
	ctx context.Context,
	t target,
	producer *streamservice.Producer,
	withdrawRun string,
) error {
	if !t.workflow() {
		return nil
	}
	if !ofRun(producer.GetKind()) {
		if withdrawRun != "" {
			return notOwner("Only a Workflow run withdraws its previous run")
		}
		return nil
	}
	ok, err := h.chainRun(ctx, t, producer.GetRunId())
	if err != nil {
		return err
	}
	if !ok {
		return notOwner("The run %q is not an open run of the Stream's chain", producer.GetRunId())
	}
	if withdrawRun == "" {
		return nil
	}
	ok, err = h.nextRun(ctx, t, withdrawRun, producer.GetRunId())
	if err != nil {
		return err
	}
	if !ok {
		return notOwner("The run %q doesn't follow %q in the Stream's chain",
			producer.GetRunId(), withdrawRun)
	}
	return nil
}

// checkActivity refuses an Activity's write whose task token isn't the Activity's current
// attempt. The epoch stays the Producer's, (attempt scheduled time, attempt), and its attempt
// must be the token's.
func (h *handler) checkActivity(
	ctx context.Context,
	namespaceName string,
	namespaceID string,
	producer *streamservice.Producer,
	token []byte,
) error {
	if producer.GetKind() != streamservice.PRODUCER_KIND_ACTIVITY {
		return nil
	}
	if len(token) == 0 {
		return notOwner("An Activity's write needs its task token")
	}
	task, err := tasktoken.NewSerializer().Deserialize(token)
	if err != nil || len(task.GetComponentRef()) > 0 || task.GetActivityId() == "" {
		return invalid("The task token is not one of a Workflow's Activity")
	}
	prefix := "act:" + encodePart(task.GetRunId()) + ":" + encodePart(task.GetActivityId()) + ":"
	if task.GetNamespaceId() != namespaceID || producer.GetRunId() != task.GetRunId() ||
		!strings.HasPrefix(producer.GetId(), prefix) ||
		producer.GetEpoch().GetAttempt() != task.GetAttempt() {
		return notOwner("The Producer is not the Activity attempt of its task token")
	}
	execution := &commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      task.GetRunId(),
	}
	req := &historyservice.DescribeWorkflowExecutionRequest{
		NamespaceId: namespaceID,
		Request: &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: namespaceName,
			Execution: execution,
		},
	}
	resp, err := h.historyClient.DescribeWorkflowExecution(ctx, req)
	if missingRun(err) {
		return notOwner("The Activity's run is gone")
	}
	if err != nil {
		return err
	}
	for _, activity := range resp.GetPendingActivities() {
		if activity.GetActivityId() == task.GetActivityId() &&
			activity.GetAttempt() == task.GetAttempt() &&
			activity.GetState() != enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
			return nil
		}
	}
	return notOwner("The task token is not the Activity's current attempt")
}

func missingRun(err error) bool {
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return true
	}
	// A request can give a run id that is no run id at all.
	_, ok := errors.AsType[*serviceerror.InvalidArgument](err)
	return ok
}
