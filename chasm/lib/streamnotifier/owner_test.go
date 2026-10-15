package streamnotifier

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	apiactivitypb "go.temporal.io/api/activity/v1" //nolint:importas
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"google.golang.org/grpc"
)

// fakeActivityClient describes an owner Activity as the activity service does.
type fakeActivityClient struct {
	activitypb.ActivityServiceClient
	t *testing.T
	// newestRun is the run that a describe with no run ID answers. Empty means no run exists.
	newestRun string
	// olderRuns are earlier runs of the ID that a describe by run ID finds.
	olderRuns map[string]bool
	status    enumspb.ActivityExecutionStatus
	failure   *failurepb.Failure
	err       error
}

func (c *fakeActivityClient) DescribeActivityExecution(
	_ context.Context,
	req *activitypb.DescribeActivityExecutionRequest,
	_ ...grpc.CallOption,
) (*activitypb.DescribeActivityExecutionResponse, error) {
	fe := req.GetFrontendRequest()
	require.Equal(c.t, "namespace-id", req.GetNamespaceId())
	require.Equal(c.t, "ns", fe.GetNamespace())
	require.True(c.t, fe.GetIncludeOutcome(), "the outcome gives the close its failure")
	if c.err != nil {
		return nil, c.err
	}
	run := fe.GetRunId()
	if run == "" {
		run = c.newestRun
	}
	if run == "" || (run != c.newestRun && !c.olderRuns[run]) {
		return nil, serviceerror.NewNotFound("activity not found")
	}
	resp := &workflowservice.DescribeActivityExecutionResponse{
		RunId: run,
		Info: &apiactivitypb.ActivityExecutionInfo{
			ActivityId: fe.GetActivityId(),
			RunId:      run,
			Status:     c.status,
		},
	}
	if c.failure != nil {
		resp.Outcome = &apiactivitypb.ActivityExecutionOutcome{
			Value: &apiactivitypb.ActivityExecutionOutcome_Failure{Failure: c.failure},
		}
	}
	return &activitypb.DescribeActivityExecutionResponse{FrontendResponse: resp}, nil
}

// newActivityOwnerTest is a notifier test whose stream a running standalone Activity owns.
func newActivityOwnerTest(t *testing.T) *notifierTest {
	nt := newNotifierTest(t, 10)
	nt.ref = &streampb.StreamReference{
		OwnerKind:  enumspb.STREAM_OWNER_KIND_ACTIVITY,
		ActivityId: "act/1",
		RunId:      "run-1",
		Topic:      "tokens",
	}
	nt.activity.newestRun = "run-1"
	nt.activity.status = enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING
	return nt
}

func applicationFailure(message, failureType string) *failurepb.Failure {
	return &failurepb.Failure{
		Message: message,
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: failureType},
		},
	}
}

func TestActivityOwner(t *testing.T) {
	t.Run("BusinessIDsNameTheActivity", func(t *testing.T) {
		activity := &streampb.StreamReference{
			OwnerKind:  enumspb.STREAM_OWNER_KIND_ACTIVITY,
			ActivityId: "a/1",
			RunId:      "run-1",
			Topic:      "tokens",
		}
		require.Equal(t, "activity/a%2F1/run-1/tokens", BusinessID(activity))
		require.NotEqual(t, BusinessID(activity), BusinessID(&streampb.StreamReference{
			OwnerKind:  enumspb.STREAM_OWNER_KIND_WORKFLOW,
			WorkflowId: "a/1",
			RunId:      "run-1",
			Topic:      "tokens",
		}), "a Workflow with the same ID gets its own notifier")
	})

	t.Run("TheNotifierKeepsTheActivityRef", func(t *testing.T) {
		nt := newActivityOwnerTest(t)
		require.NoError(t, nt.attachWorkflow("a", "sub-a"))
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			ref := n.GetStreamRef()
			require.Equal(t, enumspb.STREAM_OWNER_KIND_ACTIVITY, ref.GetOwnerKind())
			require.Equal(t, "act/1", ref.GetActivityId())
			require.Empty(t, ref.GetWorkflowId())
			require.Equal(t, "run-1", ref.GetRunId())
		})
	})

	t.Run("AnAttachTakesAnyRunOfTheActivity", func(t *testing.T) {
		nt := newActivityOwnerTest(t)
		require.NoError(t, nt.attach("newest"))

		nt = newActivityOwnerTest(t)
		nt.activity.newestRun = "run-2"
		nt.activity.olderRuns = map[string]bool{"run-1": true}
		require.NoError(t, nt.attach("older"), "an older run keys its own notifier")

		nt = newActivityOwnerTest(t)
		nt.activity.newestRun = ""
		require.NoError(t, nt.attach("not-started"), "the owner check gives it the start grace")

		nt = newActivityOwnerTest(t)
		nt.activity.newestRun = "run-2"
		var invalid *serviceerror.InvalidArgument
		require.ErrorAs(t, nt.attach("unknown"), &invalid, "a run the ID never had is refused")

		nt = newActivityOwnerTest(t)
		nt.activity.err = serviceerror.NewUnavailable("try again")
		var unavailable *serviceerror.Unavailable
		require.ErrorAs(t, nt.attach("retry"), &unavailable)
	})

	t.Run("ARunningOwnerKeepsTheStreamOpen", func(t *testing.T) {
		for _, status := range []enumspb.ActivityExecutionStatus{
			enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
			enumspb.ACTIVITY_EXECUTION_STATUS_PAUSED,
		} {
			t.Run(status.String(), func(t *testing.T) {
				nt := newActivityOwnerTest(t)
				require.NoError(t, nt.attach("a"))
				nt.activity.status = status
				// A retry or a reset keeps the run, so the check finds the same run running.
				nt.activity.failure = applicationFailure("attempt 1 failed", "Retry")
				nt.runArmedOwnerCheck()
				require.False(t, nt.describe().GetClosed())
				nt.read(func(n *StreamNotifier, _ chasm.Context) {
					require.NotNil(t, n.GetOwnerCheckTime(), "the next check is armed")
				})
			})
		}
	})

	t.Run("AnEndedOwnerClosesTheStreamWithTheRunsOutcome", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			status  enumspb.ActivityExecutionStatus
			failure *failurepb.Failure
			state   nexus.OperationState
			// check is nil for a success.
			check func(*failurepb.Failure) bool
		}{
			{
				name:   "completed",
				status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
				state:  nexus.OperationStateSucceeded,
			},
			{
				name:    "failed",
				status:  enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				failure: applicationFailure("boom", "Boom"),
				state:   nexus.OperationStateFailed,
				check: func(f *failurepb.Failure) bool {
					return f.GetMessage() == "boom" &&
						f.GetApplicationFailureInfo().GetType() == "Boom"
				},
			},
			{
				name:   "failed with no failure",
				status: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				state:  nexus.OperationStateFailed,
				check: func(f *failurepb.Failure) bool {
					return f.GetApplicationFailureInfo().GetType() == ownerFailedType
				},
			},
			{
				name:   "canceled",
				status: enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED,
				failure: &failurepb.Failure{
					Message: "canceled by the client",
					FailureInfo: &failurepb.Failure_CanceledFailureInfo{
						CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
					},
				},
				state: nexus.OperationStateCanceled,
				check: func(f *failurepb.Failure) bool {
					return f.GetMessage() == "canceled by the client"
				},
			},
			{
				name:   "canceled with no failure",
				status: enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED,
				state:  nexus.OperationStateCanceled,
				check: func(f *failurepb.Failure) bool {
					return f.GetCanceledFailureInfo() != nil
				},
			},
			{
				name:   "terminated",
				status: enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
				failure: &failurepb.Failure{
					Message: "terminated by the operator",
					FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
						TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{},
					},
				},
				state: nexus.OperationStateFailed,
				check: func(f *failurepb.Failure) bool {
					return f.GetTerminatedFailureInfo() != nil &&
						f.GetMessage() == "terminated by the operator"
				},
			},
			{
				name:   "terminated with no failure",
				status: enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
				state:  nexus.OperationStateFailed,
				check: func(f *failurepb.Failure) bool {
					return f.GetTerminatedFailureInfo() != nil
				},
			},
			{
				name:   "timed out",
				status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				failure: &failurepb.Failure{
					Message: "activity timeout",
					FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
						TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
							TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
						},
					},
				},
				state: nexus.OperationStateFailed,
				check: func(f *failurepb.Failure) bool {
					return f.GetTimeoutFailureInfo().GetTimeoutType() ==
						enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE
				},
			},
			{
				name:   "timed out with no failure",
				status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				state:  nexus.OperationStateFailed,
				check: func(f *failurepb.Failure) bool {
					return f.GetTimeoutFailureInfo() != nil
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				nt := newActivityOwnerTest(t)
				require.NoError(t, nt.attach("a"))
				nt.activity.status = tc.status
				nt.activity.failure = tc.failure
				// No Worker closes after the run closed, so the check closes at once.
				nt.runArmedOwnerCheck()
				require.True(t, nt.describe().GetClosed())
				state, failure := nt.completionOf("a")
				require.Equal(t, tc.state, state)
				if tc.check == nil {
					require.Nil(t, failure)
					return
				}
				require.True(t, tc.check(failure), "%v", failure)
			})
		}
	})

	t.Run("AnExplicitCloseWins", func(t *testing.T) {
		nt := newActivityOwnerTest(t)
		require.NoError(t, nt.attach("a"))
		require.NoError(t, nt.notify(1, true))
		nt.activity.status = enumspb.ACTIVITY_EXECUTION_STATUS_FAILED
		nt.activity.failure = applicationFailure("boom", "Boom")
		nt.read(func(n *StreamNotifier, _ chasm.Context) {
			require.Nil(t, n.GetOwnerCheckTime(), "a closed stream has no owner check")
		})
		desc := nt.describe()
		require.True(t, desc.GetClosed())
		require.Nil(t, desc.GetCloseFailure())
		state, _ := nt.completionOf("a")
		require.Equal(t, nexus.OperationStateSucceeded, state)
	})

	t.Run("ANewerRunEndsTheOldRunsStream", func(t *testing.T) {
		nt := newActivityOwnerTest(t)
		require.NoError(t, nt.attach("a"))
		nt.activity.newestRun = "run-2"
		nt.activity.olderRuns = map[string]bool{"run-1": true}
		nt.runArmedOwnerCheck()
		require.True(t, nt.describe().GetClosed())
		_, failure := nt.completionOf("a")
		require.Equal(t, activityOwnerEndedFailure, failure.GetMessage())
	})

	t.Run("AMissingOwnerGetsTheStartGrace", func(t *testing.T) {
		nt := newActivityOwnerTest(t)
		nt.activity.newestRun = ""
		require.NoError(t, nt.attach("a"))
		nt.runArmedOwnerCheck()
		require.False(t, nt.describe().GetClosed(), "the Activity may not have started yet")

		nt.backdateStart(2 * time.Minute)
		nt.runArmedOwnerCheck()
		require.True(t, nt.describe().GetClosed(), "an Activity that never started closes it")
		_, failure := nt.completionOf("a")
		require.Equal(t, activityOwnerEndedFailure, failure.GetMessage())
	})

	t.Run("ALargeFailureKeepsItsTypeAndMessage", func(t *testing.T) {
		nt := newActivityOwnerTest(t)
		require.NoError(t, nt.attach("a"))
		large := applicationFailure("boom", "Boom")
		large.StackTrace = strings.Repeat("x", maxOwnerFailureBytes)
		nt.activity.status = enumspb.ACTIVITY_EXECUTION_STATUS_FAILED
		nt.activity.failure = large
		nt.runArmedOwnerCheck()
		_, failure := nt.completionOf("a")
		require.Equal(t, "boom", failure.GetMessage())
		require.Equal(t, "Boom", failure.GetApplicationFailureInfo().GetType())
		require.Empty(t, failure.GetStackTrace())
	})
}
