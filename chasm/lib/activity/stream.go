package activity

import (
	"go.temporal.io/api/streamservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/chasm/lib/stream"
	"go.temporal.io/server/common/tasktoken"
)

// A Standalone Activity owns its Streams. They nest in the Activity's execution, so a call routes
// by the activity id, and they close in the transition that ends the Activity, with its status.
// A retry is the same execution, so a Stream goes on across attempts.

var _ stream.Host = (*Activity)(nil)

// NestedStreams returns the Activity's Streams. It implements [stream.Host].
func (a *Activity) NestedStreams() chasm.Map[string, *stream.Stream] {
	if a.Streams == nil {
		a.Streams = make(chasm.Map[string, *stream.Stream])
	}
	return a.Streams
}

// CheckOwner refuses a create that isn't the Activity's own, by its current attempt. It
// implements [stream.Host].
func (a *Activity) CheckOwner(
	ctx chasm.Context,
	owner *streamservice.Producer,
	token []byte,
) error {
	if !a.ownProducer(ctx, owner) {
		return stream.NewError(stream.ReasonNotOwner, "Only the Activity creates its Stream")
	}
	return a.checkAttempt(ctx, owner, token)
}

// CheckWriter refuses a write as the Activity's own Producer by any other than its current
// attempt. Other Producers write as on any Stream. It implements [stream.Host].
func (a *Activity) CheckWriter(
	ctx chasm.Context,
	producer *streamservice.Producer,
	token []byte,
) error {
	if !a.ownProducer(ctx, producer) {
		return nil
	}
	return a.checkAttempt(ctx, producer, token)
}

// ownProducer says whether a Producer is this Activity's, by the id that the crate makes.
func (a *Activity) ownProducer(ctx chasm.Context, producer *streamservice.Producer) bool {
	key := ctx.ExecutionKey()
	id := "sa:" + stream.EncodeIDPart(key.BusinessID) + ":" + stream.EncodeIDPart(key.RunID)
	return producer.GetKind() == streamservice.PRODUCER_KIND_STANDALONE_ACTIVITY &&
		producer.GetId() == id
}

// checkAttempt checks that a task token is this Activity's current attempt, and that the
// Producer's epoch is that attempt. The epoch stays (attempt scheduled time, attempt).
func (a *Activity) checkAttempt(
	ctx chasm.Context,
	producer *streamservice.Producer,
	token []byte,
) error {
	notOwner := stream.NewError(stream.ReasonNotOwner,
		"The task token is not the Activity's current attempt")
	task, err := tasktoken.NewSerializer().Deserialize(token)
	if err != nil || len(task.GetComponentRef()) == 0 || task.GetAttempt() == ByIDTokenAttempt {
		return notOwner
	}
	ref, err := chasm.DeserializeComponentRef(task.GetComponentRef())
	key := ctx.ExecutionKey()
	if err != nil || ref.BusinessID != key.BusinessID || ref.RunID != key.RunID {
		return notOwner
	}
	if a.validateActivityTaskToken(ctx, task, key.NamespaceID, false) != nil ||
		producer.GetEpoch().GetAttempt() != task.GetAttempt() {
		return notOwner
	}
	return nil
}

// HoldsStreams says whether a Stream of the closed Activity is still readable, so the Activity
// outlives the namespace retention that would delete it. Each Stream closed with the Activity.
func (a *Activity) HoldsStreams(ctx chasm.Context) bool {
	now := ctx.Now(a)
	for _, field := range a.Streams {
		if s, ok := field.TryGet(ctx); ok {
			if until, held := s.HeldUntil(); held && until.After(now) {
				return true
			}
		}
	}
	return false
}

// endStreams closes the Activity's Streams with its end. Each terminal transition passes its
// status, since the transition sets the Activity's status after its apply function.
func (a *Activity) endStreams(ctx chasm.MutableContext, status activitypb.ActivityExecutionStatus) {
	if len(a.Streams) == 0 {
		return
	}
	end := stream.RunEnd{Status: closeStatus(status)}
	if status == activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED {
		if output := a.Outcome.Get(ctx).GetSuccessful().GetOutput().GetPayloads(); len(output) > 0 {
			end.Result = output[0]
		}
	} else {
		end.Failure = a.terminalFailure(ctx)
	}
	namespaceName := ctx.NamespaceEntry().Name().String()
	limits := activityContextFromChasm(ctx).config.StreamLimits(namespaceName)
	for _, field := range a.Streams {
		field.Get(ctx).EndOwner(ctx, end, limits)
	}
}

func closeStatus(status activitypb.ActivityExecutionStatus) streamservice.CloseStatus {
	switch status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED:
		return streamservice.CLOSE_STATUS_COMPLETED
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED:
		return streamservice.CLOSE_STATUS_CANCELED
	case activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED:
		return streamservice.CLOSE_STATUS_TERMINATED
	case activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
		return streamservice.CLOSE_STATUS_TIMED_OUT
	default:
		return streamservice.CLOSE_STATUS_FAILED
	}
}
