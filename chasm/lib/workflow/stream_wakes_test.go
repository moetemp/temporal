package workflow

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type streamWakeTest struct {
	t          *testing.T
	wf         *Workflow
	ctx        chasm.MutableContext
	timeSource *clock.EventTimeSource
}

func newStreamWakeTest(t *testing.T) *streamWakeTest {
	logger := log.NewTestLogger()
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewLibrary(NewRegistry())))
	require.NoError(t, registry.Register(&callback.Library{}))
	workflowKey := definition.NewWorkflowKey("namespace-id", "workflow-id", "run-id")
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Unix(1700000000, 0).UTC())
	nodeBackend := &chasm.MockNodeBackend{
		HandleNextTransitionCount: func() int64 { return 2 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleGetWorkflowKey:      func() definition.WorkflowKey { return workflowKey },
		HandleNow:                 timeSource.Now,
	}
	root := chasm.NewEmptyTree(
		registry,
		nodeBackend,
		chasm.DefaultPathEncoder,
		logger,
		metrics.NoopMetricsHandler,
	)
	wf := &Workflow{MSPointer: chasm.NewMSPointer(nodeBackend)}
	require.NoError(t, root.SetRootComponent(wf))
	return &streamWakeTest{
		t:          t,
		wf:         wf,
		ctx:        chasm.NewMutableContext(context.Background(), root),
		timeSource: timeSource,
	}
}

func (st *streamWakeTest) record(wake *streampb.StreamWake) {
	st.t.Helper()
	require.NoError(st.t, st.wf.RecordStreamWake(st.ctx, wake, 32))
}

func wakeOf(subscription string, counter int64) *streampb.StreamWake {
	return &streampb.StreamWake{
		SubscriptionId: subscription,
		Position:       fmt.Sprintf("p%d", counter),
		Counter:        counter,
		Metadata:       map[string]string{"n": fmt.Sprint(counter)},
	}
}

func TestStreamWakesFoldPerReader(t *testing.T) {
	st := newStreamWakeTest(t)
	require.False(t, st.wf.HasPendingStreamWakes(st.ctx))
	st.record(wakeOf("b", 3))
	st.record(wakeOf("a", 5))
	st.record(wakeOf("a", 4))
	st.record(&streampb.StreamWake{SubscriptionId: "a", Recheck: true})
	require.True(t, st.wf.HasPendingStreamWakes(st.ctx))

	taken := st.wf.TakeStreamWakes(st.ctx)
	require.Len(t, taken, 2, "one wake per reader")
	require.Equal(t, "a", taken[0].GetSubscriptionId(), "in subscription ID order")
	require.Equal(t, int64(5), taken[0].GetCounter(), "the highest counter wins")
	require.Equal(t, "p5", taken[0].GetPosition())
	require.Equal(t, map[string]string{"n": "5"}, taken[0].GetMetadata())
	require.True(t, taken[0].GetRecheck(), "a recheck stays set")
	require.Equal(t, int64(3), taken[1].GetCounter())
	require.False(t, st.wf.HasPendingStreamWakes(st.ctx))
	require.Empty(t, st.wf.TakeStreamWakes(st.ctx), "the event took them")
}

func TestStreamWakeCloseAndResubscribeStaySet(t *testing.T) {
	st := newStreamWakeTest(t)
	st.record(&streampb.StreamWake{
		SubscriptionId: "a",
		Counter:        7,
		Closed:         true,
		CloseResult:    &commonpb.Payload{Data: []byte("done")},
	})
	st.record(&streampb.StreamWake{
		SubscriptionId: "a",
		Closed:         true,
		CloseFailure:   &failurepb.Failure{Message: "later"},
	})
	st.record(&streampb.StreamWake{SubscriptionId: "a", Resubscribe: true})
	taken := st.wf.TakeStreamWakes(st.ctx)
	require.Len(t, taken, 1)
	require.True(t, taken[0].GetClosed())
	require.Equal(t, []byte("done"), taken[0].GetCloseResult().GetData(), "the first close wins")
	require.Nil(t, taken[0].GetCloseFailure())
	require.True(t, taken[0].GetResubscribe())
	require.Equal(t, int64(7), taken[0].GetCounter())
}

func TestStreamWakeOnAScheduledTaskGetsOneFollowUp(t *testing.T) {
	st := newStreamWakeTest(t)
	st.record(wakeOf("a", 1))
	require.Len(t, st.wf.TakeStreamWakes(st.ctx), 1)
	require.True(t, st.wf.HasScheduledStreamWakes(st.ctx))

	// The task is scheduled and not started: newer wakes fold into it and ask for no task.
	st.record(wakeOf("a", 2))
	st.record(wakeOf("a", 3))
	require.False(t, st.wf.HasPendingStreamWakes(st.ctx))

	// Once the task starts, what folded into it waits for one follow-up task.
	st.wf.ClearScheduledStreamWakes(st.ctx)
	require.True(t, st.wf.HasPendingStreamWakes(st.ctx))
	require.False(t, st.wf.HasScheduledStreamWakes(st.ctx))
	taken := st.wf.TakeStreamWakes(st.ctx)
	require.Len(t, taken, 1)
	require.Equal(t, int64(3), taken[0].GetCounter())

	// After that task started, a new wake needs a task of its own.
	st.wf.ClearScheduledStreamWakes(st.ctx)
	st.record(wakeOf("a", 4))
	require.True(t, st.wf.HasPendingStreamWakes(st.ctx))
}

func TestStreamWakesWaitTheMinInterval(t *testing.T) {
	st := newStreamWakeTest(t)
	require.True(t, st.wf.StreamWakesReadyAt(st.ctx, time.Second).IsZero(),
		"the first wake schedules a task at once")
	st.record(wakeOf("a", 1))
	carried := st.timeSource.Now()
	st.wf.TakeStreamWakes(st.ctx)
	st.wf.ClearScheduledStreamWakes(st.ctx)
	require.Equal(t, carried.Add(time.Second), st.wf.StreamWakesReadyAt(st.ctx, time.Second))

	st.record(wakeOf("a", 2))
	readyAt := carried.Add(time.Second)
	st.wf.HoldStreamWakes(st.ctx, readyAt)
	require.True(t, st.wf.streamWakesHeld(st.ctx))
	st.wf.HoldStreamWakes(st.ctx, readyAt.Add(time.Minute))
	require.Equal(t, readyAt, st.wf.streamWakeState(st.ctx).GetIndex().GetReleaseTime().AsTime(),
		"an armed release that comes sooner covers a later hold")
	st.wf.releaseStreamWakes(st.ctx)
	require.False(t, st.wf.streamWakesHeld(st.ctx))
	require.True(t, st.wf.HasPendingStreamWakes(st.ctx), "the release lets the wake schedule")
}

func TestStreamWakeSourcesAreBounded(t *testing.T) {
	st := newStreamWakeTest(t)
	for i := range 3 {
		require.NoError(t, st.wf.RecordStreamWake(st.ctx, wakeOf(fmt.Sprint(i), 1), 3))
	}
	require.NoError(t, st.wf.RecordStreamWake(st.ctx, wakeOf("1", 2), 3),
		"a known reader is not counted again")
	var failed *serviceerror.FailedPrecondition
	require.ErrorAs(t, st.wf.RecordStreamWake(st.ctx, wakeOf("3", 1), 3), &failed)
	st.wf.TakeStreamWakes(st.ctx)
	require.ErrorAs(t, st.wf.RecordStreamWake(st.ctx, wakeOf("3", 1), 3), &failed,
		"a reader counts for the whole run")
}
