package streamnotifier

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	streamnotifierpb "go.temporal.io/server/chasm/lib/streamnotifier/gen/streamnotifierpb/v1"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type expiryTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config
}

type expiryTaskHandlerOptions struct {
	fx.In

	Config *Config
}

func newExpiryTaskHandler(opts expiryTaskHandlerOptions) *expiryTaskHandler {
	return &expiryTaskHandler{config: opts.Config}
}

// Validate keeps only the expiry armed by the latest activity.
func (h *expiryTaskHandler) Validate(
	_ chasm.Context,
	n *StreamNotifier,
	_ chasm.TaskInvocation,
	task *streamnotifierpb.ExpiryTask,
) (bool, error) {
	return !n.Expired &&
		n.GetLastActivityTime().AsTime().Equal(task.GetLastActivityTime().AsTime()), nil
}

// Execute fails the callers of an idle open stream and keeps the stream open, or ends a notifier
// that is idle with nothing to wait on, or ends a closed notifier once its closed retention passed.
func (h *expiryTaskHandler) Execute(
	ctx chasm.MutableContext,
	n *StreamNotifier,
	_ chasm.TaskAttributes,
	_ *streamnotifierpb.ExpiryTask,
) error {
	if n.Closed {
		n.Expired = true
		return nil
	}
	failed, err := n.failIdleCallbacks(ctx)
	if err != nil {
		return err
	}
	if !failed {
		n.Expired = true
		return nil
	}
	// The failed callbacks still need their completions delivered, so the notifier stays.
	n.touch(ctx, h.config.IdleTimeout(ctx.NamespaceEntry().Name().String()))
	return nil
}

type ownerCheckTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*streamnotifierpb.OwnerCheckTask]
	config            *Config
	owners            *ownerDescriber
	namespaceRegistry namespace.Registry
}

type ownerCheckTaskHandlerOptions struct {
	fx.In

	Config            *Config
	Owners            *ownerDescriber
	NamespaceRegistry namespace.Registry
}

func newOwnerCheckTaskHandler(opts ownerCheckTaskHandlerOptions) *ownerCheckTaskHandler {
	return &ownerCheckTaskHandler{
		config:            opts.Config,
		owners:            opts.Owners,
		namespaceRegistry: opts.NamespaceRegistry,
	}
}

// Validate keeps only the latest armed check of an open notifier.
func (h *ownerCheckTaskHandler) Validate(
	_ chasm.Context,
	n *StreamNotifier,
	_ chasm.TaskInvocation,
	task *streamnotifierpb.OwnerCheckTask,
) (bool, error) {
	return !n.Closed && !n.Expired &&
		n.GetOwnerCheckTime().AsTime().Equal(task.GetScheduledTime().AsTime()), nil
}

// Execute describes the stream's owner. If the owner ended, the stream closes with the owner's
// outcome. Otherwise the next check is armed while callbacks still wait.
func (h *ownerCheckTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *streamnotifierpb.OwnerCheckTask,
) error {
	stream, err := chasm.ReadComponent(
		ctx,
		ref,
		func(
			n *StreamNotifier,
			_ chasm.Context,
			_ struct{},
		) (*streamnotifierpb.NotifierState, error) {
			return n.NotifierState, nil
		},
		struct{}{},
	)
	if err != nil {
		return err
	}
	ns, err := h.namespaceRegistry.GetNamespaceByID(namespace.ID(ref.NamespaceID))
	if err != nil {
		return err
	}
	end, err := h.owners.ended(
		ctx,
		ns.ID().String(),
		ns.Name().String(),
		stream.GetStreamRef(),
	)
	if err != nil {
		return err
	}
	_, _, err = chasm.UpdateComponent(
		ctx,
		ref,
		func(n *StreamNotifier, mctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			if n.Closed || !n.GetOwnerCheckTime().AsTime().Equal(task.GetScheduledTime().AsTime()) {
				return struct{}{}, nil
			}
			n.OwnerCheckTime = nil
			// A Workflow Activity has no close time of its own, so its grace starts at the first
			// check that saw it gone. Seeing it pending again resets that, since it may have been
			// gone only because it was not scheduled yet.
			closeTime := end.closeTime
			if !end.ended {
				n.OwnerEndSeenTime = nil
			} else if end.workerCloses && closeTime == nil &&
				n.GetStreamRef().GetOwnerKind() == enumspb.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY {
				if n.OwnerEndSeenTime == nil {
					n.OwnerEndSeenTime = timestamppb.New(mctx.Now(n))
				}
				seen := n.OwnerEndSeenTime.AsTime()
				closeTime = &seen
			}
			// The Worker that ran the final task closes the stream with the Workflow's own result,
			// and the first close wins, so a just-closed run is left to it for the grace.
			waiting := end.workerCloses && closeTime != nil &&
				mctx.Now(n).Before(closeTime.Add(h.config.OwnerCloseGrace(ns.Name().String())))
			// A reader may subscribe before its owner starts, so a missing owner gets a grace from
			// the notifier's start.
			startGrace := h.config.OwnerStartGrace(ns.Name().String())
			if end.notFound && n.StartTime != nil &&
				mctx.Now(n).Before(n.StartTime.AsTime().Add(startGrace)) {
				waiting = true
			}
			if end.ended && !waiting {
				if err := n.close(mctx, nil, end.failure); err != nil {
					return struct{}{}, err
				}
				n.touch(mctx, h.config.ClosedRetention(ns.Name().String()))
				return struct{}{}, nil
			}
			n.armOwnerCheck(mctx, h.config.OwnerCheckInterval(ns.Name().String()))
			return struct{}{}, nil
		},
		struct{}{},
	)
	return err
}

type debounceTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config
}

type debounceTaskHandlerOptions struct {
	fx.In

	Config *Config
}

func newDebounceTaskHandler(opts debounceTaskHandlerOptions) *debounceTaskHandler {
	return &debounceTaskHandler{config: opts.Config}
}

// Validate keeps only the task of the open window of an open notifier.
func (h *debounceTaskHandler) Validate(
	_ chasm.Context,
	n *StreamNotifier,
	_ chasm.TaskInvocation,
	task *streamnotifierpb.DebounceTask,
) (bool, error) {
	return !n.Closed && !n.Expired &&
		n.GetDebounceUntil().AsTime().Equal(task.GetScheduledTime().AsTime()), nil
}

// Execute delivers the newest notification that came inside the window that ended.
func (h *debounceTaskHandler) Execute(
	ctx chasm.MutableContext,
	n *StreamNotifier,
	_ chasm.TaskAttributes,
	_ *streamnotifierpb.DebounceTask,
) error {
	return n.endDebounceWindow(ctx, h.config.DebounceWindow(ctx.NamespaceEntry().Name().String()))
}

type recheckTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config
}

type recheckTaskHandlerOptions struct {
	fx.In

	Config *Config
}

func newRecheckTaskHandler(opts recheckTaskHandlerOptions) *recheckTaskHandler {
	return &recheckTaskHandler{config: opts.Config}
}

// Validate keeps only the latest armed recheck of an open notifier.
func (h *recheckTaskHandler) Validate(
	_ chasm.Context,
	n *StreamNotifier,
	_ chasm.TaskInvocation,
	task *streamnotifierpb.RecheckTask,
) (bool, error) {
	return !n.Closed && !n.Expired &&
		n.GetRecheckTime().AsTime().Equal(task.GetScheduledTime().AsTime()), nil
}

// Execute asks the waiting Workflow readers to read again, since no notification came for the
// quiet period. A producer that appended and stopped before it notified leaves the record in the
// store, and the read finds it.
func (h *recheckTaskHandler) Execute(
	ctx chasm.MutableContext,
	n *StreamNotifier,
	_ chasm.TaskAttributes,
	_ *streamnotifierpb.RecheckTask,
) error {
	return n.recheck(ctx, h.config.RecheckMax(ctx.NamespaceEntry().Name().String()))
}
