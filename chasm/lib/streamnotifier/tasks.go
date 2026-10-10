package streamnotifier

import (
	"go.temporal.io/server/chasm"
	streamnotifierpb "go.temporal.io/server/chasm/lib/streamnotifier/gen/streamnotifierpb/v1"
	"go.uber.org/fx"
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
func (h *expiryTaskHandler) Validate(_ chasm.Context, n *StreamNotifier, _ chasm.TaskInvocation, task *streamnotifierpb.ExpiryTask) (bool, error) {
	return !n.Expired && n.GetLastActivityTime().AsTime().Equal(task.GetLastActivityTime().AsTime()), nil
}

// Execute closes an idle open stream, completing its callbacks with a failure, or ends a closed
// notifier's execution once its closed retention passed.
func (h *expiryTaskHandler) Execute(ctx chasm.MutableContext, n *StreamNotifier, _ chasm.TaskAttributes, _ *streamnotifierpb.ExpiryTask) error {
	if n.Closed {
		n.Expired = true
		return nil
	}
	if err := n.close(ctx, nil, true); err != nil {
		return err
	}
	n.touch(ctx, h.config.ClosedRetention(ctx.NamespaceEntry().Name().String()))
	return nil
}
