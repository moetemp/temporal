package stream

import (
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

type timerTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config
}

func newTimerTaskHandler(config *Config) *timerTaskHandler {
	return &timerTaskHandler{config: config}
}

func (h *timerTaskHandler) Validate(
	_ chasm.Context,
	s *Stream,
	_ chasm.TaskInvocation,
	task *streampb.StreamTimerTask,
) (bool, error) {
	return s.GetTimerDeadline() != nil &&
		s.GetTimerDeadline().AsTime().Equal(task.GetDeadline().AsTime()), nil
}

func (h *timerTaskHandler) Execute(
	ctx chasm.MutableContext,
	s *Stream,
	_ chasm.TaskAttributes,
	_ *streampb.StreamTimerTask,
) error {
	// Cleared first, so the transition arms a new task for the next deadline.
	s.TimerDeadline = nil
	s.ApplyDue(ctx, h.config.LimitsFor(ctx.NamespaceEntry().Name().String()))
	return nil
}
