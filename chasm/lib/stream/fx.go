package stream

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

var HistoryModule = fx.Module(
	"stream-history",
	fx.Provide(
		ConfigProvider,
		newTimerTaskHandler,
		newLibrary,
	),
	fx.Invoke(func(l *library, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)
