package stream

import (
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.uber.org/fx"
)

var HistoryModule = fx.Module(
	"stream-history",
	fx.Provide(
		ConfigProvider,
		newHandler,
		newGRPCHandler,
		newTimerTaskHandler,
		newLibrary,
	),
	fx.Invoke(func(l *library, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)

var FrontendModule = fx.Module(
	"stream-frontend",
	fx.Provide(ConfigProvider),
	fx.Provide(streampb.NewStreamServiceLayeredClient),
	fx.Provide(NewFrontendHandler),
)
