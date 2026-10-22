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
		newCloseChainTaskHandler,
		newReadersCheckTaskHandler,
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
	// The frontend runs the operation processors of direct Nexus calls, and the Workflow
	// component nests Streams, so the frontend registry has the library with no handlers.
	fx.Invoke(func(config *Config, registry *chasm.Registry) error {
		return registry.Register(newLibrary(config, nil, nil, nil, nil, nil))
	}),
)
