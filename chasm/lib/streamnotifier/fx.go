package streamnotifier

import (
	"go.temporal.io/server/chasm"
	streamnotifierpb "go.temporal.io/server/chasm/lib/streamnotifier/gen/streamnotifierpb/v1"
	"go.uber.org/fx"
)

var HistoryModule = fx.Module(
	"streamnotifier-history",
	fx.Provide(configProvider, newHandler, newExpiryTaskHandler, newOwnerCheckTaskHandler, newLibrary, newSystemNexusHandler),
	fx.Invoke(func(l *library, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)

var FrontendModule = fx.Module(
	"streamnotifier-frontend",
	fx.Provide(configProvider),
	fx.Provide(streamnotifierpb.NewStreamNotifierServiceLayeredClient),
	fx.Provide(NewFrontendHandler),
	fx.Provide(newComponentOnlyLibrary),
	fx.Invoke(func(l *componentOnlyLibrary, registry *chasm.Registry) error {
		// The frontend names the component in refs but runs none of its tasks.
		return registry.Register(l)
	}),
)
