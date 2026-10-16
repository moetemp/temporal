package streamnotifier

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

var HistoryModule = fx.Module(
	"streamnotifier-history",
	fx.Provide(
		configProvider,
		newHandler,
		newExpiryTaskHandler,
		newOwnerCheckTaskHandler,
		newLibrary,
	),
	fx.Invoke(func(l *library, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)
