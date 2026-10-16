package streamnotifier

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	streamnotifierpb "go.temporal.io/server/chasm/lib/streamnotifier/gen/streamnotifierpb/v1"
	"go.uber.org/fx"
)

var HistoryModule = fx.Module(
	"streamnotifier-history",
	// The owner check describes an owner Activity on whichever history host owns it.
	fx.Provide(fx.Private, activitypb.NewActivityServiceLayeredClient),
	fx.Provide(
		configProvider,
		newOwnerDescriber,
		newHandler,
		newExpiryTaskHandler,
		newOwnerCheckTaskHandler,
		newRecheckTaskHandler,
		newDebounceTaskHandler,
		newLibrary,
		newSystemNexusHandler,
	),
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
