package streamnotifier

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
)

var Enabled = dynamicconfig.NewNamespaceBoolSetting(
	"streamnotifier.enabled",
	false,
	`Experimental. Serves the stream notifier RPCs (AttachStreamCallback, DetachStreamCallback,
NotifyStream, DescribeStreamNotifier), which hand a stream's progress and its close to the Nexus
callbacks attached to it.`,
)

var MaxCallbacks = dynamicconfig.NewNamespaceIntSetting(
	"streamnotifier.maxCallbacks",
	100,
	`The maximum number of callbacks one stream notifier holds. An attach beyond it is refused.`,
)

var IdleTimeout = dynamicconfig.NewNamespaceDurationSetting(
	"streamnotifier.idleTimeout",
	7*24*time.Hour,
	`How long an open stream notifier waits for an attach or a notification before it closes the
stream and completes its callbacks with a failure, so an abandoned stream does not hold callbacks
forever.`,
)

var ClosedRetention = dynamicconfig.NewNamespaceDurationSetting(
	"streamnotifier.closedRetention",
	24*time.Hour,
	`How long a closed stream notifier keeps completing callbacks attached late before its execution
completes and the namespace retention applies. An attach after that is refused.`,
)

// Config holds the stream notifier settings.
type Config struct {
	Enabled         dynamicconfig.BoolPropertyFnWithNamespaceFilter
	ChasmEnabled    dynamicconfig.BoolPropertyFnWithNamespaceFilter
	MaxCallbacks    dynamicconfig.IntPropertyFnWithNamespaceFilter
	IdleTimeout     dynamicconfig.DurationPropertyFnWithNamespaceFilter
	ClosedRetention dynamicconfig.DurationPropertyFnWithNamespaceFilter
	// The close result becomes the operation's result, so it takes the payload blob limits.
	BlobSizeLimitError dynamicconfig.IntPropertyFnWithNamespaceFilter
	BlobSizeLimitWarn  dynamicconfig.IntPropertyFnWithNamespaceFilter
}

func configProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		Enabled:            Enabled.Get(dc),
		ChasmEnabled:       dynamicconfig.EnableChasm.Get(dc),
		MaxCallbacks:       MaxCallbacks.Get(dc),
		IdleTimeout:        IdleTimeout.Get(dc),
		ClosedRetention:    ClosedRetention.Get(dc),
		BlobSizeLimitError: dynamicconfig.BlobSizeLimitError.Get(dc),
		BlobSizeLimitWarn:  dynamicconfig.BlobSizeLimitWarn.Get(dc),
	}
}
