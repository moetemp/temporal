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
	`How long an open stream notifier waits for an attach or a notification before it fails the
callbacks attached to it, so an abandoned stream does not hold callers forever. The stream stays
open: later notifications and attaches work as before. A notifier that goes idle with no callbacks
ends, and the next attach or notification starts a new one.`,
)

var OwnerCheckInterval = dynamicconfig.NewNamespaceDurationSetting(
	"streamnotifier.ownerCheckInterval",
	5*time.Minute,
	`How often a stream notifier with callbacks attached checks whether the stream's owner Workflow
ended. If its run chain closed without closing the stream, the notifier closes it and fails the
callbacks.`,
)

var OwnerCloseGrace = dynamicconfig.NewNamespaceDurationSetting(
	"streamnotifier.ownerCloseGrace",
	time.Minute,
	`How long the owner check leaves a stream alone after its owner's run completed, failed or was
canceled. The Worker that ran the final task closes the stream with the Workflow's own result, and
the notifier keeps the first close, so the check closes it only once this passed. The default
matches how long Core retries its notifier close. A terminated or timed-out owner has no Worker
close, so the check closes its stream at once.`,
)

var ClosedRetention = dynamicconfig.NewNamespaceDurationSetting(
	"streamnotifier.closedRetention",
	7*24*time.Hour,
	`How long a closed stream notifier keeps completing callbacks attached late before its execution
completes and the namespace retention applies. An attach after that is refused. Don't set it
shorter than the stream store's retention, or a reader that starts late fails while the store
still holds the records it would read. The default matches the default store retention.`,
)

// Config holds the stream notifier settings.
type Config struct {
	Enabled            dynamicconfig.BoolPropertyFnWithNamespaceFilter
	ChasmEnabled       dynamicconfig.BoolPropertyFnWithNamespaceFilter
	MaxCallbacks       dynamicconfig.IntPropertyFnWithNamespaceFilter
	IdleTimeout        dynamicconfig.DurationPropertyFnWithNamespaceFilter
	ClosedRetention    dynamicconfig.DurationPropertyFnWithNamespaceFilter
	OwnerCheckInterval dynamicconfig.DurationPropertyFnWithNamespaceFilter
	OwnerCloseGrace    dynamicconfig.DurationPropertyFnWithNamespaceFilter
	MaxIDLength        dynamicconfig.IntPropertyFn
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
		OwnerCheckInterval: OwnerCheckInterval.Get(dc),
		OwnerCloseGrace:    OwnerCloseGrace.Get(dc),
		MaxIDLength:        dynamicconfig.MaxIDLengthLimit.Get(dc),
		BlobSizeLimitError: dynamicconfig.BlobSizeLimitError.Get(dc),
		BlobSizeLimitWarn:  dynamicconfig.BlobSizeLimitWarn.Get(dc),
	}
}
