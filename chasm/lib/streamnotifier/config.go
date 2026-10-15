package streamnotifier

import (
	"time"

	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/dynamicconfig"
)

var Enabled = dynamicconfig.NewNamespaceBoolSetting(
	"streamnotifier.enabled",
	false,
	`Experimental. Serves the stream notifier RPCs (AttachStreamCallback, DetachStreamCallback,
NotifyStream, NotifyStreams, DescribeStreamNotifier, PollStreamNotifier), which hand a stream's
progress and its close to the Nexus callbacks attached to it.`,
)

var MaxCallbacks = dynamicconfig.NewNamespaceIntSetting(
	"streamnotifier.maxCallbacks",
	2000,
	`The maximum number of callbacks one stream notifier holds. A notifier is full when it holds
this many callbacks or when an attach would take its callbacks past
streamnotifier.maxCallbackBytes. A full notifier makes room for an attach by dropping the oldest
callbacks that already completed, and refuses the attach if that is not enough. The default matches
callback.maxPerExecution. A callback that carries a Temporal callback token takes about 1.2 KB, so
2,000 of them stay under the byte limit.`,
)

var MaxCallbackBytes = dynamicconfig.NewNamespaceIntSetting(
	"streamnotifier.maxCallbackBytes",
	4*1024*1024,
	`The maximum summed size, in bytes, of the callbacks one stream notifier holds. It works next to
streamnotifier.maxCallbacks: an attach that would take the callbacks past it is refused the same
way, after the notifier drops the oldest callbacks that already completed. Callers with callback
headers near frontend.callbackHeaderMaxLength reach this limit before the callback count limit. The
size of a callback grows after its attach, for example with the progress it delivers, so keep this
well under limit.mutableStateSize.error, which ends the notifier and fails every caller when the
notifier passes it.`,
)

var IdleTimeout = dynamicconfig.NewNamespaceDurationSetting(
	"streamnotifier.idleTimeout",
	7*24*time.Hour,
	`How long an open stream notifier waits for an attach or a notification before it fails the
callbacks attached to it, so an abandoned stream does not hold callers forever. The stream stays
open: later notifications and attaches work as before. A notifier that goes idle with no callbacks
ends, and the next attach or notification starts a new one.`,
)

var IdleTimeoutNoCallbacks = dynamicconfig.NewNamespaceDurationSetting(
	"streamnotifier.idleTimeoutNoCallbacks",
	time.Hour,
	`How long an open stream notifier with no callbacks waiting waits for an attach or a
notification before it ends. A poll starts a notifier when the stream has none, so a sleeping
client has a notifier to wake it. A poll that names the wrong first run starts one that nobody
notifies, and this ends it soon.`,
)

var PollMaxWait = dynamicconfig.NewNamespaceDurationSetting(
	"streamnotifier.pollMaxWait",
	time.Minute,
	`The longest PollStreamNotifier waits for a counter above the one it names, or for the close.
At the limit it returns with no change, and the client reads the stream again. That read finds
records whose notification was lost, so this also bounds how long a client sleeps after a
producer stopped before it notified.`,
)

var RecheckInitial = dynamicconfig.NewNamespaceDurationSetting(
	"streamnotifier.recheckInitial",
	time.Minute,
	`How long a stream notifier with Workflow readers waits with no notification before it asks
them to read the stream again. A producer that appended and stopped before it notified leaves a
record that this read finds. Each recheck doubles the wait up to streamnotifier.recheckMax, and a
notification sets it back. Zero turns rechecks off.`,
)

var RecheckMax = dynamicconfig.NewNamespaceDurationSetting(
	"streamnotifier.recheckMax",
	time.Hour,
	`The longest wait between two rechecks of a stream notifier's Workflow readers. An idle reader
gets about 30 rechecks a day with the defaults, each one Workflow Task and one read.`,
)

var OwnerCheckInterval = dynamicconfig.NewNamespaceDurationSetting(
	"streamnotifier.ownerCheckInterval",
	30*time.Second,
	`How often a stream notifier with callbacks attached checks whether the stream's owner Workflow
ended. If its run chain closed without closing the stream, the notifier closes it and fails the
callbacks. No Worker closes a stream when its owner's chain ends, so this interval plus
streamnotifier.ownerCloseGrace bounds how long waiting callbacks wait for an implicit close. Each
check costs one describe of the owner per interval for each notifier with callbacks attached.`,
)

var OwnerCloseGrace = dynamicconfig.NewNamespaceDurationSetting(
	"streamnotifier.ownerCloseGrace",
	time.Minute,
	`How long the owner check leaves a stream alone after its owner's run completed, failed or was
canceled. The Worker that ran the final task closes the stream with the Workflow's own result, and
the notifier keeps the first close, so the check closes it only once this passed. The default
matches how long Core retries its notifier close. A terminated or timed-out owner has no Worker
close, so the check closes its stream at once. For a stream that a Workflow Activity owns, the grace
starts at the first check that found the Activity no longer pending in its run.`,
)

var OwnerStartGrace = dynamicconfig.NewNamespaceDurationSetting(
	"streamnotifier.ownerStartGrace",
	time.Minute,
	`How long the owner check of a new stream notifier leaves alone an owner Workflow that does not
exist. A reader may subscribe before the owner starts, for example when one request starts the
Workflow and another reads its stream. Once this passed from the notifier's start, an owner that
does not exist closes the stream with a failure. Readers of the store give the owner the same
grace. A wrong Workflow ID waits this long before its callers fail.`,
)

var MaxNotifyBatch = dynamicconfig.NewNamespaceIntSetting(
	"streamnotifier.maxNotifyBatch",
	1000,
	`The most entries one NotifyStreams request may carry. The frontend applies the entries in
parallel, each on the notifier of its stream.`,
)

var DebounceWindow = dynamicconfig.NewNamespaceDurationSetting(
	"streamnotifier.debounceWindow",
	time.Second,
	`How often a stream notifier delivers progress to its callbacks. A notification after a quiet
window goes out at once and opens a window. Notifications inside the window update the counter
and wait. At the window's end the newest one goes out and opens the next window. So a busy stream
wakes each Workflow reader and each Nexus caller at most once per window. A poll returns as soon as
the counter passes the one it waits for, and a close goes out at once. Zero delivers every
notification at once.`,
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
	Enabled                dynamicconfig.BoolPropertyFnWithNamespaceFilter
	ChasmEnabled           dynamicconfig.BoolPropertyFnWithNamespaceFilter
	ActivityEnabled        dynamicconfig.BoolPropertyFnWithNamespaceFilter
	MaxCallbacks           dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxCallbackBytes       dynamicconfig.IntPropertyFnWithNamespaceFilter
	IdleTimeout            dynamicconfig.DurationPropertyFnWithNamespaceFilter
	IdleTimeoutNoCallbacks dynamicconfig.DurationPropertyFnWithNamespaceFilter
	PollMaxWait            dynamicconfig.DurationPropertyFnWithNamespaceFilter
	RecheckInitial         dynamicconfig.DurationPropertyFnWithNamespaceFilter
	RecheckMax             dynamicconfig.DurationPropertyFnWithNamespaceFilter
	ClosedRetention        dynamicconfig.DurationPropertyFnWithNamespaceFilter
	OwnerCheckInterval     dynamicconfig.DurationPropertyFnWithNamespaceFilter
	OwnerCloseGrace        dynamicconfig.DurationPropertyFnWithNamespaceFilter
	OwnerStartGrace        dynamicconfig.DurationPropertyFnWithNamespaceFilter
	MaxNotifyBatch         dynamicconfig.IntPropertyFnWithNamespaceFilter
	DebounceWindow         dynamicconfig.DurationPropertyFnWithNamespaceFilter
	MaxIDLength            dynamicconfig.IntPropertyFn
	// The close result becomes the operation's result, so it takes the payload blob limits.
	BlobSizeLimitError dynamicconfig.IntPropertyFnWithNamespaceFilter
	BlobSizeLimitWarn  dynamicconfig.IntPropertyFnWithNamespaceFilter
}

func configProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		Enabled:                Enabled.Get(dc),
		ChasmEnabled:           dynamicconfig.EnableChasm.Get(dc),
		ActivityEnabled:        activity.Enabled.Get(dc),
		MaxCallbacks:           MaxCallbacks.Get(dc),
		MaxCallbackBytes:       MaxCallbackBytes.Get(dc),
		IdleTimeout:            IdleTimeout.Get(dc),
		IdleTimeoutNoCallbacks: IdleTimeoutNoCallbacks.Get(dc),
		PollMaxWait:            PollMaxWait.Get(dc),
		RecheckInitial:         RecheckInitial.Get(dc),
		RecheckMax:             RecheckMax.Get(dc),
		ClosedRetention:        ClosedRetention.Get(dc),
		OwnerCheckInterval:     OwnerCheckInterval.Get(dc),
		OwnerCloseGrace:        OwnerCloseGrace.Get(dc),
		OwnerStartGrace:        OwnerStartGrace.Get(dc),
		MaxNotifyBatch:         MaxNotifyBatch.Get(dc),
		DebounceWindow:         DebounceWindow.Get(dc),
		MaxIDLength:            dynamicconfig.MaxIDLengthLimit.Get(dc),
		BlobSizeLimitError:     dynamicconfig.BlobSizeLimitError.Get(dc),
		BlobSizeLimitWarn:      dynamicconfig.BlobSizeLimitWarn.Get(dc),
	}
}
