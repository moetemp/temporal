package streamnotifier

import (
	"go.temporal.io/server/common/metrics"
)

var NotificationHeld = metrics.NewCounterDef(
	"stream_notifier_notification_held",
	metrics.WithDescription(
		"Stream notifications that came inside a debounce window and waited for its end.",
	),
)
var TrailingDelivery = metrics.NewCounterDef(
	"stream_notifier_trailing_delivery",
	metrics.WithDescription(
		"Deliveries at the end of a debounce window of the newest notification held in it.",
	),
)
var WaitBelowDelivered = metrics.NewCounterDef(
	"stream_notifier_wait_below_delivered",
	metrics.WithDescription(
		"Waits after a counter below one the callback already got, so the reader's store and the "+
			"notifier disagree.",
	),
)
