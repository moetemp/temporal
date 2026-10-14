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
