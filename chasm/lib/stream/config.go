package stream

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
)

var (
	Enabled = dynamicconfig.NewNamespaceBoolSetting(
		"stream.enabled",
		false,
		`Enables the Stream service, as System Nexus operations and as the frontend StreamService.`,
	)

	DefaultRetention = dynamicconfig.NewNamespaceDurationSetting(
		"stream.defaultRetention",
		7*24*time.Hour,
		`The Retention of a Stream whose create gives none. A Record older than the Retention is
dropped, and a closed Stream expires one Retention after its close.`,
	)

	SafetyTTL = dynamicconfig.NewNamespaceDurationSetting(
		"stream.safetyTTL",
		90*24*time.Hour,
		`How long an open Stream that a Workflow created lives after its last write. An open
Stream that a client or an Activity created lives one Retention after its last write.`,
	)

	MaxStoredBytes = dynamicconfig.NewNamespaceIntSetting(
		"stream.maxStoredBytes",
		4*1024*1024,
		`The most Record bytes a Stream keeps. The Records are in the Stream's mutable state, so an
append past the cap drops the oldest batches, as the Retention does.`,
	)

	MaxWorkflowStateBytes = dynamicconfig.NewNamespaceIntSetting(
		"stream.maxWorkflowStateBytes",
		6*1024*1024,
		`The most mutable state bytes a Workflow may reach through appends to its Streams. It stays
under limit.mutableStateSize.error, which terminates a run.`,
	)

	MaxBatchBytes = dynamicconfig.NewNamespaceIntSetting(
		"stream.maxBatchBytes",
		2*1024*1024,
		`The most bytes of one append.`,
	)

	MaxParkedReads = dynamicconfig.NewNamespaceIntSetting(
		"stream.maxParkedReads",
		1000,
		`The most Workflow reads that wait on one Stream at a time. A read past the cap fails with a
retryable error, so the caller backs off.`,
	)

	MaxProducers = dynamicconfig.NewNamespaceIntSetting(
		"stream.maxProducers",
		1000,
		`The most Producers whose epoch and sequence a Stream keeps. Past it, the Producer with the
oldest last batch is dropped.`,
	)
)

type Config struct {
	Enabled          dynamicconfig.BoolPropertyFnWithNamespaceFilter
	DefaultRetention dynamicconfig.DurationPropertyFnWithNamespaceFilter
	SafetyTTL        dynamicconfig.DurationPropertyFnWithNamespaceFilter
	MaxStoredBytes   dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxBatchBytes    dynamicconfig.IntPropertyFnWithNamespaceFilter

	MaxWorkflowStateBytes dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxParkedReads        dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxProducers          dynamicconfig.IntPropertyFnWithNamespaceFilter
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		Enabled:          Enabled.Get(dc),
		DefaultRetention: DefaultRetention.Get(dc),
		SafetyTTL:        SafetyTTL.Get(dc),
		MaxStoredBytes:   MaxStoredBytes.Get(dc),
		MaxBatchBytes:    MaxBatchBytes.Get(dc),

		MaxWorkflowStateBytes: MaxWorkflowStateBytes.Get(dc),
		MaxParkedReads:        MaxParkedReads.Get(dc),
		MaxProducers:          MaxProducers.Get(dc),
	}
}

// Limits are the namespace's settings that one transition needs, resolved by the caller.
type Limits struct {
	DefaultRetention time.Duration
	SafetyTTL        time.Duration
	MaxStoredBytes   int64
	MaxBatchBytes    int64
	MaxParkedReads   int
	MaxProducers     int
	// Set for a Workflow Stream only.
	MaxWorkflowStateBytes int64
}

// LimitsFor resolves the limits of a namespace.
func (c *Config) LimitsFor(namespaceName string) Limits {
	return Limits{
		DefaultRetention: c.DefaultRetention(namespaceName),
		SafetyTTL:        c.SafetyTTL(namespaceName),
		MaxStoredBytes:   int64(c.MaxStoredBytes(namespaceName)),
		MaxBatchBytes:    int64(c.MaxBatchBytes(namespaceName)),
		MaxParkedReads:   c.MaxParkedReads(namespaceName),
		MaxProducers:     c.MaxProducers(namespaceName),
	}
}
