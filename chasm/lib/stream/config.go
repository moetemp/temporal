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
append past the cap drops the oldest batches, as the Retention does. The append fails when an open
Workflow reader still needs them.`,
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

	MaxWorkflowReaders = dynamicconfig.NewNamespaceIntSetting(
		"stream.maxWorkflowReaders",
		100,
		`The most open Workflow runs whose read floor one Stream keeps. A page of a new reader past
the cap fails with a retryable error, so the caller backs off.`,
	)

	ReaderCheckInterval = dynamicconfig.NewNamespaceDurationSetting(
		"stream.readerCheckInterval",
		5*time.Minute,
		`How often a Stream with read floors checks whether their runs are still open. The floor of
a closed run goes at the next check.`,
	)

	MaxWakeWait = dynamicconfig.NewNamespaceDurationSetting(
		"stream.maxWakeWait",
		7*24*time.Hour,
		`The longest wait of a Workflow's wake_only read, and of its wait for the create. A parked
read adds nothing to History while it waits, so an idle reader costs nothing. Other reads wait
at most 60 s.`,
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
	MaxWorkflowReaders    dynamicconfig.IntPropertyFnWithNamespaceFilter
	ReaderCheckInterval   dynamicconfig.DurationPropertyFnWithNamespaceFilter
	MaxWakeWait           dynamicconfig.DurationPropertyFnWithNamespaceFilter
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
		MaxWorkflowReaders:    MaxWorkflowReaders.Get(dc),
		ReaderCheckInterval:   ReaderCheckInterval.Get(dc),
		MaxWakeWait:           MaxWakeWait.Get(dc),
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
	MaxReaders       int
	ReaderCheck      time.Duration
	MaxWakeWait      time.Duration
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
		MaxReaders:       c.MaxWorkflowReaders(namespaceName),
		ReaderCheck:      c.ReaderCheckInterval(namespaceName),
		MaxWakeWait:      c.MaxWakeWait(namespaceName),
	}
}
