package bridge

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// WithTracesEnabled enables or disables per-poll OpenTelemetry spans (disabled by default).
func WithTracesEnabled(enabled bool) Option {
	return func(o *options) { o.enableTraces = enabled }
}

// WithMetricsEnabled enables or disables OpenTelemetry metrics (disabled by default).
// When enabled the following instruments are registered:
//
//   - ledger.bridge.polls (counter) — total poll cycles, attrs: bridge.name, status
//   - ledger.bridge.poll.duration (histogram, s) — per-cycle latency
//   - ledger.bridge.mutations.applied (counter) — mutations written to sink, attrs: mutation.type
//   - ledger.bridge.mutations.skipped (counter) — mutations dropped by config, attrs: mutation.type
//   - ledger.bridge.mutations.errors (counter) — mutations that failed to apply, attrs: mutation.type
//   - ledger.bridge.lag (histogram, s) — replication lag (age of oldest mutation per poll)
func WithMetricsEnabled(enabled bool) Option {
	return func(o *options) { o.enableMetrics = enabled }
}

// WithTracer sets a custom tracer instead of using the global OTel provider.
func WithTracer(t trace.Tracer) Option {
	return func(o *options) { o.tracer = t }
}

// WithMeter sets a custom meter instead of using the global OTel provider.
func WithMeter(m metric.Meter) Option {
	return func(o *options) { o.meter = m }
}

// WithTracerName sets the tracer name. Defaults to "github.com/rbaliyan/ledger/bridge".
func WithTracerName(name string) Option {
	return func(o *options) { o.tracerName = name }
}

// WithMeterName sets the meter name. Defaults to "github.com/rbaliyan/ledger/bridge".
func WithMeterName(name string) Option {
	return func(o *options) { o.meterName = name }
}
