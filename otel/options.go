// Package otel provides OpenTelemetry instrumentation for the ledger library.
package otel

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type options struct {
	tracerName    string
	meterName     string
	backendName   string
	enableTraces  bool
	enableMetrics bool
	tracer        trace.Tracer
	meter         metric.Meter
}

func defaultOptions() options {
	return options{
		tracerName:    "github.com/rbaliyan/ledger",
		meterName:     "github.com/rbaliyan/ledger",
		backendName:   "unknown",
		enableTraces:  false,
		enableMetrics: false,
	}
}

// Option configures the instrumented store.
type Option func(*options)

// WithTracerName sets the tracer name used when creating spans.
func WithTracerName(name string) Option {
	return func(o *options) { o.tracerName = name }
}

// WithMeterName sets the meter name used when creating instruments.
func WithMeterName(name string) Option {
	return func(o *options) { o.meterName = name }
}

// WithTracer sets a custom tracer instead of using the global provider.
func WithTracer(t trace.Tracer) Option {
	return func(o *options) { o.tracer = t }
}

// WithMeter sets a custom meter instead of using the global provider.
func WithMeter(m metric.Meter) Option {
	return func(o *options) { o.meter = m }
}

// WithBackendName sets the backend name recorded on all spans and metrics.
// Use values like "sqlite", "postgres", or "mongodb".
func WithBackendName(name string) Option {
	return func(o *options) { o.backendName = name }
}

// WithTracesEnabled enables or disables tracing (disabled by default).
func WithTracesEnabled(enabled bool) Option {
	return func(o *options) { o.enableTraces = enabled }
}

// WithMetricsEnabled enables or disables metrics (disabled by default).
func WithMetricsEnabled(enabled bool) Option {
	return func(o *options) { o.enableMetrics = enabled }
}
