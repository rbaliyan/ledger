package bridge

import "go.opentelemetry.io/otel/metric"

// bridgeMetrics holds all OTel metric instruments for the Bridge.
type bridgeMetrics struct {
	pollTotal        metric.Int64Counter
	pollDuration     metric.Float64Histogram
	mutationsApplied metric.Int64Counter
	mutationsSkipped metric.Int64Counter
	mutationErrors   metric.Int64Counter
	lagSeconds       metric.Float64Histogram
}

func initBridgeMetrics(meter metric.Meter) (*bridgeMetrics, error) {
	m := &bridgeMetrics{}
	var err error

	m.pollTotal, err = meter.Int64Counter(
		"ledger.bridge.polls",
		metric.WithDescription("Total number of Bridge poll cycles"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	m.pollDuration, err = meter.Float64Histogram(
		"ledger.bridge.poll.duration",
		metric.WithDescription("Duration of each Bridge poll cycle"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	m.mutationsApplied, err = meter.Int64Counter(
		"ledger.bridge.mutations.applied",
		metric.WithDescription("Total number of mutations successfully applied to the sink"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	m.mutationsSkipped, err = meter.Int64Counter(
		"ledger.bridge.mutations.skipped",
		metric.WithDescription("Total number of mutations skipped (type excluded by configuration)"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	m.mutationErrors, err = meter.Int64Counter(
		"ledger.bridge.mutations.errors",
		metric.WithDescription("Total number of mutations that failed to apply"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	m.lagSeconds, err = meter.Float64Histogram(
		"ledger.bridge.lag",
		metric.WithDescription("Replication lag: age of the oldest mutation processed in each poll cycle"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}
