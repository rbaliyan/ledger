package otel

import "go.opentelemetry.io/otel/metric"

// storeMetrics holds all OTel metric instruments for the store.
type storeMetrics struct {
	operationCount   metric.Int64Counter
	errorCount       metric.Int64Counter
	operationLatency metric.Float64Histogram
	entriesAppended  metric.Int64Counter
	entriesRead      metric.Int64Counter
	entriesTrimmed   metric.Int64Counter
}

func initMetrics(meter metric.Meter) (*storeMetrics, error) {
	m := &storeMetrics{}
	var err error

	m.operationCount, err = meter.Int64Counter(
		"ledger.operations.total",
		metric.WithDescription("Total number of ledger store operations"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	m.errorCount, err = meter.Int64Counter(
		"ledger.errors.total",
		metric.WithDescription("Total number of ledger store operation errors"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	m.operationLatency, err = meter.Float64Histogram(
		"ledger.operation.duration",
		metric.WithDescription("Duration of ledger store operations in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	m.entriesAppended, err = meter.Int64Counter(
		"ledger.entries.appended",
		metric.WithDescription("Total number of entries appended to ledger streams"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	m.entriesRead, err = meter.Int64Counter(
		"ledger.entries.read",
		metric.WithDescription("Total number of entries read from ledger streams"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	m.entriesTrimmed, err = meter.Int64Counter(
		"ledger.entries.trimmed",
		metric.WithDescription("Total number of entries trimmed from ledger streams"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}
