// Package otel provides opt-in OpenTelemetry tracing and metrics for any
// ledger.Store implementation. Wrap an existing store with [WrapStore] to record
// spans and counters for Append, Read, Count, Trim, and other operations.
// Both traces and metrics are disabled by default; enable them with
// [WithTracesEnabled] and [WithMetricsEnabled].
package otel

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/rbaliyan/ledger"
)

// Compile-time interface checks.
var (
	_ ledger.Store[int64, []byte] = (*InstrumentedStore[int64, []byte])(nil)
	_ ledger.HealthChecker        = (*InstrumentedStore[int64, []byte])(nil)
)

// InstrumentedStore wraps a [ledger.Store] with OpenTelemetry tracing and metrics.
// I is the store ID type; P is the store-native payload type.
type InstrumentedStore[I comparable, P any] struct {
	store     ledger.Store[I, P]
	tracer    trace.Tracer
	meter     metric.Meter
	metrics   *storeMetrics
	opts      options
	storeType string // table or collection name, from Type() if available
}

// WrapStore wraps a [ledger.Store] with OpenTelemetry instrumentation.
// Both tracing and metrics are disabled by default — opt in with
// [WithTracesEnabled] and [WithMetricsEnabled].
//
// If the underlying store exposes a Type() string method (as all concrete
// backends do), the table or collection name is recorded on all spans and
// metrics as the "ledger.store_type" attribute.
func WrapStore[I comparable, P any](store ledger.Store[I, P], opts ...Option) (*InstrumentedStore[I, P], error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	is := &InstrumentedStore[I, P]{
		store: store,
		opts:  o,
	}

	if typer, ok := store.(interface{ Type() string }); ok {
		is.storeType = typer.Type()
	}

	if o.enableTraces {
		if o.tracer != nil {
			is.tracer = o.tracer
		} else {
			is.tracer = otel.Tracer(o.tracerName)
		}
	}

	if o.enableMetrics {
		var m metric.Meter
		if o.meter != nil {
			m = o.meter
		} else {
			m = otel.Meter(o.meterName)
		}
		is.meter = m

		metrics, err := initMetrics(m)
		if err != nil {
			return nil, err
		}
		is.metrics = metrics
	}

	return is, nil
}

// Unwrap returns the underlying store.
func (s *InstrumentedStore[I, P]) Unwrap() ledger.Store[I, P] {
	return s.store
}

// Append adds entries to the named stream.
func (s *InstrumentedStore[I, P]) Append(ctx context.Context, stream string, entries ...ledger.RawEntry[P]) ([]I, error) {
	if !s.opts.enableTraces {
		start := time.Now()
		ids, err := s.store.Append(ctx, stream, entries...)
		s.recordOperation(ctx, "append", stream, start, err)
		if err == nil {
			s.recordEntriesAppended(ctx, stream, int64(len(ids)))
		}
		return ids, err
	}

	attrs := append(s.commonAttributes(),
		attribute.String("ledger.stream", stream),
		attribute.Int("ledger.entry_count", len(entries)),
	)

	ctx, span := s.tracer.Start(ctx, "ledger.Append", trace.WithAttributes(attrs...))
	defer span.End()

	start := time.Now()
	ids, err := s.store.Append(ctx, stream, entries...)
	s.recordOperation(ctx, "append", stream, start, err)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
		span.SetAttributes(attribute.Int("ledger.appended_count", len(ids)))
		s.recordEntriesAppended(ctx, stream, int64(len(ids)))
	}

	return ids, err
}

// Read returns entries from the named stream.
func (s *InstrumentedStore[I, P]) Read(ctx context.Context, stream string, opts ...ledger.ReadOption) ([]ledger.StoredEntry[I, P], error) {
	if !s.opts.enableTraces {
		start := time.Now()
		entries, err := s.store.Read(ctx, stream, opts...)
		s.recordOperation(ctx, "read", stream, start, err)
		if err == nil {
			s.recordEntriesRead(ctx, stream, int64(len(entries)))
		}
		return entries, err
	}

	attrs := append(s.commonAttributes(),
		attribute.String("ledger.stream", stream),
	)

	ctx, span := s.tracer.Start(ctx, "ledger.Read", trace.WithAttributes(attrs...))
	defer span.End()

	start := time.Now()
	entries, err := s.store.Read(ctx, stream, opts...)
	s.recordOperation(ctx, "read", stream, start, err)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
		span.SetAttributes(attribute.Int("ledger.result_count", len(entries)))
		s.recordEntriesRead(ctx, stream, int64(len(entries)))
	}

	return entries, err
}

// Count returns the number of entries in the named stream.
func (s *InstrumentedStore[I, P]) Count(ctx context.Context, stream string) (int64, error) {
	if !s.opts.enableTraces {
		start := time.Now()
		n, err := s.store.Count(ctx, stream)
		s.recordOperation(ctx, "count", stream, start, err)
		return n, err
	}

	attrs := append(s.commonAttributes(),
		attribute.String("ledger.stream", stream),
	)

	ctx, span := s.tracer.Start(ctx, "ledger.Count", trace.WithAttributes(attrs...))
	defer span.End()

	start := time.Now()
	n, err := s.store.Count(ctx, stream)
	s.recordOperation(ctx, "count", stream, start, err)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
		span.SetAttributes(attribute.Int64("ledger.count", n))
	}

	return n, err
}

// Stat returns metrics for the named stream.
func (s *InstrumentedStore[I, P]) Stat(ctx context.Context, stream string) (ledger.StreamStat[I], error) {
	if !s.opts.enableTraces {
		start := time.Now()
		stat, err := s.store.Stat(ctx, stream)
		s.recordOperation(ctx, "stat", stream, start, err)
		return stat, err
	}

	attrs := append(s.commonAttributes(),
		attribute.String("ledger.stream", stream),
	)

	ctx, span := s.tracer.Start(ctx, "ledger.Stat", trace.WithAttributes(attrs...))
	defer span.End()

	start := time.Now()
	stat, err := s.store.Stat(ctx, stream)
	s.recordOperation(ctx, "stat", stream, start, err)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
		span.SetAttributes(
			attribute.Int64("ledger.count", stat.Count),
			attribute.String("ledger.first_id", fmt.Sprintf("%v", stat.FirstID)),
			attribute.String("ledger.last_id", fmt.Sprintf("%v", stat.LastID)),
		)
	}

	return stat, err
}

// Search performs a full-text search, delegating to the wrapped store if it
// implements [ledger.Searcher]. Returns [ledger.ErrNotSupported] otherwise.
func (s *InstrumentedStore[I, P]) Search(ctx context.Context, stream string, query string, opts ...ledger.ReadOption) ([]ledger.StoredEntry[I, P], error) {
	searcher, ok := s.store.(ledger.Searcher[I, P])
	if !ok {
		return nil, ledger.ErrNotSupported
	}
	if !s.opts.enableTraces {
		start := time.Now()
		entries, err := searcher.Search(ctx, stream, query, opts...)
		s.recordOperation(ctx, "search", stream, start, err)
		return entries, err
	}
	attrs := append(s.commonAttributes(),
		attribute.String("ledger.stream", stream),
		attribute.String("ledger.query", query),
	)
	ctx, span := s.tracer.Start(ctx, "ledger.Search", trace.WithAttributes(attrs...))
	defer span.End()

	start := time.Now()
	entries, err := searcher.Search(ctx, stream, query, opts...)
	s.recordOperation(ctx, "search", stream, start, err)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
		span.SetAttributes(attribute.Int("ledger.result_count", len(entries)))
	}
	return entries, err
}

// SetTags replaces all tags on an entry.
func (s *InstrumentedStore[I, P]) SetTags(ctx context.Context, stream string, id I, tags []string) error {
	if !s.opts.enableTraces {
		start := time.Now()
		err := s.store.SetTags(ctx, stream, id, tags)
		s.recordOperation(ctx, "set_tags", stream, start, err)
		return err
	}

	attrs := append(s.commonAttributes(),
		attribute.String("ledger.stream", stream),
		attribute.String("ledger.entry_id", fmt.Sprintf("%v", id)),
	)

	ctx, span := s.tracer.Start(ctx, "ledger.SetTags", trace.WithAttributes(attrs...))
	defer span.End()

	start := time.Now()
	err := s.store.SetTags(ctx, stream, id, tags)
	s.recordOperation(ctx, "set_tags", stream, start, err)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return err
}

// SetAnnotations merges annotations into an entry.
func (s *InstrumentedStore[I, P]) SetAnnotations(ctx context.Context, stream string, id I, annotations map[string]*string) error {
	if !s.opts.enableTraces {
		start := time.Now()
		err := s.store.SetAnnotations(ctx, stream, id, annotations)
		s.recordOperation(ctx, "set_annotations", stream, start, err)
		return err
	}

	attrs := append(s.commonAttributes(),
		attribute.String("ledger.stream", stream),
		attribute.String("ledger.entry_id", fmt.Sprintf("%v", id)),
	)

	ctx, span := s.tracer.Start(ctx, "ledger.SetAnnotations", trace.WithAttributes(attrs...))
	defer span.End()

	start := time.Now()
	err := s.store.SetAnnotations(ctx, stream, id, annotations)
	s.recordOperation(ctx, "set_annotations", stream, start, err)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return err
}

// Trim deletes entries with IDs less than or equal to beforeID from the stream.
func (s *InstrumentedStore[I, P]) Trim(ctx context.Context, stream string, beforeID I) (int64, error) {
	if !s.opts.enableTraces {
		start := time.Now()
		n, err := s.store.Trim(ctx, stream, beforeID)
		s.recordOperation(ctx, "trim", stream, start, err)
		if err == nil {
			s.recordEntriesTrimmed(ctx, stream, n)
		}
		return n, err
	}

	attrs := append(s.commonAttributes(),
		attribute.String("ledger.stream", stream),
		attribute.String("ledger.before_id", fmt.Sprintf("%v", beforeID)),
	)

	ctx, span := s.tracer.Start(ctx, "ledger.Trim", trace.WithAttributes(attrs...))
	defer span.End()

	start := time.Now()
	n, err := s.store.Trim(ctx, stream, beforeID)
	s.recordOperation(ctx, "trim", stream, start, err)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
		span.SetAttributes(attribute.Int64("ledger.trimmed_count", n))
		s.recordEntriesTrimmed(ctx, stream, n)
	}

	return n, err
}

// ListStreamIDs returns distinct stream IDs in this store.
func (s *InstrumentedStore[I, P]) ListStreamIDs(ctx context.Context, opts ...ledger.ListOption) ([]string, error) {
	if !s.opts.enableTraces {
		start := time.Now()
		ids, err := s.store.ListStreamIDs(ctx, opts...)
		s.recordOperation(ctx, "list_stream_ids", "", start, err)
		return ids, err
	}

	ctx, span := s.tracer.Start(ctx, "ledger.ListStreamIDs",
		trace.WithAttributes(s.commonAttributes()...))
	defer span.End()

	start := time.Now()
	ids, err := s.store.ListStreamIDs(ctx, opts...)
	s.recordOperation(ctx, "list_stream_ids", "", start, err)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
		span.SetAttributes(attribute.Int("ledger.stream_count", len(ids)))
	}

	return ids, err
}

// Close releases resources held by the store.
func (s *InstrumentedStore[I, P]) Close(ctx context.Context) error {
	if !s.opts.enableTraces {
		return s.store.Close(ctx)
	}

	ctx, span := s.tracer.Start(ctx, "ledger.Close",
		trace.WithAttributes(s.commonAttributes()...))
	defer span.End()

	err := s.store.Close(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return err
}

// Health performs a health check on the underlying store if it implements
// [ledger.HealthChecker]. Returns nil if the underlying store does not support it.
func (s *InstrumentedStore[I, P]) Health(ctx context.Context) error {
	checker, ok := s.store.(ledger.HealthChecker)
	if !ok {
		return nil
	}

	if !s.opts.enableTraces {
		return checker.Health(ctx)
	}

	ctx, span := s.tracer.Start(ctx, "ledger.Health",
		trace.WithAttributes(s.commonAttributes()...))
	defer span.End()

	err := checker.Health(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return err
}

// commonAttributes returns attributes shared by all spans and metrics.
// The returned slice has len==cap so that callers can safely append without aliasing.
func (s *InstrumentedStore[I, P]) commonAttributes() []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 2)
	attrs = append(attrs, attribute.String("ledger.backend", s.opts.backendName))
	if s.storeType != "" {
		attrs = append(attrs, attribute.String("ledger.store_type", s.storeType))
	}
	return attrs[:len(attrs):len(attrs)]
}

// recordOperation records metrics for a store operation.
func (s *InstrumentedStore[I, P]) recordOperation(ctx context.Context, op, stream string, start time.Time, err error) {
	if !s.opts.enableMetrics {
		return
	}

	latency := time.Since(start).Seconds()
	attrs := make([]attribute.KeyValue, 0, 4)
	attrs = append(attrs,
		attribute.String("operation", op),
		attribute.String("ledger.backend", s.opts.backendName),
	)
	if stream != "" {
		attrs = append(attrs, attribute.String("ledger.stream", stream))
	}
	if s.storeType != "" {
		attrs = append(attrs, attribute.String("ledger.store_type", s.storeType))
	}

	s.metrics.operationCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	s.metrics.operationLatency.Record(ctx, latency, metric.WithAttributes(attrs...))

	if err != nil {
		errorAttrs := append(attrs, attribute.String("error_type", errorType(err)))
		s.metrics.errorCount.Add(ctx, 1, metric.WithAttributes(errorAttrs...))
	}
}

func (s *InstrumentedStore[I, P]) recordEntriesAppended(ctx context.Context, stream string, count int64) {
	if !s.opts.enableMetrics || count == 0 {
		return
	}
	attrs := s.streamAttributes(stream)
	s.metrics.entriesAppended.Add(ctx, count, metric.WithAttributes(attrs...))
}

func (s *InstrumentedStore[I, P]) recordEntriesRead(ctx context.Context, stream string, count int64) {
	if !s.opts.enableMetrics || count == 0 {
		return
	}
	attrs := s.streamAttributes(stream)
	s.metrics.entriesRead.Add(ctx, count, metric.WithAttributes(attrs...))
}

func (s *InstrumentedStore[I, P]) recordEntriesTrimmed(ctx context.Context, stream string, count int64) {
	if !s.opts.enableMetrics || count == 0 {
		return
	}
	attrs := s.streamAttributes(stream)
	s.metrics.entriesTrimmed.Add(ctx, count, metric.WithAttributes(attrs...))
}

func (s *InstrumentedStore[I, P]) streamAttributes(stream string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 3)
	attrs = append(attrs,
		attribute.String("ledger.backend", s.opts.backendName),
		attribute.String("ledger.stream", stream),
	)
	if s.storeType != "" {
		attrs = append(attrs, attribute.String("ledger.store_type", s.storeType))
	}
	return attrs
}

// errorType classifies an error for the "error_type" metric attribute.
func errorType(err error) string {
	switch {
	case errors.Is(err, ledger.ErrEntryNotFound):
		return "not_found"
	case errors.Is(err, ledger.ErrStoreClosed):
		return "store_closed"
	case errors.Is(err, ledger.ErrInvalidCursor):
		return "invalid_cursor"
	case errors.Is(err, ledger.ErrEncode):
		return "encode"
	case errors.Is(err, ledger.ErrDecode):
		return "decode"
	default:
		return "internal"
	}
}
