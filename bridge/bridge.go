package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/ledger"
	internalReplication "github.com/rbaliyan/ledger/internal/replication"
)

// Stats holds counters accumulated since the Bridge started.
type Stats struct {
	PollCount  int64
	ApplyCount int64
	SkipCount  int64
	ErrorCount int64
}

// Bridge polls a source mutation log and applies changes to a sink store.
// SI is the source store ID type; DI is the sink store ID type.
//
// Eventual consistency: tag and annotation updates lag behind the source by up
// to one polling interval (default 5s).
//
// Multiple Bridge instances may run against the same source and sink safely —
// all mutations are idempotent at the sink (unique source_id index). Cursor
// writes are monotonic: an instance that lags behind will never regress a
// faster instance's cursor position.
//
// Wrap sink streams in [ReadOnlyStream] to prevent accidental direct writes.
type Bridge[SI comparable, DI comparable] struct {
	mutations         ledger.Store[SI, json.RawMessage]
	sink              ledger.Store[DI, json.RawMessage]
	sinkCursor        ledger.CursorStore
	sinkLookup        ledger.SourceIDLookup[DI]
	codec             IDCodec[SI]
	name              string
	interval          time.Duration
	batchSize         int
	logger            *slog.Logger
	skipMutationTypes map[MutationType]struct{}

	startOnce  sync.Once
	stopOnce   sync.Once
	started    atomic.Bool
	stop       chan struct{}
	stopped    chan struct{}
	pollCount  atomic.Int64
	applyCount atomic.Int64
	skipCount  atomic.Int64
	errorCount atomic.Int64
}

// Option configures a Bridge.
type Option func(*options)

type options struct {
	name              string
	interval          time.Duration
	batchSize         int
	logger            *slog.Logger
	skipMutationTypes map[MutationType]struct{}
}

func defaultOptions() options {
	return options{
		name:      "default",
		interval:  5 * time.Second,
		batchSize: 500,
		logger:    slog.Default(),
	}
}

// WithName sets the cursor name used to track progress.
// Use a unique name per source-sink pair. Defaults to "default".
func WithName(name string) Option {
	return func(o *options) { o.name = name }
}

// WithInterval sets the polling interval. Defaults to 5s.
func WithInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.interval = d
		}
	}
}

// WithBatchSize sets the number of mutations processed per poll. Defaults to 500.
func WithBatchSize(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.batchSize = n
		}
	}
}

// WithLogger sets the logger. Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithSkipMutationTypes instructs the Bridge to silently drop mutation events of the
// given types instead of applying them to the sink. Use this when the sink backend does
// not support the operation — e.g. pass MutationSetTags and MutationSetAnnotations when
// bridging to ClickHouse.
func WithSkipMutationTypes(types ...MutationType) Option {
	return func(o *options) {
		if o.skipMutationTypes == nil {
			o.skipMutationTypes = make(map[MutationType]struct{}, len(types))
		}
		for _, t := range types {
			o.skipMutationTypes[t] = struct{}{}
		}
	}
}

// New creates a Bridge. mutations is the source mutation log store (same DB as source).
// sink is the destination store. codec encodes the mutation log's ID type for cursor storage.
//
// If sink implements [ledger.CursorStore], progress is persisted in the sink DB so the
// Bridge resumes from where it left off after a restart.
// If sink implements [ledger.SourceIDLookup], SetTags, SetAnnotations, and Trim mutations
// are applied; otherwise they are skipped with a warning.
func New[SI comparable, DI comparable](
	mutations ledger.Store[SI, json.RawMessage],
	sink ledger.Store[DI, json.RawMessage],
	codec IDCodec[SI],
	opts ...Option,
) *Bridge[SI, DI] {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	b := &Bridge[SI, DI]{
		mutations:         mutations,
		sink:              sink,
		codec:             codec,
		name:              o.name,
		interval:          o.interval,
		batchSize:         o.batchSize,
		logger:            o.logger,
		skipMutationTypes: o.skipMutationTypes,
		stop:              make(chan struct{}),
		stopped:           make(chan struct{}),
	}
	b.sinkCursor, _ = sink.(ledger.CursorStore)
	b.sinkLookup, _ = sink.(ledger.SourceIDLookup[DI])
	if b.sinkCursor == nil {
		o.logger.Warn("bridge sink does not implement CursorStore; progress will not be persisted across restarts", "name", o.name)
	}
	return b
}

// Start begins bridging in a background goroutine. Safe to call once; subsequent
// calls are no-ops. The provided context controls goroutine lifetime; cancelling it
// is equivalent to calling Stop.
func (b *Bridge[SI, DI]) Start(ctx context.Context) {
	b.startOnce.Do(func() {
		b.started.Store(true)
		go b.run(ctx)
	})
}

// Stop signals the Bridge to stop and waits for it to exit.
// Safe to call multiple times. If Start was never called, Stop returns immediately.
func (b *Bridge[SI, DI]) Stop() {
	b.stopOnce.Do(func() { close(b.stop) })
	if b.started.Load() {
		<-b.stopped
	}
}

// Poll runs a single bridging cycle and returns any error. Safe to call
// concurrently with Start/Stop — it does not affect the background goroutine.
func (b *Bridge[SI, DI]) Poll(ctx context.Context) error {
	return b.poll(ctx)
}

// Stats returns a snapshot of the bridging counters.
func (b *Bridge[SI, DI]) Stats() Stats {
	return Stats{
		PollCount:  b.pollCount.Load(),
		ApplyCount: b.applyCount.Load(),
		SkipCount:  b.skipCount.Load(),
		ErrorCount: b.errorCount.Load(),
	}
}

func (b *Bridge[SI, DI]) run(ctx context.Context) {
	defer close(b.stopped)
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := b.poll(ctx); err != nil {
				b.errorCount.Add(1)
				b.logger.Warn("bridge poll failed", "error", err, "name", b.name)
			}
		}
	}
}

func (b *Bridge[SI, DI]) poll(ctx context.Context) error {
	b.pollCount.Add(1)

	cursor := b.codec.Zero()
	if b.sinkCursor != nil {
		s, ok, err := b.sinkCursor.GetCursor(ctx, b.name)
		if err != nil {
			return fmt.Errorf("get cursor: %w", err)
		}
		if ok {
			cursor, err = b.codec.Decode(s)
			if err != nil {
				return fmt.Errorf("decode cursor: %w", err)
			}
		}
	}

	readOpts := []ledger.ReadOption{ledger.Limit(b.batchSize)}
	if cursor != b.codec.Zero() {
		readOpts = append(readOpts, ledger.After(cursor))
	}
	entries, err := b.mutations.Read(ctx, internalReplication.MutationStream, readOpts...)
	if err != nil {
		return fmt.Errorf("read mutations: %w", err)
	}

	var lastID SI
	for _, e := range entries {
		var evt MutationEvent
		if err := json.Unmarshal(e.Payload, &evt); err != nil {
			return fmt.Errorf("decode mutation event: %w", err)
		}
		if err := b.apply(ctx, evt); err != nil {
			return fmt.Errorf("apply %s mutation on stream %q: %w", evt.Type, evt.Stream, err)
		}
		lastID = e.ID
	}

	if len(entries) > 0 && b.sinkCursor != nil {
		if err := b.advanceCursor(ctx, lastID); err != nil {
			return fmt.Errorf("advance cursor: %w", err)
		}
	}
	return nil
}

// advanceCursor writes the cursor only if lastID is strictly greater than the
// current stored cursor. Re-reading the cursor before writing prevents a lagging
// Bridge instance from regressing the position advanced by a faster instance.
func (b *Bridge[SI, DI]) advanceCursor(ctx context.Context, lastID SI) error {
	// Re-read current cursor to guard against concurrent advancement.
	currentStr, ok, err := b.sinkCursor.GetCursor(ctx, b.name)
	if err != nil {
		return err
	}
	current := b.codec.Zero()
	if ok {
		current, err = b.codec.Decode(currentStr)
		if err != nil {
			return fmt.Errorf("decode current cursor: %w", err)
		}
	}
	// Only advance — never regress.
	if !b.codec.Less(current, lastID) {
		return nil
	}
	return b.sinkCursor.SetCursor(ctx, b.name, b.codec.Encode(lastID))
}

func (b *Bridge[SI, DI]) apply(ctx context.Context, evt MutationEvent) error {
	if _, skip := b.skipMutationTypes[evt.Type]; skip {
		b.skipCount.Add(1)
		b.logger.Debug("skipping mutation type", "type", evt.Type, "stream", evt.Stream)
		return nil
	}
	b.applyCount.Add(1)
	switch evt.Type {
	case MutationAppend:
		return b.applyAppend(ctx, evt)
	case MutationSetTags:
		return b.applySetTags(ctx, evt)
	case MutationSetAnnotations:
		return b.applySetAnnotations(ctx, evt)
	case MutationTrim:
		return b.applyTrim(ctx, evt)
	default:
		b.logger.Warn("unknown mutation type, skipping", "type", evt.Type)
		return ledger.ErrNotSupported
	}
}

func (b *Bridge[SI, DI]) applyAppend(ctx context.Context, evt MutationEvent) error {
	if len(evt.Entries) == 0 {
		return nil
	}
	raw := make([]ledger.RawEntry[json.RawMessage], len(evt.Entries))
	for i, e := range evt.Entries {
		raw[i] = ledger.RawEntry[json.RawMessage]{
			Payload:       e.Payload,
			OrderKey:      e.OrderKey,
			DedupKey:      e.DedupKey,
			SchemaVersion: e.SchemaVersion,
			Metadata:      e.Metadata,
			Tags:          e.Tags,
			SourceID:      e.ID,
		}
	}
	_, err := b.sink.Append(ctx, evt.Stream, raw...)
	return err
}

func (b *Bridge[SI, DI]) applySetTags(ctx context.Context, evt MutationEvent) error {
	if b.sinkLookup == nil {
		b.logger.Warn("sink does not support SourceIDLookup, skipping set_tags", "stream", evt.Stream, "source_id", evt.EntryID)
		return nil
	}
	sinkID, ok, err := b.sinkLookup.FindBySourceID(ctx, evt.Stream, evt.EntryID)
	if err != nil {
		return err
	}
	if !ok {
		b.logger.Warn("source entry not found in sink for set_tags (may have been trimmed)", "stream", evt.Stream, "source_id", evt.EntryID)
		return nil
	}
	return b.sink.SetTags(ctx, evt.Stream, sinkID, evt.Tags)
}

func (b *Bridge[SI, DI]) applySetAnnotations(ctx context.Context, evt MutationEvent) error {
	if b.sinkLookup == nil {
		b.logger.Warn("sink does not support SourceIDLookup, skipping set_annotations", "stream", evt.Stream, "source_id", evt.EntryID)
		return nil
	}
	sinkID, ok, err := b.sinkLookup.FindBySourceID(ctx, evt.Stream, evt.EntryID)
	if err != nil {
		return err
	}
	if !ok {
		b.logger.Warn("source entry not found in sink for set_annotations (may have been trimmed)", "stream", evt.Stream, "source_id", evt.EntryID)
		return nil
	}
	return b.sink.SetAnnotations(ctx, evt.Stream, sinkID, evt.Annotations)
}

func (b *Bridge[SI, DI]) applyTrim(ctx context.Context, evt MutationEvent) error {
	if b.sinkLookup == nil {
		b.logger.Warn("sink does not support SourceIDLookup, skipping trim", "stream", evt.Stream, "source_id", evt.BeforeID)
		return nil
	}
	sinkID, ok, err := b.sinkLookup.FindBySourceID(ctx, evt.Stream, evt.BeforeID)
	if err != nil {
		return err
	}
	if !ok {
		b.logger.Warn("trim cursor not found in sink (may have been trimmed already)", "stream", evt.Stream, "source_id", evt.BeforeID)
		return nil
	}
	_, err = b.sink.Trim(ctx, evt.Stream, sinkID)
	return err
}
