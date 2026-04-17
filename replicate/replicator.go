package replicate

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/rbaliyan/ledger"
	internalReplication "github.com/rbaliyan/ledger/internal/replication"
)

// Replicator polls a source mutation log and applies changes to a sink store.
// SI is the source store ID type; DI is the sink store ID type.
//
// Replication is eventually consistent: tag and annotation changes lag by up to
// one polling interval (default 5s).
//
// The sink store should be wrapped in a ReadOnlyStream to prevent accidental writes.
type Replicator[SI comparable, DI comparable] struct {
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

	stop    chan struct{}
	stopped chan struct{}
}

// ReplicatorOption configures a Replicator.
type ReplicatorOption func(*replicatorOptions)

type replicatorOptions struct {
	name             string
	interval         time.Duration
	batchSize        int
	logger           *slog.Logger
	skipMutationTypes map[MutationType]struct{}
}

func defaultReplicatorOptions() replicatorOptions {
	return replicatorOptions{
		name:      "default",
		interval:  5 * time.Second,
		batchSize: 500,
		logger:    slog.Default(),
	}
}

// WithName sets the cursor name used to track replication progress.
// Use a unique name per replication pair. Defaults to "default".
func WithName(name string) ReplicatorOption {
	return func(o *replicatorOptions) { o.name = name }
}

// WithInterval sets the polling interval. Defaults to 5s.
func WithInterval(d time.Duration) ReplicatorOption {
	return func(o *replicatorOptions) {
		if d > 0 {
			o.interval = d
		}
	}
}

// WithBatchSize sets the number of mutations processed per poll. Defaults to 500.
func WithBatchSize(n int) ReplicatorOption {
	return func(o *replicatorOptions) {
		if n > 0 {
			o.batchSize = n
		}
	}
}

// WithReplicatorLogger sets the logger. Defaults to slog.Default().
func WithReplicatorLogger(l *slog.Logger) ReplicatorOption {
	return func(o *replicatorOptions) { o.logger = l }
}

// WithSkipMutationTypes instructs the replicator to silently drop mutation events of the
// given types instead of applying them to the sink. Use this when the sink backend does
// not support the operation — e.g. pass MutationSetTags and MutationSetAnnotations when
// replicating to ClickHouse.
func WithSkipMutationTypes(types ...MutationType) ReplicatorOption {
	return func(o *replicatorOptions) {
		if o.skipMutationTypes == nil {
			o.skipMutationTypes = make(map[MutationType]struct{}, len(types))
		}
		for _, t := range types {
			o.skipMutationTypes[t] = struct{}{}
		}
	}
}

// New creates a Replicator. mutations is the source mutation log store (same DB as source).
// sink is the destination store. codec serialises the mutation log's ID type for cursor storage.
//
// If sink implements [ledger.CursorStore], replication progress is persisted in the sink DB.
// If sink implements [ledger.SourceIDLookup], SetTags, SetAnnotations, and Trim mutations
// are applied; otherwise they are skipped with a warning.
func New[SI comparable, DI comparable](
	mutations ledger.Store[SI, json.RawMessage],
	sink ledger.Store[DI, json.RawMessage],
	codec IDCodec[SI],
	opts ...ReplicatorOption,
) *Replicator[SI, DI] {
	o := defaultReplicatorOptions()
	for _, opt := range opts {
		opt(&o)
	}
	r := &Replicator[SI, DI]{
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
	r.sinkCursor, _ = sink.(ledger.CursorStore)
	r.sinkLookup, _ = sink.(ledger.SourceIDLookup[DI])
	return r
}

// Start begins replication in a background goroutine. Call Stop to shut it down.
// The provided context controls background goroutine lifetime; cancelling it is
// equivalent to calling Stop.
func (r *Replicator[SI, DI]) Start(ctx context.Context) {
	go r.run(ctx)
}

// Stop signals the replicator to stop and waits for it to exit.
func (r *Replicator[SI, DI]) Stop() {
	close(r.stop)
	<-r.stopped
}

func (r *Replicator[SI, DI]) run(ctx context.Context) {
	defer close(r.stopped)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.poll(ctx); err != nil {
				r.logger.Warn("replication poll failed", "error", err, "cursor_name", r.name)
			}
		}
	}
}

func (r *Replicator[SI, DI]) poll(ctx context.Context) error {
	cursor := r.codec.Zero()
	if r.sinkCursor != nil {
		s, ok, err := r.sinkCursor.GetCursor(ctx, r.name)
		if err != nil {
			return fmt.Errorf("get cursor: %w", err)
		}
		if ok {
			cursor, err = r.codec.Decode(s)
			if err != nil {
				return fmt.Errorf("decode cursor: %w", err)
			}
		}
	}

	entries, err := r.mutations.Read(ctx,
		internalReplication.MutationStream,
		ledger.After(cursor),
		ledger.Limit(r.batchSize),
	)
	if err != nil {
		return fmt.Errorf("read mutations: %w", err)
	}

	var lastID SI
	for _, e := range entries {
		var evt MutationEvent
		if err := json.Unmarshal(e.Payload, &evt); err != nil {
			return fmt.Errorf("decode mutation event: %w", err)
		}
		if err := r.apply(ctx, evt); err != nil {
			return fmt.Errorf("apply %s mutation on stream %q: %w", evt.Type, evt.Stream, err)
		}
		lastID = e.ID
	}

	if len(entries) > 0 && r.sinkCursor != nil {
		if err := r.sinkCursor.SetCursor(ctx, r.name, r.codec.Encode(lastID)); err != nil {
			return fmt.Errorf("set cursor: %w", err)
		}
	}
	return nil
}

func (r *Replicator[SI, DI]) apply(ctx context.Context, evt MutationEvent) error {
	if _, skip := r.skipMutationTypes[evt.Type]; skip {
		r.logger.Debug("skipping mutation type", "type", evt.Type, "stream", evt.Stream)
		return nil
	}
	switch evt.Type {
	case MutationAppend:
		return r.applyAppend(ctx, evt)
	case MutationSetTags:
		return r.applySetTags(ctx, evt)
	case MutationSetAnnotations:
		return r.applySetAnnotations(ctx, evt)
	case MutationTrim:
		return r.applyTrim(ctx, evt)
	default:
		r.logger.Warn("unknown mutation type, skipping", "type", evt.Type)
		return nil
	}
}

func (r *Replicator[SI, DI]) applyAppend(ctx context.Context, evt MutationEvent) error {
	if len(evt.Entries) == 0 {
		return nil
	}
	raw := make([]ledger.RawEntry[json.RawMessage], len(evt.Entries))
	sourceIDs := make([]string, len(evt.Entries))
	for i, e := range evt.Entries {
		raw[i] = ledger.RawEntry[json.RawMessage]{
			Payload:       e.Payload,
			OrderKey:      e.OrderKey,
			DedupKey:      e.DedupKey,
			SchemaVersion: e.SchemaVersion,
			Metadata:      e.Metadata,
			Tags:          e.Tags,
		}
		sourceIDs[i] = e.ID
	}
	ctx = internalReplication.WithSourceIDs(ctx, sourceIDs)
	_, err := r.sink.Append(ctx, evt.Stream, raw...)
	return err
}

func (r *Replicator[SI, DI]) applySetTags(ctx context.Context, evt MutationEvent) error {
	if r.sinkLookup == nil {
		r.logger.Warn("sink does not support SourceIDLookup, skipping set_tags", "stream", evt.Stream, "source_id", evt.EntryID)
		return nil
	}
	sinkID, ok, err := r.sinkLookup.FindBySourceID(ctx, evt.Stream, evt.EntryID)
	if err != nil {
		return err
	}
	if !ok {
		r.logger.Warn("source entry not found in sink for set_tags (may have been trimmed)", "stream", evt.Stream, "source_id", evt.EntryID)
		return nil
	}
	return r.sink.SetTags(ctx, evt.Stream, sinkID, evt.Tags)
}

func (r *Replicator[SI, DI]) applySetAnnotations(ctx context.Context, evt MutationEvent) error {
	if r.sinkLookup == nil {
		r.logger.Warn("sink does not support SourceIDLookup, skipping set_annotations", "stream", evt.Stream, "source_id", evt.EntryID)
		return nil
	}
	sinkID, ok, err := r.sinkLookup.FindBySourceID(ctx, evt.Stream, evt.EntryID)
	if err != nil {
		return err
	}
	if !ok {
		r.logger.Warn("source entry not found in sink for set_annotations (may have been trimmed)", "stream", evt.Stream, "source_id", evt.EntryID)
		return nil
	}
	return r.sink.SetAnnotations(ctx, evt.Stream, sinkID, evt.Annotations)
}

func (r *Replicator[SI, DI]) applyTrim(ctx context.Context, evt MutationEvent) error {
	if r.sinkLookup == nil {
		r.logger.Warn("sink does not support SourceIDLookup, skipping trim", "stream", evt.Stream, "source_id", evt.BeforeID)
		return nil
	}
	sinkID, ok, err := r.sinkLookup.FindBySourceID(ctx, evt.Stream, evt.BeforeID)
	if err != nil {
		return err
	}
	if !ok {
		r.logger.Warn("trim cursor not found in sink (may have been trimmed already)", "stream", evt.Stream, "source_id", evt.BeforeID)
		return nil
	}
	_, err = r.sink.Trim(ctx, evt.Stream, sinkID)
	return err
}
