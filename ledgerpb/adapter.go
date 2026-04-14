package ledgerpb

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/rbaliyan/ledger"
)

// int64Backend adapts Store[int64, json.RawMessage] to Backend.
// Integer IDs are serialised as decimal strings.
type int64Backend struct {
	store  ledger.Store[int64, json.RawMessage]
	health func(context.Context) error
}

// NewInt64Backend wraps a Store[int64, json.RawMessage] (SQLite, PostgreSQL) as a
// Backend for the gRPC Server. Integer IDs are exposed as decimal strings on the wire.
//
// If the store also implements ledger.HealthChecker the Health endpoint is enabled.
func NewInt64Backend(s ledger.Store[int64, json.RawMessage]) Backend {
	b := &int64Backend{store: s}
	if hc, ok := s.(ledger.HealthChecker); ok {
		b.health = hc.Health
	}
	return b
}

func (b *int64Backend) Append(ctx context.Context, stream string, entries ...InputEntry) ([]string, error) {
	raw := make([]ledger.RawEntry[json.RawMessage], len(entries))
	for i, e := range entries {
		raw[i] = ledger.RawEntry[json.RawMessage]{
			Payload:       e.Payload,
			OrderKey:      e.OrderKey,
			DedupKey:      e.DedupKey,
			SchemaVersion: e.SchemaVersion,
			Metadata:      e.Metadata,
			Tags:          e.Tags,
		}
	}
	ids, err := b.store.Append(ctx, stream, raw...)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = strconv.FormatInt(id, 10)
	}
	return out, nil
}

func (b *int64Backend) Read(ctx context.Context, stream string, opts ReadOptions) ([]StoredEntry, error) {
	ropts, err := opts.toInt64Opts()
	if err != nil {
		return nil, err
	}
	entries, err := b.store.Read(ctx, stream, ropts...)
	if err != nil {
		return nil, err
	}
	out := make([]StoredEntry, len(entries))
	for i, e := range entries {
		out[i] = storedFromInt64(e)
	}
	return out, nil
}

func (b *int64Backend) Count(ctx context.Context, stream string) (int64, error) {
	return b.store.Count(ctx, stream)
}

func (b *int64Backend) SetTags(ctx context.Context, stream string, id string, tags []string) error {
	n, err := parseIntID(id)
	if err != nil {
		return err
	}
	return b.store.SetTags(ctx, stream, n, tags)
}

func (b *int64Backend) SetAnnotations(ctx context.Context, stream string, id string, annotations map[string]*string) error {
	n, err := parseIntID(id)
	if err != nil {
		return err
	}
	return b.store.SetAnnotations(ctx, stream, n, annotations)
}

func (b *int64Backend) Trim(ctx context.Context, stream string, beforeID string) (int64, error) {
	n, err := parseIntID(beforeID)
	if err != nil {
		return 0, err
	}
	return b.store.Trim(ctx, stream, n)
}

func (b *int64Backend) ListStreamIDs(ctx context.Context, after string, limit int) ([]string, error) {
	var opts []ledger.ListOption
	if after != "" {
		opts = append(opts, ledger.ListAfter(after))
	}
	if limit > 0 {
		opts = append(opts, ledger.ListLimit(limit))
	}
	return b.store.ListStreamIDs(ctx, opts...)
}

func (b *int64Backend) Health(ctx context.Context) error {
	if b.health != nil {
		return b.health(ctx)
	}
	return nil
}

// stringBackend adapts Store[string, json.RawMessage] to Backend.
type stringBackend struct {
	store  ledger.Store[string, json.RawMessage]
	health func(context.Context) error
}

// NewStringBackend wraps a Store[string, json.RawMessage] (MongoDB) as a Backend
// for the gRPC Server.
//
// If the store also implements ledger.HealthChecker the Health endpoint is enabled.
func NewStringBackend(s ledger.Store[string, json.RawMessage]) Backend {
	b := &stringBackend{store: s}
	if hc, ok := s.(ledger.HealthChecker); ok {
		b.health = hc.Health
	}
	return b
}

func (b *stringBackend) Append(ctx context.Context, stream string, entries ...InputEntry) ([]string, error) {
	raw := make([]ledger.RawEntry[json.RawMessage], len(entries))
	for i, e := range entries {
		raw[i] = ledger.RawEntry[json.RawMessage]{
			Payload:       e.Payload,
			OrderKey:      e.OrderKey,
			DedupKey:      e.DedupKey,
			SchemaVersion: e.SchemaVersion,
			Metadata:      e.Metadata,
			Tags:          e.Tags,
		}
	}
	return b.store.Append(ctx, stream, raw...)
}

func (b *stringBackend) Read(ctx context.Context, stream string, opts ReadOptions) ([]StoredEntry, error) {
	entries, err := b.store.Read(ctx, stream, opts.toStringOpts()...)
	if err != nil {
		return nil, err
	}
	out := make([]StoredEntry, len(entries))
	for i, e := range entries {
		out[i] = storedFromString(e)
	}
	return out, nil
}

func (b *stringBackend) Count(ctx context.Context, stream string) (int64, error) {
	return b.store.Count(ctx, stream)
}

func (b *stringBackend) SetTags(ctx context.Context, stream string, id string, tags []string) error {
	return b.store.SetTags(ctx, stream, id, tags)
}

func (b *stringBackend) SetAnnotations(ctx context.Context, stream string, id string, annotations map[string]*string) error {
	return b.store.SetAnnotations(ctx, stream, id, annotations)
}

func (b *stringBackend) Trim(ctx context.Context, stream string, beforeID string) (int64, error) {
	return b.store.Trim(ctx, stream, beforeID)
}

func (b *stringBackend) ListStreamIDs(ctx context.Context, after string, limit int) ([]string, error) {
	var opts []ledger.ListOption
	if after != "" {
		opts = append(opts, ledger.ListAfter(after))
	}
	if limit > 0 {
		opts = append(opts, ledger.ListLimit(limit))
	}
	return b.store.ListStreamIDs(ctx, opts...)
}

func (b *stringBackend) Health(ctx context.Context) error {
	if b.health != nil {
		return b.health(ctx)
	}
	return nil
}

// parseIntID converts a decimal string ID to int64.
// Returns ledger.ErrInvalidCursor on parse failure.
func parseIntID(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: expected decimal int64 ID, got %q", ledger.ErrInvalidCursor, s)
	}
	return n, nil
}

// toInt64Opts converts ReadOptions to []ledger.ReadOption for an int64 store.
// Returns an error if After is non-empty and cannot be parsed as int64.
func (o ReadOptions) toInt64Opts() ([]ledger.ReadOption, error) {
	var opts []ledger.ReadOption
	if o.After != "" {
		id, err := parseIntID(o.After)
		if err != nil {
			return nil, err
		}
		opts = append(opts, ledger.After[int64](id))
	}
	return append(opts, o.commonOpts()...), nil
}

// toStringOpts converts ReadOptions to []ledger.ReadOption for a string store.
func (o ReadOptions) toStringOpts() []ledger.ReadOption {
	var opts []ledger.ReadOption
	if o.After != "" {
		opts = append(opts, ledger.After[string](o.After))
	}
	return append(opts, o.commonOpts()...)
}

// commonOpts builds the ReadOptions that are backend-agnostic.
func (o ReadOptions) commonOpts() []ledger.ReadOption {
	var opts []ledger.ReadOption
	if o.Limit > 0 {
		opts = append(opts, ledger.Limit(o.Limit))
	}
	if o.Desc {
		opts = append(opts, ledger.Desc())
	}
	if o.OrderKey != "" {
		opts = append(opts, ledger.WithOrderKey(o.OrderKey))
	}
	if o.Tag != "" {
		opts = append(opts, ledger.WithTag(o.Tag))
	}
	if len(o.AllTags) > 0 {
		opts = append(opts, ledger.WithAllTags(o.AllTags...))
	}
	return opts
}

// storedFromInt64 converts a ledger.StoredEntry[int64, json.RawMessage] to StoredEntry.
func storedFromInt64(e ledger.StoredEntry[int64, json.RawMessage]) StoredEntry {
	return StoredEntry{
		ID:            strconv.FormatInt(e.ID, 10),
		Stream:        e.Stream,
		Payload:       e.Payload,
		OrderKey:      e.OrderKey,
		DedupKey:      e.DedupKey,
		SchemaVersion: e.SchemaVersion,
		Metadata:      e.Metadata,
		Tags:          e.Tags,
		Annotations:   e.Annotations,
		CreatedAt:     e.CreatedAt,
		UpdatedAt:     e.UpdatedAt,
	}
}

// storedFromString converts a ledger.StoredEntry[string, json.RawMessage] to StoredEntry.
func storedFromString(e ledger.StoredEntry[string, json.RawMessage]) StoredEntry {
	return StoredEntry{
		ID:            e.ID,
		Stream:        e.Stream,
		Payload:       e.Payload,
		OrderKey:      e.OrderKey,
		DedupKey:      e.DedupKey,
		SchemaVersion: e.SchemaVersion,
		Metadata:      e.Metadata,
		Tags:          e.Tags,
		Annotations:   e.Annotations,
		CreatedAt:     e.CreatedAt,
		UpdatedAt:     e.UpdatedAt,
	}
}
