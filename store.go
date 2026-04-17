// Package ledger provides an append-only log with typed generic entries,
// schema versioning, deduplication, and pluggable storage backends.
//
// The core abstraction is a two-level generic design:
//   - [Store] is the backend interface, generic over the ID type I and the
//     store-native payload type P (e.g. json.RawMessage for SQL, bson.Raw for MongoDB).
//   - [Stream] is a lightweight typed handle, generic over I, P, and the user's
//     domain payload type T. A [PayloadCodec] bridges T and P.
//
// Streams are cheap to create — one per operation, then discard. Schema versioning
// with [Upcaster] chains enables safe payload evolution without downtime.
//
// Each entry has immutable fields (Payload, OrderKey, DedupKey, SchemaVersion, Metadata,
// CreatedAt) set at append time, and mutable fields (Tags, Annotations) that can be
// updated after the entry is written.
//
// Backends: sqlite (Store[int64, json.RawMessage]), postgres (Store[int64, json.RawMessage]),
// mongodb (Store[string, bson.Raw]).
package ledger

import (
	"context"
	"time"
)

// Store is the backend storage interface for append-only log entries.
// I is the ID type used for cursor-based iteration (e.g., int64 for SQL, string for MongoDB).
// P is the store-native payload type (e.g., json.RawMessage for SQL, bson.Raw for MongoDB).
//
// A Store represents a single entity type — one table or collection. The stream
// parameter on each method identifies an instance within that type (e.g., a
// store created with table "orders" may contain streams "user-123", "user-456").
//
// Append semantics: SQL backends (sqlite, postgres) use transactions for atomic
// batch inserts. The MongoDB backend uses InsertMany with ordered:false, meaning
// partial success is possible on non-dedup errors.
//
// Transaction support: pass a *sql.Tx or *mongo.Session via [WithTx] to have
// operations participate in an external transaction.
type Store[I comparable, P any] interface {
	// Append adds entries to the named stream. Returns the IDs of newly appended entries.
	// Entries with a non-empty DedupKey that already exists in the stream are silently skipped.
	Append(ctx context.Context, stream string, entries ...RawEntry[P]) ([]I, error)

	// Read returns entries from the named stream, ordered by ID.
	// Reading a non-existent stream returns nil, nil.
	Read(ctx context.Context, stream string, opts ...ReadOption) ([]StoredEntry[I, P], error)

	// Count returns the number of entries in the named stream.
	Count(ctx context.Context, stream string) (int64, error)

	// SetTags replaces all tags on an entry. Tags are mutable labels for
	// categorization and filtering (e.g., "processed", "archived").
	SetTags(ctx context.Context, stream string, id I, tags []string) error

	// SetAnnotations merges annotations into an entry. Existing keys are
	// overwritten, new keys are added, and keys with nil values are deleted.
	// Annotations are mutable key-value metadata (e.g., "processed_at", "error").
	SetAnnotations(ctx context.Context, stream string, id I, annotations map[string]*string) error

	// Trim deletes entries from the named stream with IDs less than or equal to beforeID.
	// Returns the number of entries deleted.
	Trim(ctx context.Context, stream string, beforeID I) (int64, error)

	// ListStreamIDs returns distinct stream IDs that have at least one entry in
	// this store. Results are ordered ascending by stream ID and cursor-paginated
	// via [ListAfter] and [ListLimit]. Returns nil, nil for an empty store.
	//
	// A stream that has been fully trimmed is not returned (no separate stream
	// registry is maintained).
	ListStreamIDs(ctx context.Context, opts ...ListOption) ([]string, error)

	// Close releases resources held by the store. The caller is responsible for
	// closing the underlying database connection.
	Close(ctx context.Context) error
}

// HealthChecker is an optional interface that Store implementations may provide
// to report backend health (e.g., database connectivity).
type HealthChecker interface {
	Health(ctx context.Context) error
}

// CursorStore is an optional interface that Store implementations may provide
// to persist replication cursors durably alongside their data.
// The cursor is a string-serialized store ID tracking progress through a mutation log.
type CursorStore interface {
	GetCursor(ctx context.Context, name string) (string, bool, error)
	SetCursor(ctx context.Context, name string, cursor string) error
}

// SourceIDLookup is an optional interface that Store implementations may provide
// to resolve a sink entry ID from its replication source ID.
// Used by the replicator to apply SetTags and SetAnnotations mutations.
type SourceIDLookup[I comparable] interface {
	FindBySourceID(ctx context.Context, stream, sourceID string) (I, bool, error)
}

// RawEntry is an entry with an already-encoded payload, ready for storage.
// P is the store-native payload type.
type RawEntry[P any] struct {
	Payload       P                 // Encoded payload in the store's native format.
	OrderKey      string            // Ordering key for filtering (e.g., aggregate ID).
	DedupKey      string            // Deduplication key. Empty means no dedup.
	SchemaVersion int               // Schema version of the payload.
	Metadata      map[string]string // Arbitrary immutable key-value metadata.
	Tags          []string          // Initial tags (mutable after append via SetTags).
	SourceID      string            // Replication source entry ID. Empty for native writes.
}

// StoredEntry is a raw entry read back from the store, including its assigned ID and timestamp.
// I is the store ID type; P is the store-native payload type.
type StoredEntry[I comparable, P any] struct {
	ID            I                 // Store-assigned unique ID.
	Stream        string            // Stream name this entry belongs to.
	Payload       P                 // Encoded payload in the store's native format.
	OrderKey      string            // Ordering key.
	DedupKey      string            // Deduplication key.
	SchemaVersion int               // Schema version at write time.
	Metadata      map[string]string // Immutable key-value metadata (set at append).
	Tags          []string          // Mutable tags (updated via SetTags).
	Annotations   map[string]string // Mutable annotations (updated via SetAnnotations).
	CreatedAt     time.Time         // Timestamp when the entry was stored.
	UpdatedAt     *time.Time        // Timestamp of last tag/annotation update, nil if never updated.
}

// Order specifies the sort direction when reading entries.
type Order int

const (
	// Ascending reads entries oldest first (default).
	Ascending Order = iota
	// Descending reads entries newest first.
	Descending
)

// String returns the string representation of the order direction.
func (o Order) String() string {
	switch o {
	case Descending:
		return "descending"
	default:
		return "ascending"
	}
}

// ReadOptions holds the parsed read parameters.
// This type is intended for Store implementors.
type ReadOptions struct {
	after    any
	limit    int
	orderKey string
	order    Order
	tag      string
	allTags  []string
}

// Limit returns the maximum number of entries to return.
func (o ReadOptions) Limit() int { return o.limit }

// Order returns the sort direction.
func (o ReadOptions) Order() Order { return o.order }

// OrderKeyFilter returns the order key filter, or empty string if not set.
func (o ReadOptions) OrderKeyFilter() string { return o.orderKey }

// HasAfter reports whether a cursor was set.
func (o ReadOptions) HasAfter() bool { return o.after != nil }

// Tag returns the single-tag filter, or empty string if not set.
func (o ReadOptions) Tag() string { return o.tag }

// AllTags returns the all-tags filter, or nil if not set.
func (o ReadOptions) AllTags() []string { return o.allTags }

func defaultReadOptions() ReadOptions {
	return ReadOptions{
		limit: 100,
		order: Ascending,
	}
}

// ApplyReadOptions applies the given options and returns the resolved ReadOptions.
// This function is intended for Store implementors.
func ApplyReadOptions(opts ...ReadOption) ReadOptions {
	o := defaultReadOptions()
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// After returns a ReadOption that sets the cursor position.
// Only entries after this ID are returned.
func After[I comparable](id I) ReadOption {
	return func(o *ReadOptions) { o.after = id }
}

// AfterValue returns the cursor value as the requested type.
// Returns the zero value and false if no cursor is set or the type doesn't match.
// This function is intended for Store implementors.
func AfterValue[I comparable](o ReadOptions) (I, bool) {
	if o.after == nil {
		var zero I
		return zero, false
	}
	v, ok := o.after.(I)
	return v, ok
}

// ReadOption configures how entries are read from a stream.
type ReadOption func(*ReadOptions)

// Limit returns a ReadOption that sets the maximum number of entries to return.
// The default limit is 100.
func Limit(n int) ReadOption {
	return func(o *ReadOptions) {
		if n > 0 {
			o.limit = n
		}
	}
}

// WithOrderKey returns a ReadOption that filters entries by ordering key.
func WithOrderKey(key string) ReadOption {
	return func(o *ReadOptions) { o.orderKey = key }
}

// Desc returns a ReadOption that reads entries in descending order (newest first).
func Desc() ReadOption {
	return func(o *ReadOptions) { o.order = Descending }
}

// WithTag returns a ReadOption that filters entries having a specific tag.
func WithTag(tag string) ReadOption {
	return func(o *ReadOptions) { o.tag = tag }
}

// WithAllTags returns a ReadOption that filters entries having ALL specified tags.
func WithAllTags(tags ...string) ReadOption {
	return func(o *ReadOptions) { o.allTags = tags }
}

// ListOptions holds the parsed list parameters.
// This type is intended for Store implementors.
type ListOptions struct {
	after string
	limit int
}

// Limit returns the maximum number of stream IDs to return.
func (o ListOptions) Limit() int { return o.limit }

// After returns the cursor value. Only stream IDs strictly greater than this
// value are returned. Empty string means no cursor.
func (o ListOptions) After() string { return o.after }

// HasAfter reports whether a cursor was set.
func (o ListOptions) HasAfter() bool { return o.after != "" }

func defaultListOptions() ListOptions {
	return ListOptions{limit: 100}
}

// ApplyListOptions applies the given options and returns the resolved ListOptions.
// This function is intended for Store implementors.
func ApplyListOptions(opts ...ListOption) ListOptions {
	o := defaultListOptions()
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// ListOption configures how stream IDs are listed by [Store.ListStreamIDs].
type ListOption func(*ListOptions)

// ListLimit returns a ListOption that sets the maximum number of stream IDs to return.
// The default limit is 100.
func ListLimit(n int) ListOption {
	return func(o *ListOptions) {
		if n > 0 {
			o.limit = n
		}
	}
}

// ListAfter returns a ListOption that sets the cursor position.
// Only stream IDs strictly greater than this value are returned.
func ListAfter(streamID string) ListOption {
	return func(o *ListOptions) { o.after = streamID }
}
