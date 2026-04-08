// Package ledger provides an append-only log with typed generic entries,
// schema versioning, deduplication, and pluggable storage backends.
//
// The core abstraction is a two-level generic design:
//   - [Store] is the backend interface, generic over the ID type (int64 for SQL, string for MongoDB).
//   - [Stream] is a lightweight typed handle, generic over both ID and payload types.
//
// Streams are cheap to create — one per operation, then discard. Schema versioning
// with [Upcaster] chains enables safe payload evolution without downtime.
//
// Backends: sqlite, postgres, mongodb.
package ledger

import (
	"context"
	"time"
)

// Store is the backend storage interface for append-only log entries.
// I is the ID type used for cursor-based iteration (e.g., int64 for SQL, string for MongoDB).
//
// Append semantics: SQL backends (sqlite, postgres) use transactions for atomic
// batch inserts. The MongoDB backend uses InsertMany with ordered:false, meaning
// partial success is possible on non-dedup errors.
type Store[I comparable] interface {
	// Append adds entries to the named stream. Returns the IDs of newly appended entries.
	// Entries with a non-empty DedupKey that already exists in the stream are silently skipped.
	Append(ctx context.Context, stream string, entries ...RawEntry) ([]I, error)

	// Read returns entries from the named stream, ordered by ID.
	// Reading a non-existent stream returns nil, nil.
	Read(ctx context.Context, stream string, opts ...ReadOption) ([]StoredEntry[I], error)

	// Count returns the number of entries in the named stream.
	Count(ctx context.Context, stream string) (int64, error)

	// Trim deletes entries from the named stream with IDs less than or equal to beforeID.
	// Returns the number of entries deleted.
	Trim(ctx context.Context, stream string, beforeID I) (int64, error)

	// Close releases resources held by the store. The caller is responsible for
	// closing the underlying database connection.
	Close(ctx context.Context) error
}

// HealthChecker is an optional interface that Store implementations may provide
// to report backend health (e.g., database connectivity).
type HealthChecker interface {
	Health(ctx context.Context) error
}

// RawEntry is an entry with an already-encoded payload, ready for storage.
type RawEntry struct {
	Payload       []byte            // Encoded payload bytes.
	OrderKey      string            // Ordering key for filtering (e.g., aggregate ID).
	DedupKey      string            // Deduplication key. Empty means no dedup.
	SchemaVersion int               // Schema version of the payload.
	Metadata      map[string]string // Arbitrary key-value metadata.
}

// StoredEntry is a raw entry read back from the store, including its assigned ID and timestamp.
type StoredEntry[I comparable] struct {
	ID            I                 // Store-assigned unique ID.
	Stream        string            // Stream name this entry belongs to.
	Payload       []byte            // Encoded payload bytes.
	OrderKey      string            // Ordering key.
	DedupKey      string            // Deduplication key.
	SchemaVersion int               // Schema version at write time.
	Metadata      map[string]string // Arbitrary key-value metadata.
	CreatedAt     time.Time         // Timestamp when the entry was stored.
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
}

// Limit returns the maximum number of entries to return.
func (o ReadOptions) Limit() int { return o.limit }

// Order returns the sort direction.
func (o ReadOptions) Order() Order { return o.order }

// OrderKeyFilter returns the order key filter, or empty string if not set.
func (o ReadOptions) OrderKeyFilter() string { return o.orderKey }

// HasAfter reports whether a cursor was set.
func (o ReadOptions) HasAfter() bool { return o.after != nil }

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
