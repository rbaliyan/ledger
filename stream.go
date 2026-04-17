package ledger

import (
	"context"
	"fmt"
	"time"
)

// Entry is a typed entry read back from a stream.
type Entry[I comparable, T any] struct {
	ID            I                 // Store-assigned unique ID.
	Stream        string            // Stream name this entry belongs to.
	Payload       T                 // Decoded payload.
	OrderKey      string            // Ordering key.
	DedupKey      string            // Deduplication key.
	SchemaVersion int               // Schema version at write time (before upcasting).
	Metadata      map[string]string // Immutable key-value metadata (set at append).
	Tags          []string          // Mutable tags (updated via SetTags).
	Annotations   map[string]string // Mutable annotations (updated via SetAnnotations).
	CreatedAt     time.Time         // Timestamp when the entry was stored.
	UpdatedAt     *time.Time        // Timestamp of last tag/annotation update.
}

// AppendInput describes an entry to append to a stream.
type AppendInput[T any] struct {
	Payload  T                 // Payload to encode and store.
	OrderKey string            // Ordering key for filtering (e.g., aggregate ID).
	DedupKey string            // Deduplication key. Empty means no dedup.
	Metadata map[string]string // Immutable key-value metadata.
	Tags     []string          // Initial tags (can be updated later via SetTags).
}

// options holds per-stream configuration that does not depend on the domain type T.
// P is the store-native payload type.
type options[P any] struct {
	schemaVersion int
	upcasters     []Upcaster[P]
}

// Option configures a Stream. P is the store-native payload type.
type Option[P any] func(*options[P])

// WithSchemaVersion sets the schema version stamped on new entries.
// Defaults to 1. When reading entries with older versions, registered
// upcasters are applied to transform the payload before decoding.
func WithSchemaVersion[P any](v int) Option[P] {
	return func(o *options[P]) {
		if v > 0 {
			o.schemaVersion = v
		}
	}
}

// WithUpcaster registers an upcaster for transforming payloads from one
// schema version to the next. Register in sequence (v1→v2, v2→v3).
func WithUpcaster[P any](u Upcaster[P]) Option[P] {
	return func(o *options[P]) { o.upcasters = append(o.upcasters, u) }
}

// Stream is a lightweight, typed handle to a stream instance within a store.
//
//   - I is the store ID type (e.g. int64 for SQL, string for MongoDB).
//   - P is the store-native payload type (e.g. json.RawMessage, bson.Raw).
//   - T is the user's domain payload type.
//
// The codec bridges T and P on every append and read. The Store represents
// the entity type (table/collection); the Stream's id identifies the
// particular instance within that type.
//
// It is cheap to create — create one per operation and discard it.
// Stream is safe for concurrent use.
type Stream[I comparable, P any, T any] struct {
	id            string
	store         Store[I, P]
	codec         PayloadCodec[T, P]
	schemaVersion int
	upcasters     []Upcaster[P]
}

// NewStream creates a lightweight stream handle. The stream does not need to
// exist in the store beforehand — it is created implicitly on first append.
// The id identifies the stream instance within the store's type.
//
// codec is required and must not be nil. For SQL backends use [JSONCodec];
// for MongoDB use the BSONCodec provided by the mongodb package.
//
// Returns an error if store or codec is nil.
func NewStream[I comparable, P any, T any](
	store Store[I, P],
	id string,
	codec PayloadCodec[T, P],
	opts ...Option[P],
) (Stream[I, P, T], error) {
	if store == nil {
		return Stream[I, P, T]{}, fmt.Errorf("ledger: NewStream called with nil store")
	}
	if codec == nil {
		return Stream[I, P, T]{}, fmt.Errorf("ledger: NewStream called with nil codec")
	}
	o := options[P]{
		schemaVersion: 1,
	}
	for _, fn := range opts {
		fn(&o)
	}
	return Stream[I, P, T]{
		id:            id,
		store:         store,
		codec:         codec,
		schemaVersion: o.schemaVersion,
		upcasters:     o.upcasters,
	}, nil
}

// ID returns the stream instance ID within the store's type.
func (s Stream[I, P, T]) ID() string { return s.id }

// SchemaVersion returns the current schema version used for new entries.
func (s Stream[I, P, T]) SchemaVersion() int { return s.schemaVersion }

// Append encodes and appends entries to the stream. Returns IDs of newly appended entries.
// Each entry is stamped with the stream's current schema version.
// Entries with duplicate dedup keys are silently skipped.
func (s Stream[I, P, T]) Append(ctx context.Context, entries ...AppendInput[T]) ([]I, error) {
	raw := make([]RawEntry[P], len(entries))
	for i, e := range entries {
		data, err := s.codec.Marshal(e.Payload)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrEncode, err)
		}
		raw[i] = RawEntry[P]{
			Payload:       data,
			OrderKey:      e.OrderKey,
			DedupKey:      e.DedupKey,
			SchemaVersion: s.schemaVersion,
			Metadata:      e.Metadata,
			Tags:          e.Tags,
		}
	}
	return s.store.Append(ctx, s.id, raw...)
}

// Read returns decoded entries from the stream. Entries written with an older
// schema version are automatically upcasted to the current version before
// decoding into T.
func (s Stream[I, P, T]) Read(ctx context.Context, opts ...ReadOption) ([]Entry[I, T], error) {
	stored, err := s.store.Read(ctx, s.id, opts...)
	if err != nil {
		return nil, err
	}
	entries := make([]Entry[I, T], len(stored))
	for i, se := range stored {
		payload := se.Payload

		if se.SchemaVersion > 0 && se.SchemaVersion < s.schemaVersion {
			payload, err = upcastChain(ctx, payload, se.SchemaVersion, s.schemaVersion, s.upcasters)
			if err != nil {
				return nil, fmt.Errorf("entry %v: %w", se.ID, err)
			}
		}

		var decoded T
		if err := s.codec.Unmarshal(payload, &decoded); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrDecode, err)
		}
		entries[i] = Entry[I, T]{
			ID:            se.ID,
			Stream:        se.Stream,
			Payload:       decoded,
			OrderKey:      se.OrderKey,
			DedupKey:      se.DedupKey,
			SchemaVersion: se.SchemaVersion,
			Metadata:      se.Metadata,
			Tags:          se.Tags,
			Annotations:   se.Annotations,
			CreatedAt:     se.CreatedAt,
			UpdatedAt:     se.UpdatedAt,
		}
	}
	return entries, nil
}

// SetTags replaces all tags on an entry in this stream.
func (s Stream[I, P, T]) SetTags(ctx context.Context, id I, tags []string) error {
	return s.store.SetTags(ctx, s.id, id, tags)
}

// SetAnnotations merges annotations into an entry in this stream.
func (s Stream[I, P, T]) SetAnnotations(ctx context.Context, id I, annotations map[string]*string) error {
	return s.store.SetAnnotations(ctx, s.id, id, annotations)
}
