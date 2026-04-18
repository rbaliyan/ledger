package ledgerpb

import (
	"context"
	"encoding/json"
	"time"
)

// ReadOptions configures how entries are fetched from a Backend.
// It mirrors ledger.ReadOptions but uses plain Go values instead of functional
// options so adapters can inspect the cursor without type assertions.
type ReadOptions struct {
	After    string   // cursor: only entries with ID > After are returned; "" means start
	Limit    int      // 0 means backend default (100)
	Desc     bool     // newest-first when true
	OrderKey string   // filter by order_key field
	Tag      string   // filter entries that carry this single tag
	AllTags  []string // filter entries that carry ALL of these tags
}

// InputEntry is a single entry to be appended to a stream.
type InputEntry struct {
	Payload       json.RawMessage
	OrderKey      string
	DedupKey      string
	SchemaVersion int
	Metadata      map[string]string
	Tags          []string
}

// StoredEntry is an entry read back from a Backend.
type StoredEntry struct {
	ID            string
	Stream        string
	Payload       json.RawMessage
	OrderKey      string
	DedupKey      string
	SchemaVersion int
	Metadata      map[string]string
	Tags          []string
	Annotations   map[string]string
	CreatedAt     time.Time
	UpdatedAt     *time.Time
}

// Backend is the uniform store interface the Server wraps.
// It uses string IDs and json.RawMessage payloads so the gRPC server remains
// backend-agnostic. Use NewInt64Backend or NewStringBackend to wrap an existing
// ledger store.
type Backend interface {
	// Append adds entries to the named stream and returns the IDs of written entries.
	Append(ctx context.Context, stream string, entries ...InputEntry) ([]string, error)

	// Read returns entries from the named stream according to opts.
	Read(ctx context.Context, stream string, opts ReadOptions) ([]StoredEntry, error)

	// Count returns the total number of entries in the stream.
	Count(ctx context.Context, stream string) (int64, error)

	// SetTags replaces all tags on the identified entry.
	SetTags(ctx context.Context, stream string, id string, tags []string) error

	// SetAnnotations merges annotations; nil-valued keys are deleted.
	SetAnnotations(ctx context.Context, stream string, id string, annotations map[string]*string) error

	// Trim deletes entries with ID <= beforeID and returns the count deleted.
	Trim(ctx context.Context, stream string, beforeID string) (int64, error)

	// ListStreamIDs returns paginated distinct stream IDs.
	// after is a cursor (stream ID); limit 0 means backend default.
	ListStreamIDs(ctx context.Context, after string, limit int) ([]string, error)

	// Health reports backend connectivity. Returns nil when healthy.
	Health(ctx context.Context) error
}

// StreamRenamer is an optional extension of Backend for backends that support
// renaming human-readable stream names without touching entries.
// The muxBackend in internal/server implements this interface.
type StreamRenamer interface {
	RenameStream(ctx context.Context, oldName, newName string) error
}
