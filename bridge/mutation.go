// Package bridge provides stream bridging between ledger stores.
// A Bridge polls a source store's mutation log and applies changes to a sink store.
//
// Mutation logging must be enabled on the source store via WithMutationLog (SQLite,
// PostgreSQL, and MongoDB backends). The sink store may be any backend.
//
// Eventual consistency: tag and annotation updates may lag behind the source by up to
// one polling interval. This is documented behaviour.
package bridge

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// MutationType identifies the kind of change recorded in a mutation log entry.
type MutationType string

const (
	MutationAppend         MutationType = "append"
	MutationSetTags        MutationType = "set_tags"
	MutationSetAnnotations MutationType = "set_annotations"
	MutationTrim           MutationType = "trim"
)

// MutationEvent is a single change decoded from the mutation log.
type MutationEvent struct {
	Type        MutationType       `json:"type"`
	Stream      string             `json:"stream"`
	Entries     []AppendEntry      `json:"entries,omitempty"`
	EntryID     string             `json:"entry_id,omitempty"`
	Tags        []string           `json:"tags,omitempty"`
	Annotations map[string]*string `json:"annotations,omitempty"`
	BeforeID    string             `json:"before_id,omitempty"`
}

// AppendEntry is one entry within an append mutation event.
type AppendEntry struct {
	ID            string            `json:"id"`
	Payload       json.RawMessage   `json:"payload"`
	OrderKey      string            `json:"order_key,omitempty"`
	DedupKey      string            `json:"dedup_key,omitempty"`
	SchemaVersion int               `json:"schema_version"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	Tags          []string          `json:"tags,omitempty"`
}

// IDCodec serialises and deserialises store IDs to/from string for cursor storage.
// Less must return true if a should be considered an earlier position than b.
type IDCodec[I comparable] interface {
	Encode(I) string
	Decode(string) (I, error)
	Zero() I
	Less(a, b I) bool
}

// Int64Codec implements IDCodec[int64] for SQLite and PostgreSQL stores.
// Cursors are zero-padded to 19 digits so that lexicographic order in the
// cursor column matches numeric order (avoids "9" > "10" edge cases).
type Int64Codec struct{}

func (Int64Codec) Encode(id int64) string         { return fmt.Sprintf("%019d", id) }
func (Int64Codec) Decode(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) }
func (Int64Codec) Zero() int64                    { return 0 }
func (Int64Codec) Less(a, b int64) bool           { return a < b }

// StringCodec implements IDCodec[string] for MongoDB stores.
// Ordering is lexicographic, which is correct for MongoDB ObjectID hex strings
// (first 4 bytes are a big-endian timestamp).
type StringCodec struct{}

func (StringCodec) Encode(id string) string         { return id }
func (StringCodec) Decode(s string) (string, error) { return s, nil }
func (StringCodec) Zero() string                    { return "" }
func (StringCodec) Less(a, b string) bool           { return a < b }
