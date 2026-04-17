// Package replicate provides stream replication between ledger stores.
// A Replicator polls a source store's mutation log and applies changes to a sink store.
//
// Mutation logging must be enabled on the source store via WithMutationLog (SQLite and
// PostgreSQL backends only). The sink store must be backed by any backend.
//
// Eventual consistency: tag and annotation updates may lag behind the source by up to
// one polling interval. This is documented behaviour.
package replicate

import (
	"encoding/json"
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
type IDCodec[I comparable] interface {
	Encode(I) string
	Decode(string) (I, error)
	Zero() I
}

// Int64Codec implements IDCodec[int64] for SQLite and PostgreSQL stores.
type Int64Codec struct{}

func (Int64Codec) Encode(id int64) string         { return strconv.FormatInt(id, 10) }
func (Int64Codec) Decode(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) }
func (Int64Codec) Zero() int64                    { return 0 }

// StringCodec implements IDCodec[string] for MongoDB stores.
type StringCodec struct{}

func (StringCodec) Encode(id string) string         { return id }
func (StringCodec) Decode(s string) (string, error) { return s, nil }
func (StringCodec) Zero() string                    { return "" }
