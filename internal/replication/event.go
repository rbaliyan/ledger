package replication

import "encoding/json"

// Event is the JSON-serialized form of a mutation event written to the mutation log.
type Event struct {
	Type        string             `json:"type"`
	Stream      string             `json:"stream"`
	Entries     []EventEntry       `json:"entries,omitempty"`
	EntryID     string             `json:"entry_id,omitempty"`
	Tags        []string           `json:"tags,omitempty"`
	Annotations map[string]*string `json:"annotations,omitempty"`
	BeforeID    string             `json:"before_id,omitempty"`
}

// EventEntry describes one entry in an append mutation event.
type EventEntry struct {
	ID            string            `json:"id"`
	Payload       json.RawMessage   `json:"payload"`
	OrderKey      string            `json:"order_key,omitempty"`
	DedupKey      string            `json:"dedup_key,omitempty"`
	SchemaVersion int               `json:"schema_version"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	Tags          []string          `json:"tags,omitempty"`
}
