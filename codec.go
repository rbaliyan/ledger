package ledger

import "encoding/json"

// PayloadCodec marshals and unmarshals between a domain type T and the
// store-native payload type P (e.g. [encoding/json.RawMessage] for SQL backends,
// [go.mongodb.org/mongo-driver/v2/bson.Raw] for MongoDB).
//
// Each store backend declares its own P; the codec bridges the user's T to it.
type PayloadCodec[T, P any] interface {
	Marshal(v T) (P, error)
	Unmarshal(p P, v *T) error
}

// CloseableCodec is a [PayloadCodec] that holds internal resources (e.g. encoder
// goroutines or reusable buffers) and must be closed when no longer needed.
// [NewZstdCodec] and [NewZstdCodecLevel] return this interface.
type CloseableCodec[T, P any] interface {
	PayloadCodec[T, P]
	Close()
}

// JSONCodec implements [PayloadCodec][T, json.RawMessage] using [encoding/json].
// It is the default codec for the sqlite and postgres backends.
type JSONCodec[T any] struct{}

// Marshal encodes v to JSON.
func (JSONCodec[T]) Marshal(v T) (json.RawMessage, error) { return json.Marshal(v) }

// Unmarshal decodes JSON into v.
func (JSONCodec[T]) Unmarshal(p json.RawMessage, v *T) error { return json.Unmarshal(p, v) }
