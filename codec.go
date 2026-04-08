package ledger

import "encoding/json"

// Codec encodes and decodes entry payloads.
type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
}

// JSONCodec is the default codec using encoding/json.
type JSONCodec struct{}

// Encode marshals v to JSON.
func (JSONCodec) Encode(v any) ([]byte, error) { return json.Marshal(v) }

// Decode unmarshals JSON data into v.
func (JSONCodec) Decode(data []byte, v any) error { return json.Unmarshal(data, v) }
