package ledger

import (
	"encoding/json"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

const (
	zstdVersionByte  byte = 0x01 // zstd-compressed JSON payload
	plainVersionByte byte = 0x00 // explicit plain-JSON marker
)

// zstdCodec is the concrete implementation; kept unexported so callers depend
// only on [CloseableCodec] and [PayloadCodec], not on internal state layout.
type zstdCodec[T any] struct {
	enc *zstd.Encoder
	dec *zstd.Decoder
}

// compile-time assertion
var _ CloseableCodec[struct{}, json.RawMessage] = (*zstdCodec[struct{}])(nil)

// NewZstdCodec returns a [CloseableCodec][T, json.RawMessage] that compresses
// payloads with zstd at the default compression level.
//
// Newly marshalled payloads are prefixed with a single version byte (0x01)
// followed by the zstd-compressed JSON. Unmarshal handles three cases:
//   - 0x01 prefix: decompress then JSON-decode (current format)
//   - 0x00 prefix: JSON-decode the remaining bytes (explicit plain marker)
//   - any other first byte: treat the entire slice as plain JSON (legacy entries
//     written before this codec was introduced, which start with '{' or '[')
//
// This allows a store to transition from [JSONCodec] to [NewZstdCodec] without
// rewriting existing entries — old and new entries coexist in the same stream.
//
// Call [CloseableCodec.Close] when the codec is no longer needed to release
// internal decoder goroutines.
func NewZstdCodec[T any]() (CloseableCodec[T, json.RawMessage], error) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, fmt.Errorf("ledger/zstd: create encoder: %w", err)
	}
	dec, err := newZstdDecoder()
	if err != nil {
		_ = enc.Close()
		return nil, fmt.Errorf("ledger/zstd: create decoder: %w", err)
	}
	return &zstdCodec[T]{enc: enc, dec: dec}, nil
}

// NewZstdCodecLevel returns a [CloseableCodec][T, json.RawMessage] using the
// specified compression level.
func NewZstdCodecLevel[T any](level zstd.EncoderLevel) (CloseableCodec[T, json.RawMessage], error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
	if err != nil {
		return nil, fmt.Errorf("ledger/zstd: create encoder: %w", err)
	}
	dec, err := newZstdDecoder()
	if err != nil {
		_ = enc.Close()
		return nil, fmt.Errorf("ledger/zstd: create decoder: %w", err)
	}
	return &zstdCodec[T]{enc: enc, dec: dec}, nil
}

// newZstdDecoder creates a decoder with a 64 MiB memory cap to prevent
// decompression bombs from exhausting process memory.
func newZstdDecoder() (*zstd.Decoder, error) {
	return zstd.NewReader(nil,
		zstd.WithDecoderMaxMemory(64<<20),
		zstd.WithDecodeAllCapLimit(true),
	)
}

// Close releases internal encoder and decoder resources.
func (c *zstdCodec[T]) Close() {
	_ = c.enc.Close()
	c.dec.Close()
}

// Marshal JSON-encodes v and zstd-compresses the result with a version prefix.
func (c *zstdCodec[T]) Marshal(v T) (json.RawMessage, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("%w: json marshal: %v", ErrEncode, err)
	}
	compressed := c.enc.EncodeAll(raw, make([]byte, 0, 1+len(raw)/2))
	out := make([]byte, 1+len(compressed))
	out[0] = zstdVersionByte
	copy(out[1:], compressed)
	return json.RawMessage(out), nil
}

// Unmarshal decodes a payload produced by either [NewZstdCodec] or [JSONCodec].
func (c *zstdCodec[T]) Unmarshal(p json.RawMessage, v *T) error {
	if len(p) == 0 {
		return fmt.Errorf("%w: empty payload", ErrDecode)
	}
	var raw []byte
	switch p[0] {
	case zstdVersionByte:
		var err error
		raw, err = c.dec.DecodeAll(p[1:], nil)
		if err != nil {
			return fmt.Errorf("%w: zstd decompress: %v", ErrDecode, err)
		}
	case plainVersionByte:
		raw = p[1:]
	default:
		// Legacy: payload has no version byte — treat the whole slice as plain JSON.
		raw = p
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("%w: json unmarshal: %v", ErrDecode, err)
	}
	return nil
}
