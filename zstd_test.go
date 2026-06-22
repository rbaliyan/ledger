package ledger

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/klauspost/compress/zstd"
)

type zstdPayload struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// TestZstdCodec_RoundTrip verifies a payload marshalled by the zstd codec
// decodes back to the original value (zstd-compressed 0x01 format).
func TestZstdCodec_RoundTrip(t *testing.T) {
	t.Parallel()
	codec, err := NewZstdCodec[zstdPayload]()
	if err != nil {
		t.Fatalf("NewZstdCodec: %v", err)
	}
	defer codec.(CloseableCodec[zstdPayload, json.RawMessage]).Close()

	in := zstdPayload{Name: "alpha", Count: 42}
	raw, err := codec.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("Marshal returned empty payload")
	}
	if raw[0] != zstdVersionByte {
		t.Errorf("first byte = %#x, want zstd version byte %#x", raw[0], zstdVersionByte)
	}

	var out zstdPayload
	if err := codec.Unmarshal(raw, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out != in {
		t.Errorf("round-trip = %+v, want %+v", out, in)
	}
}

// TestZstdCodec_RoundTripLargePayload exercises a payload large enough that the
// decompressed output is non-trivial, guarding against a regression where the
// decoder caps output to a nil destination's capacity.
func TestZstdCodec_RoundTripLargePayload(t *testing.T) {
	t.Parallel()
	codec, err := NewZstdCodec[zstdPayload]()
	if err != nil {
		t.Fatalf("NewZstdCodec: %v", err)
	}
	defer codec.(CloseableCodec[zstdPayload, json.RawMessage]).Close()

	big := zstdPayload{Name: makeRepeated("x", 100_000), Count: 1}
	raw, err := codec.Marshal(big)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var out zstdPayload
	if err := codec.Unmarshal(raw, &out); err != nil {
		t.Fatalf("Unmarshal large payload: %v", err)
	}
	if out != big {
		t.Errorf("large round-trip mismatch: name len got %d want %d", len(out.Name), len(big.Name))
	}
}

func makeRepeated(s string, n int) string {
	b := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		b = append(b, s...)
	}
	return string(b)
}

func TestZstdCodecLevel_Marshal(t *testing.T) {
	t.Parallel()
	codec, err := NewZstdCodecLevel[zstdPayload](zstd.SpeedBestCompression)
	if err != nil {
		t.Fatalf("NewZstdCodecLevel: %v", err)
	}
	defer codec.(CloseableCodec[zstdPayload, json.RawMessage]).Close()

	in := zstdPayload{Name: "beta", Count: 7}
	raw, err := codec.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if len(raw) == 0 || raw[0] != zstdVersionByte {
		t.Errorf("Marshal output = %v, want non-empty with zstd version prefix", raw)
	}
}

// TestZstdCodec_DecodeFormats verifies the decoder accepts all three on-wire
// formats: zstd-compressed (0x01), explicit-plain (0x00), and legacy raw JSON.
func TestZstdCodec_DecodeFormats(t *testing.T) {
	t.Parallel()
	codec, err := NewZstdCodec[zstdPayload]()
	if err != nil {
		t.Fatalf("NewZstdCodec: %v", err)
	}
	defer codec.(CloseableCodec[zstdPayload, json.RawMessage]).Close()

	want := zstdPayload{Name: "gamma", Count: 99}
	jsonBytes, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	t.Run("zstd", func(t *testing.T) {
		raw, err := codec.Marshal(want)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if raw[0] != zstdVersionByte {
			t.Fatalf("first byte = %#x, want zstd version byte", raw[0])
		}
		var got zstdPayload
		if err := codec.Unmarshal(raw, &got); err != nil {
			t.Fatalf("Unmarshal zstd: %v", err)
		}
		if got != want {
			t.Errorf("got %+v, want %+v", got, want)
		}
	})

	t.Run("explicit_plain", func(t *testing.T) {
		raw := json.RawMessage(append([]byte{plainVersionByte}, jsonBytes...))
		var got zstdPayload
		if err := codec.Unmarshal(raw, &got); err != nil {
			t.Fatalf("Unmarshal plain: %v", err)
		}
		if got != want {
			t.Errorf("got %+v, want %+v", got, want)
		}
	})

	t.Run("legacy_raw_json", func(t *testing.T) {
		// Golden legacy payload: raw JSON with no version byte. Locks the
		// backward-compatibility path so entries written before the zstd codec
		// existed (which begin with '{') still decode.
		legacy := json.RawMessage(`{"name":"gamma","count":99}`)
		if legacy[0] != '{' {
			t.Fatalf("legacy payload must start with '{', got %#x", legacy[0])
		}
		var got zstdPayload
		if err := codec.Unmarshal(legacy, &got); err != nil {
			t.Fatalf("Unmarshal legacy: %v", err)
		}
		if got != want {
			t.Errorf("got %+v, want %+v", got, want)
		}
	})
}

func TestZstdCodec_EmptyPayload(t *testing.T) {
	t.Parallel()
	codec, err := NewZstdCodec[zstdPayload]()
	if err != nil {
		t.Fatalf("NewZstdCodec: %v", err)
	}
	defer codec.(CloseableCodec[zstdPayload, json.RawMessage]).Close()

	var got zstdPayload
	err = codec.Unmarshal(json.RawMessage(nil), &got)
	if err == nil {
		t.Fatal("expected error on empty payload, got nil")
	}
	if !errors.Is(err, ErrDecode) {
		t.Errorf("error = %v, want ErrDecode", err)
	}
}

func TestZstdCodec_CorruptInput(t *testing.T) {
	t.Parallel()
	codec, err := NewZstdCodec[zstdPayload]()
	if err != nil {
		t.Fatalf("NewZstdCodec: %v", err)
	}
	defer codec.(CloseableCodec[zstdPayload, json.RawMessage]).Close()

	// zstd version byte followed by bytes that are not a valid zstd frame.
	corrupt := json.RawMessage([]byte{zstdVersionByte, 0xde, 0xad, 0xbe, 0xef})
	var got zstdPayload
	err = codec.Unmarshal(corrupt, &got)
	if err == nil {
		t.Fatal("expected error on corrupt zstd input, got nil")
	}
	if !errors.Is(err, ErrDecode) {
		t.Errorf("error = %v, want ErrDecode", err)
	}
}

func TestZstdCodec_ImplementsCloseableCodec(t *testing.T) {
	t.Parallel()
	codec, err := NewZstdCodec[zstdPayload]()
	if err != nil {
		t.Fatalf("NewZstdCodec: %v", err)
	}
	cc, ok := codec.(CloseableCodec[zstdPayload, json.RawMessage])
	if !ok {
		t.Fatal("zstd codec does not implement CloseableCodec")
	}
	// Close must not panic and must be safe to call.
	cc.Close()
}
