package ledger

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
)

// zstdFuzzPayload is a small struct used to exercise the zstd codec round-trip.
type zstdFuzzPayload struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// FuzzZstdUnmarshal feeds arbitrary bytes to zstdCodec.Unmarshal and asserts it
// never panics and only ever returns nil or an error wrapping ErrDecode.
func FuzzZstdUnmarshal(f *testing.F) {
	codec, err := NewZstdCodec[zstdFuzzPayload]()
	if err != nil {
		f.Fatalf("NewZstdCodec: %v", err)
	}
	closeCodec(codec)

	f.Add([]byte(nil))
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x01})
	f.Add([]byte{0x00, '{', '}'})
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"name":"x","count":1}`))
	// A real Marshal output (0x01 + zstd frame).
	if raw, mErr := codec.Marshal(zstdFuzzPayload{Name: "seed", Count: 7}); mErr == nil {
		f.Add([]byte(raw))
		// A TRUNCATED valid zstd frame: keep the 0x01 prefix and the first half of
		// a real compressed frame. The decoder must reject this cleanly (ErrDecode)
		// rather than panicking or hanging on the incomplete frame.
		if len(raw) > 3 {
			f.Add([]byte(raw[:1+(len(raw)-1)/2]))
		}
	}
	// 0x01 prefix followed by garbage (not a valid zstd frame).
	f.Add(append([]byte{0x01}, []byte("not a zstd frame")...))
	// Highly compressible blob marked as plain JSON would fail JSON decode but
	// must still only yield ErrDecode.
	f.Add(append([]byte{0x00}, bytes.Repeat([]byte("A"), 4096)...))

	f.Fuzz(func(t *testing.T, data []byte) {
		var v zstdFuzzPayload
		err := codec.Unmarshal(json.RawMessage(data), &v)
		if err != nil && !errors.Is(err, ErrDecode) {
			t.Errorf("Unmarshal returned error not wrapping ErrDecode: %v", err)
		}
	})
}

// FuzzZstdRoundTrip asserts Marshal then Unmarshal recovers the input value,
// and that the legacy (bare JSON) and explicit-plain (0x00 prefix) branches
// decode to the same value.
func FuzzZstdRoundTrip(f *testing.F) {
	codec, err := NewZstdCodec[zstdFuzzPayload]()
	if err != nil {
		f.Fatalf("NewZstdCodec: %v", err)
	}
	closeCodec(codec)

	f.Add("", 0)
	f.Add("hello", 42)
	f.Add("世界", -1)
	f.Add("a\x00b", 1<<31)

	f.Fuzz(func(t *testing.T, name string, count int) {
		in := zstdFuzzPayload{Name: name, Count: count}

		// encoding/json replaces invalid UTF-8 bytes in strings with U+FFFD, so
		// such a value cannot survive any JSON round-trip — that is a property of
		// encoding/json, not of the zstd codec. The contract being tested is that
		// the codec is transparent over plain JSON: decode(encode(v)) must equal
		// what a reference encoding/json round-trip produces. Compute that
		// reference value (want) once and compare every branch against it.
		jsonBytes, err := json.Marshal(in)
		if err != nil {
			t.Fatalf("json.Marshal(%+v): %v", in, err)
		}
		var want zstdFuzzPayload
		if err := json.Unmarshal(jsonBytes, &want); err != nil {
			t.Fatalf("json.Unmarshal reference: %v", err)
		}

		raw, err := codec.Marshal(in)
		if err != nil {
			t.Fatalf("Marshal(%+v): %v", in, err)
		}
		var got zstdFuzzPayload
		if err := codec.Unmarshal(raw, &got); err != nil {
			t.Fatalf("Unmarshal after Marshal: %v", err)
		}
		if got != want {
			t.Errorf("zstd round-trip mismatch: got %+v want %+v", got, want)
		}

		// The plain-JSON encoding of the same value must decode identically via
		// both the 0x00 (explicit plain) and legacy (no prefix) branches.
		var legacyGot zstdFuzzPayload
		if err := codec.Unmarshal(json.RawMessage(jsonBytes), &legacyGot); err != nil {
			t.Fatalf("Unmarshal legacy (bare JSON): %v", err)
		}
		if legacyGot != want {
			t.Errorf("legacy branch mismatch: got %+v want %+v", legacyGot, want)
		}

		plainMarked := append([]byte{plainVersionByte}, jsonBytes...)
		var plainGot zstdFuzzPayload
		if err := codec.Unmarshal(json.RawMessage(plainMarked), &plainGot); err != nil {
			t.Fatalf("Unmarshal explicit-plain (0x00): %v", err)
		}
		if plainGot != want {
			t.Errorf("explicit-plain branch mismatch: got %+v want %+v", plainGot, want)
		}
	})
}

// closeCodec releases codec resources at the end of the fuzz run.
func closeCodec[T any](c CloseableCodec[T, json.RawMessage]) {
	// Intentionally not closing during fuzzing: the codec is reused across all
	// fuzz iterations. The decoder goroutines are reclaimed at process exit.
	_ = c
}
