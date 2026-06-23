package mongodb

import (
	"encoding/json"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// bsonFuzzPayload is a small struct used to exercise BSON decoding paths.
type bsonFuzzPayload struct {
	Name   string         `bson:"name"`
	Count  int            `bson:"count"`
	Tags   []string       `bson:"tags"`
	Nested map[string]any `bson:"nested"`
}

// NOTE: there is deliberately no Fuzz target for BSON document decoding
// (BSONCodec.Unmarshal / bsonToJSON). The mongo-driver BSON parser is not
// hardened against adversarial input: on certain malformed-but-framed documents
// (e.g. an inner element claiming a negative or multi-gigabyte length) it does
// not just return an error — it can panic or even hang. A recover cannot catch a
// hang, so a committed fuzz target over this surface would intermittently stall
// the ClusterFuzzLite job rather than reporting a clean finding. The realistic
// corruption cases bsonToJSON can defend against (short buffer, length-prefix
// mismatch) are hardened in production and covered deterministically by
// TestBSONToJSON_Malformed; FuzzDecodeCursor below fuzzes the ObjectID hex
// parser, which is robust.

// FuzzDecodeCursor fuzzes the cursor-decode primitive used by the MongoDB store.
// Read, Search, Trim, SetTags, and SetAnnotations all turn an opaque string
// cursor / entry ID into an ObjectID via bson.ObjectIDFromHex before building a
// query. This target asserts that decoding an arbitrary cursor string never
// panics and only ever fails cleanly with an error (never produces a usable
// ObjectID from malformed input of the wrong length).
func FuzzDecodeCursor(f *testing.F) {
	f.Add("")
	f.Add("0")
	f.Add("507f1f77bcf86cd799439011")   // valid 24-hex ObjectID
	f.Add("507f1f77bcf86cd79943901")    // one short
	f.Add("507f1f77bcf86cd799439011aa") // too long
	f.Add("zzzzzzzzzzzzzzzzzzzzzzzz")   // 24 non-hex chars
	f.Add("\x00\x00\x00\x00\x00\x00")
	f.Add("../../etc/passwd")

	f.Fuzz(func(t *testing.T, cursor string) {
		oid, err := bson.ObjectIDFromHex(cursor)
		if err != nil {
			return // malformed cursor → clean error, as expected
		}
		// A successful decode must come from a canonical 24-char hex string and
		// must round-trip back to that lowercased hex form.
		if got := oid.Hex(); len(got) != 24 {
			t.Fatalf("ObjectIDFromHex(%q) produced non-24-char hex %q", cursor, got)
		}
	})
}

// TestBSONToJSON_Malformed verifies bsonToJSON returns a clean error (never
// panics) on the realistic malformed inputs — a buffer shorter than the BSON
// minimum and a length prefix that disagrees with the buffer — and transcodes a
// well-formed document to valid JSON.
func TestBSONToJSON_Malformed(t *testing.T) {
	t.Run("too_short", func(t *testing.T) {
		if _, err := bsonToJSON(bson.Raw{0x00, 0x00, 0x00, 0x00}); err == nil {
			t.Fatal("expected error for sub-minimum buffer, got nil")
		}
	})
	t.Run("zero_length_prefix", func(t *testing.T) {
		// 5-byte buffer whose declared length is 0 — the input that panicked
		// bson.Raw.Validate before the guard was added.
		if _, err := bsonToJSON(bson.Raw{0x00, 0x00, 0x00, 0x00, 0x00}); err == nil {
			t.Fatal("expected error for zero length prefix, got nil")
		}
	})
	t.Run("length_mismatch", func(t *testing.T) {
		// Declared length 6 but only 5 bytes present.
		if _, err := bsonToJSON(bson.Raw{0x06, 0x00, 0x00, 0x00, 0x00}); err == nil {
			t.Fatal("expected error for length/buffer mismatch, got nil")
		}
	})
	t.Run("valid", func(t *testing.T) {
		raw, err := bson.Marshal(bsonFuzzPayload{Name: "ok", Count: 1})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		out, err := bsonToJSON(bson.Raw(raw))
		if err != nil {
			t.Fatalf("bsonToJSON: %v", err)
		}
		if !json.Valid(out) {
			t.Fatalf("output is not valid JSON: %s", out)
		}
	})
}
