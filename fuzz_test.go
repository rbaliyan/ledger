package ledger

import "testing"

func FuzzValidateName(f *testing.F) {
	f.Add("ledger_entries")
	f.Add("my_table")
	f.Add("")
	f.Add("1bad")
	f.Add("Robert'; DROP TABLE --")
	f.Add("a")
	f.Add("_private")

	f.Fuzz(func(t *testing.T, name string) {
		err := ValidateName(name)
		if err == nil && name == "" {
			t.Error("empty name should be invalid")
		}
	})
}

func FuzzJSONCodecRoundTrip(f *testing.F) {
	f.Add([]byte(`{"key":"value"}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`42`))
	f.Add([]byte(`"hello"`))
	f.Add([]byte(`[1,2,3]`))

	codec := JSONCodec{}
	f.Fuzz(func(t *testing.T, data []byte) {
		var v any
		if err := codec.Decode(data, &v); err != nil {
			return // invalid JSON, skip
		}
		encoded, err := codec.Encode(v)
		if err != nil {
			t.Fatalf("Encode after Decode: %v", err)
		}
		var v2 any
		if err := codec.Decode(encoded, &v2); err != nil {
			t.Fatalf("Decode after Encode: %v", err)
		}
	})
}
