package ledger

import (
	"encoding/json"
	"testing"
)

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

	codec := JSONCodec[any]{}
	f.Fuzz(func(t *testing.T, data []byte) {
		var v any
		if err := codec.Unmarshal(json.RawMessage(data), &v); err != nil {
			return // invalid JSON, skip
		}
		encoded, err := codec.Marshal(v)
		if err != nil {
			t.Fatalf("Marshal after Unmarshal: %v", err)
		}
		var v2 any
		if err := codec.Unmarshal(encoded, &v2); err != nil {
			t.Fatalf("Unmarshal after Marshal: %v", err)
		}
	})
}
