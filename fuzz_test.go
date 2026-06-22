package ledger

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"
)

// safeIdentifier is an independent re-statement of the safe-identifier contract
// that ValidateName must enforce. Keeping it separate from validate.go's regexp
// means the fuzzer cross-checks the contract rather than the implementation
// against itself.
var safeIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func FuzzValidateName(f *testing.F) {
	f.Add("ledger_entries")
	f.Add("my_table")
	f.Add("")
	f.Add("1bad")
	f.Add("Robert'; DROP TABLE --")
	f.Add("a")
	f.Add("_private")
	f.Add("a b")
	f.Add("a-b")
	f.Add("a;b")
	f.Add("a\"b")

	f.Fuzz(func(t *testing.T, name string) {
		err := ValidateName(name)
		if err == nil && name == "" {
			t.Error("empty name should be invalid")
		}
		if err != nil {
			if !errors.Is(err, ErrInvalidName) {
				t.Errorf("ValidateName(%q) error not ErrInvalidName: %v", name, err)
			}
			return
		}
		// On acceptance the name must satisfy the safe-identifier contract and
		// must contain no SQL metacharacters that could enable injection when
		// the name is interpolated into a table/collection identifier.
		if !safeIdentifier.MatchString(name) {
			t.Errorf("ValidateName accepted %q but it does not match the safe-identifier contract", name)
		}
		for _, bad := range []string{"'", "\"", "`", ";", "-", "/", "\\", "*", " ", "\t", "\n", "\r", "(", ")", "."} {
			if strings.Contains(name, bad) {
				t.Errorf("ValidateName accepted %q containing SQL metacharacter %q", name, bad)
			}
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
