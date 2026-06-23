package ledger

import (
	"context"
	"encoding/json"
	"testing"
)

// FuzzFieldMapperUpcast feeds arbitrary bytes to a FieldMapper.Upcast and asserts
// it never panics. When Upcast succeeds on a JSON object, it asserts the field
// operations actually happened: the renamed key is gone and its target present,
// the removed key is gone, and the default is applied when its field was missing.
func FuzzFieldMapperUpcast(f *testing.F) {
	f.Add([]byte(`{"old":"v","keep":1}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`[]`))
	f.Add([]byte(``))
	f.Add([]byte(`not json`))
	f.Add([]byte(`{"nested":{"a":[1,2,{"b":true}]}}`))
	f.Add([]byte(`{"legacy_id":99,"email":null}`))
	// old present + email already set: rename happens, default must NOT overwrite.
	f.Add([]byte(`{"old":1,"email":"set@example.com","legacy_id":7}`))
	// new already present alongside old: documents the overwrite behaviour.
	f.Add([]byte(`{"old":1,"new":2}`))
	// deeply nested object — exercises map round-trip on complex values.
	f.Add([]byte(`{"old":{"a":{"b":{"c":[1,2,{"d":3}]}}}}`))

	const (
		oldKey     = "old"
		newKey     = "new"
		defaultKey = "email"
		defaultVal = "unknown@example.com"
		removeKey  = "legacy_id"
	)

	mapper := NewFieldMapper(1, 2).
		RenameField(oldKey, newKey).
		AddDefault(defaultKey, defaultVal).
		RemoveField(removeKey)

	f.Fuzz(func(t *testing.T, data []byte) {
		// Record what the input object looked like before upcasting, so the
		// post-conditions can be checked against the actual input shape.
		var in map[string]any
		hadObject := json.Unmarshal(data, &in) == nil && in != nil

		out, err := mapper.Upcast(context.Background(), json.RawMessage(data))
		if err != nil {
			return // invalid input is an acceptable outcome
		}

		// Upcast only succeeds on JSON it could unmarshal; its output must be
		// valid JSON so downstream decoding never sees corruption.
		if !json.Valid(out) {
			t.Errorf("Upcast produced invalid JSON output %q from input %q", out, data)
		}

		if !hadObject {
			// Non-object JSON (null, arrays, scalars): Upcast returns the input
			// unchanged. No field-level invariants to assert.
			return
		}

		var got map[string]any
		if err := json.Unmarshal(out, &got); err != nil {
			t.Fatalf("Upcast output not a JSON object: %v (out %q)", err, out)
		}

		_, hadOld := in[oldKey]
		_, hadNew := in[newKey]

		// Rename: when "old" existed, it must be removed and "new" must be present.
		// (When the input also had "old" but lacked it, the map lookup is false.)
		if hadOld {
			if _, stillOld := got[oldKey]; stillOld {
				t.Errorf("rename did not remove %q: in=%q out=%q", oldKey, data, out)
			}
			if _, hasNew := got[newKey]; !hasNew {
				t.Errorf("rename did not create %q: in=%q out=%q", newKey, data, out)
			}
		} else if !hadNew {
			// "old" absent and "new" absent in input => "new" must stay absent.
			if _, hasNew := got[newKey]; hasNew {
				t.Errorf("unexpected %q appeared without %q in input: in=%q out=%q", newKey, oldKey, data, out)
			}
		}

		// Removal: the removed key must never survive.
		if _, removed := got[removeKey]; removed {
			t.Errorf("RemoveField(%q) left the key in output: in=%q out=%q", removeKey, data, out)
		}

		// Default: the field must always be present after upcast (added when
		// missing, preserved when already set).
		if _, hasDefault := got[defaultKey]; !hasDefault {
			t.Errorf("default field %q missing from output: in=%q out=%q", defaultKey, data, out)
		}
		// When the input lacked the default field, the configured default value
		// must be the one applied.
		if _, hadDefault := in[defaultKey]; !hadDefault {
			if got[defaultKey] != defaultVal {
				t.Errorf("default field %q = %v, want %q: in=%q out=%q", defaultKey, got[defaultKey], defaultVal, data, out)
			}
		}
	})
}
