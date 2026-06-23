package ledger

import (
	"context"
	"encoding/json"
	"testing"
)

// FuzzUpcastChain drives input bytes through a multi-step upcaster chain
// (v1→v2→v3) via the unexported upcastChain walker. It covers the chain-walking
// logic — finding each step, threading the intermediate result, and stopping at
// the target version — not just a single mapper. It asserts no panic and that a
// successful upcast of valid JSON produces valid JSON.
func FuzzUpcastChain(f *testing.F) {
	f.Add([]byte(`{"v1a":1,"v1b":2}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`[]`))
	f.Add([]byte(``))
	f.Add([]byte(`not json`))
	f.Add([]byte(`{"v1a":"x"}`))
	f.Add([]byte(`{"v2a":"already-renamed","stale":true}`))
	f.Add([]byte(`{"nested":{"a":{"b":[1,2,3]}}}`))

	// Two-step chain: v1→v2 renames v1a→v2a and adds a default; v2→v3 removes a
	// field and adds another default. Walking the whole chain requires both steps.
	step1 := NewFieldMapper(1, 2).
		RenameField("v1a", "v2a").
		AddDefault("added_in_v2", "d2")
	step2 := NewFieldMapper(2, 3).
		RemoveField("stale").
		AddDefault("added_in_v3", "d3")
	chain := []Upcaster[json.RawMessage]{step1, step2}

	f.Fuzz(func(t *testing.T, data []byte) {
		out, err := upcastChain(context.Background(), json.RawMessage(data), 1, 3, chain)
		if err != nil {
			return // invalid input / decode failure is an acceptable outcome
		}
		if !json.Valid(out) {
			t.Errorf("upcastChain produced invalid JSON output %q from input %q", out, data)
		}

		// For a JSON object input that survived the whole chain, both defaults
		// must be present and the removed field gone — confirming every step ran.
		var in map[string]any
		if json.Unmarshal(data, &in) != nil || in == nil {
			return
		}
		var got map[string]any
		if err := json.Unmarshal(out, &got); err != nil {
			t.Fatalf("chain output not a JSON object: %v (out %q)", err, out)
		}
		if _, ok := got["added_in_v2"]; !ok {
			t.Errorf("v1→v2 default missing after chain: in=%q out=%q", data, out)
		}
		if _, ok := got["added_in_v3"]; !ok {
			t.Errorf("v2→v3 default missing after chain: in=%q out=%q", data, out)
		}
		if _, ok := got["stale"]; ok {
			t.Errorf("v2→v3 removal did not run: in=%q out=%q", data, out)
		}
	})
}
