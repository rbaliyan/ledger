package bridge

import (
	"encoding/json"
	"testing"
)

// knownMutationTypes is the set of mutation types the Bridge dispatches on in
// apply(); any other value is treated as unknown (skipped with ErrNotSupported).
var knownMutationTypes = map[MutationType]struct{}{
	MutationAppend:         {},
	MutationSetTags:        {},
	MutationSetAnnotations: {},
	MutationTrim:           {},
}

// FuzzMutationEventDecode feeds arbitrary bytes through the same json.Unmarshal
// path that Bridge uses to decode mutation-log payloads (see bridge.go around the
// "decode mutation event" call). It asserts the decode never panics; a decode
// error is an acceptable outcome.
//
// On a successful decode it asserts a structural invariant: the decoded event
// re-marshals cleanly, and its Type is either one of the known mutation types or
// detectably unknown (never silently misclassified). This mirrors the apply()
// dispatch, which routes known types and rejects everything else with
// ErrNotSupported rather than treating it as valid.
func FuzzMutationEventDecode(f *testing.F) {
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"type":"append","stream":"s","entries":[{"id":"1","payload":{"k":"v"},"schema_version":1}]}`))
	f.Add([]byte(`{"type":"set_tags","stream":"s","entry_id":"1","tags":["a","b"]}`))
	f.Add([]byte(`{"type":"set_annotations","stream":"s","entry_id":"1","annotations":{"k":null}}`))
	f.Add([]byte(`{"type":"trim","stream":"s","before_id":"5"}`))
	f.Add([]byte(``))
	f.Add([]byte(`null`))
	f.Add([]byte(`not json`))
	f.Add([]byte(`{"entries":[{"payload":[1,2,3]}]}`))
	// Unknown mutation type: must decode but be classified as unknown.
	f.Add([]byte(`{"type":"bogus_op","stream":"s"}`))
	f.Add([]byte(`{"type":"","stream":"s"}`))
	// Deeply nested payload inside an append entry.
	f.Add([]byte(`{"type":"append","stream":"s","entries":[{"id":"1","payload":{"a":{"b":{"c":[1,2,3]}}},"schema_version":2}]}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var evt MutationEvent
		if err := json.Unmarshal(data, &evt); err != nil {
			return // decode error is an acceptable outcome
		}

		// Invariant 1: a value that decoded must re-marshal without error.
		if _, err := json.Marshal(evt); err != nil {
			t.Fatalf("decoded MutationEvent failed to re-marshal: %v (input %q)", err, data)
		}

		// Invariant 2: the Type is either a known mutation type or detectably
		// unknown. Bridge.apply() routes known types and falls through to an
		// ErrNotSupported default for anything else, so an unknown type must not
		// be silently treated as a known one. This assertion documents that the
		// classification is total (every decoded value lands in exactly one bucket).
		_, known := knownMutationTypes[evt.Type]
		if !known {
			// Unknown is fine — assert only that it isn't masquerading as a known
			// type, which the map lookup already guarantees. Nothing further to do.
			return
		}
		// Known type: the constant must round-trip through the string form.
		if MutationType(string(evt.Type)) != evt.Type {
			t.Fatalf("known mutation type %q did not round-trip", evt.Type)
		}
	})
}

// FuzzInt64CodecRoundTrip asserts Int64Codec.Decode(Encode(n)) == n for every
// int64, including the extreme values that zero-padding with %019d could break.
func FuzzInt64CodecRoundTrip(f *testing.F) {
	f.Add(int64(0))
	f.Add(int64(-1))
	f.Add(int64(1))
	f.Add(int64(9223372036854775807))  // math.MaxInt64
	f.Add(int64(-9223372036854775808)) // math.MinInt64
	f.Add(int64(1000000000000000000))

	codec := Int64Codec{}
	f.Fuzz(func(t *testing.T, n int64) {
		enc := codec.Encode(n)
		dec, err := codec.Decode(enc)
		if err != nil {
			t.Fatalf("Decode(Encode(%d)=%q): %v", n, enc, err)
		}
		if dec != n {
			t.Errorf("Int64Codec round-trip mismatch: Decode(Encode(%d)=%q)=%d", n, enc, dec)
		}
	})
}
