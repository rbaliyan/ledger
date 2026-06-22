package ledgerpb

import (
	"strconv"
	"testing"
)

// FuzzParseIntID feeds arbitrary strings to parseIntID (the wire string-ID parse
// path used by the int64 provider). It asserts no panic and that any successful
// parse round-trips back to the same decimal string via strconv.FormatInt.
func FuzzParseIntID(f *testing.F) {
	f.Add("")
	f.Add("0")
	f.Add("1")
	f.Add("-1")
	f.Add("9223372036854775807")
	f.Add("-9223372036854775808")
	f.Add("9223372036854775808") // overflow
	f.Add("01")
	f.Add(" 1")
	f.Add("1.0")
	f.Add("0x10")
	f.Add("not a number")

	f.Fuzz(func(t *testing.T, s string) {
		n, err := parseIntID(s)
		if err != nil {
			return // malformed/empty input is an acceptable outcome
		}
		// A successful parse must round-trip: the canonical decimal form must
		// re-parse to the same value.
		reparsed, err := parseIntID(strconv.FormatInt(n, 10))
		if err != nil {
			t.Fatalf("parseIntID(FormatInt(%d)) failed: %v", n, err)
		}
		if reparsed != n {
			t.Errorf("round-trip mismatch: parseIntID(%q)=%d, reparse=%d", s, n, reparsed)
		}
	})
}
