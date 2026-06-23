package otel

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/rbaliyan/ledger"
)

// BenchmarkInstrumentedAppend measures the wrapper overhead on Append with
// tracing off versus on. The traces_off path is the "cheap when disabled"
// claim: it should add only a time.Now() and a metrics-disabled early return
// over the underlying (no-op) store.
func BenchmarkInstrumentedAppend(b *testing.B) {
	ctx := context.Background()
	entry := ledger.RawEntry[json.RawMessage]{Payload: json.RawMessage(`{"id":"bench","amount":42.5}`)}

	b.Run("traces_off", func(b *testing.B) {
		wrapped, err := WrapStore[int64, json.RawMessage](newFakeStore("orders"))
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := wrapped.Append(ctx, "s1", entry); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("traces_on", func(b *testing.B) {
		wrapped, err := WrapStore[int64, json.RawMessage](newFakeStore("orders"),
			WithTracesEnabled(true))
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := wrapped.Append(ctx, "s1", entry); err != nil {
				b.Fatal(err)
			}
		}
	})
}
