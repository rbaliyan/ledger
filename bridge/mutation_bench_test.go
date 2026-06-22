package bridge_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/rbaliyan/ledger/bridge"
)

// BenchmarkInt64CodecEncode measures the zero-padded int64 cursor encoding
// applied on every replicated entry.
func BenchmarkInt64CodecEncode(b *testing.B) {
	codec := bridge.Int64Codec{}
	const id int64 = 1234567890

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = codec.Encode(id)
	}
}

// BenchmarkMutationEventRoundtrip measures Marshal+Unmarshal of an append
// mutation event carrying N entries — the per-poll serialisation cost of the
// bridge replication path.
func BenchmarkMutationEventRoundtrip(b *testing.B) {
	for _, n := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			entries := make([]bridge.AppendEntry, n)
			for i := range entries {
				entries[i] = bridge.AppendEntry{
					ID:            fmt.Sprintf("%019d", i),
					Payload:       json.RawMessage(`{"id":"bench","amount":42.5,"currency":"USD"}`),
					OrderKey:      "order-key",
					SchemaVersion: 1,
					Metadata:      map[string]string{"region": "us-east"},
					Tags:          []string{"priority", "verified"},
				}
			}
			event := bridge.MutationEvent{
				Type:    bridge.MutationAppend,
				Stream:  "user-123",
				Entries: entries,
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				data, err := json.Marshal(event)
				if err != nil {
					b.Fatal(err)
				}
				var decoded bridge.MutationEvent
				if err := json.Unmarshal(data, &decoded); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
