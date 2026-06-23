package ledgerpb

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/rbaliyan/ledger"
)

// BenchmarkStoredFromInt64 measures converting a page of stored entries from
// the int64 backend representation to the wire StoredEntry — the per-Read
// marshalling cost in the gRPC server, dominated by strconv.FormatInt on each
// ID.
func BenchmarkStoredFromInt64(b *testing.B) {
	now := time.Now()
	const n = 100
	entries := make([]ledger.StoredEntry[int64, json.RawMessage], n)
	for i := range entries {
		entries[i] = ledger.StoredEntry[int64, json.RawMessage]{
			ID:            int64(i + 1),
			Stream:        "user-123",
			Payload:       json.RawMessage(`{"id":"bench","amount":42.5,"currency":"USD"}`),
			OrderKey:      "order-key",
			DedupKey:      "dedup-key",
			SchemaVersion: 1,
			Metadata:      map[string]string{"region": "us-east"},
			Tags:          []string{"priority", "verified"},
			Annotations:   map[string]string{"reviewed": "true"},
			CreatedAt:     now,
		}
	}

	out := make([]StoredEntry, n)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for i, e := range entries {
			out[i] = storedFromInt64(e)
		}
	}
}
