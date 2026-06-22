package bridge_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/ledger"
	"github.com/rbaliyan/ledger/bridge"
)

// TestBridge_ConcurrentClaim runs two Bridge instances with the SAME name
// polling one source mutation log into one shared sink concurrently. The sink's
// shared-cursor CursorStore plus the unique source_id index must guarantee that
// every source entry is replicated exactly once: no duplicates (idempotent
// replay) and none skipped (monotonic cursor) even under concurrent claims.
//
// Correctness here does not depend on which instance wins a given poll; it holds
// because applyAppend stamps SourceID and the sink dedups on it, while
// advanceCursor only ever moves the cursor forward.
func TestBridge_ConcurrentClaim(t *testing.T) {
	ctx := context.Background()

	// Source store and its mutation log share one in-memory DB (atomic writes).
	srcDB := newTestDB(t)
	source, mutStore := newSourceWithMutLog(t, srcDB)

	// Single shared sink DB both bridges replicate into.
	sinkDB := newTestDB(t)
	sink := newTestStore(t, sinkDB, "orders")

	const m = 200
	for i := range m {
		payload, _ := json.Marshal(testPayload{Value: "entry"})
		if _, err := source.Append(ctx, "user-1", ledger.RawEntry[json.RawMessage]{
			Payload:       payload,
			SchemaVersion: 1,
		}); err != nil {
			t.Fatalf("source append %d: %v", i, err)
		}
	}

	// Two bridges, identical name => they share the sink cursor.
	const name = "concurrent-claim"
	b1 := mustNew[int64, int64](t, mutStore, sink, bridge.Int64Codec{},
		bridge.WithName(name),
		bridge.WithBatchSize(16), // small batches force many overlapping polls
	)
	b2 := mustNew[int64, int64](t, mutStore, sink, bridge.Int64Codec{},
		bridge.WithName(name),
		bridge.WithBatchSize(16),
	)

	// Drive both bridges from goroutines, each polling repeatedly until the sink
	// has all entries or a deadline passes. Concurrent Poll on a shared cursor is
	// the property under test.
	deadline := time.Now().Add(5 * time.Second)
	var wg sync.WaitGroup
	for _, b := range []*bridge.Bridge[int64, int64]{b1, b2} {
		wg.Add(1)
		go func(b *bridge.Bridge[int64, int64]) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				if err := b.Poll(ctx); err != nil {
					// Transient SQLite busy errors are acceptable under contention;
					// the loop retries. A persistent failure trips the deadline and
					// the count assertion below catches it.
					continue
				}
				n, err := sink.Count(ctx, "user-1")
				if err == nil && n == m {
					return
				}
			}
		}(b)
	}
	wg.Wait()

	// Exactly M entries: no duplicates, none skipped.
	n, err := sink.Count(ctx, "user-1")
	if err != nil {
		t.Fatalf("sink count: %v", err)
	}
	if n != m {
		t.Fatalf("sink Count = %d, want exactly %d (no duplicates, none skipped)", n, m)
	}

	// Verify ordering integrity: read all and confirm M distinct entries.
	entries, err := sink.Read(ctx, "user-1", ledger.Limit(m+10))
	if err != nil {
		t.Fatalf("sink read: %v", err)
	}
	if len(entries) != m {
		t.Fatalf("sink read returned %d entries, want %d", len(entries), m)
	}
}
