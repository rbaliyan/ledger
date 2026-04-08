package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/rbaliyan/ledger"
	"github.com/rbaliyan/ledger/sqlite"
	_ "modernc.org/sqlite"
)

func benchStore(b *testing.B) *sqlite.Store {
	b.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	store, err := sqlite.New(context.Background(), db)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close(context.Background()) })
	return store
}

func BenchmarkAppend(b *testing.B) {
	store := benchStore(b)
	ctx := context.Background()
	entry := ledger.RawEntry{
		Payload:       []byte(`{"id":"bench","amount":42.5}`),
		OrderKey:      "key-1",
		SchemaVersion: 1,
	}

	b.ResetTimer()
	for i := range b.N {
		store.Append(ctx, "bench", ledger.RawEntry{
			Payload:       entry.Payload,
			OrderKey:      entry.OrderKey,
			DedupKey:      fmt.Sprintf("d-%d", i),
			SchemaVersion: entry.SchemaVersion,
		})
	}
}

func BenchmarkAppendBatch(b *testing.B) {
	store := benchStore(b)
	ctx := context.Background()

	batch := make([]ledger.RawEntry, 100)
	for i := range batch {
		batch[i] = ledger.RawEntry{
			Payload:       []byte(`{"id":"bench"}`),
			SchemaVersion: 1,
		}
	}

	b.ResetTimer()
	for range b.N {
		store.Append(ctx, "bench-batch", batch...)
	}
}

func BenchmarkRead(b *testing.B) {
	store := benchStore(b)
	ctx := context.Background()

	for i := range 1000 {
		store.Append(ctx, "bench-read", ledger.RawEntry{
			Payload:       []byte(fmt.Sprintf(`{"i":%d}`, i)),
			SchemaVersion: 1,
		})
	}

	b.ResetTimer()
	for range b.N {
		store.Read(ctx, "bench-read", ledger.Limit(100))
	}
}

func BenchmarkReadWithCursor(b *testing.B) {
	store := benchStore(b)
	ctx := context.Background()

	for i := range 1000 {
		store.Append(ctx, "bench-cursor", ledger.RawEntry{
			Payload:       []byte(fmt.Sprintf(`{"i":%d}`, i)),
			SchemaVersion: 1,
		})
	}

	b.ResetTimer()
	for range b.N {
		var cursor int64
		for {
			opts := []ledger.ReadOption{ledger.Limit(100)}
			if cursor > 0 {
				opts = append(opts, ledger.After(cursor))
			}
			entries, _ := store.Read(ctx, "bench-cursor", opts...)
			if len(entries) == 0 {
				break
			}
			cursor = entries[len(entries)-1].ID
		}
	}
}
