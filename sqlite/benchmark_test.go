package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
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
	// Pin to a single connection: a :memory: database is per-connection, so a
	// pooled second connection would not see the table created on the first.
	db.SetMaxOpenConns(1)
	b.Cleanup(func() { db.Close() })
	// Discard the async index-creation warnings so they don't drown the
	// benchmark output.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := sqlite.New(context.Background(), db, sqlite.WithLogger(logger))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close(context.Background()) })
	return store
}

func BenchmarkAppend(b *testing.B) {
	store := benchStore(b)
	ctx := context.Background()
	payload := json.RawMessage(`{"id":"bench","amount":42.5}`)

	// Pre-generate dedup keys so the timed loop only measures Append.
	dedupKeys := make([]string, b.N)
	for i := range dedupKeys {
		dedupKeys[i] = fmt.Sprintf("d-%d", i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		store.Append(ctx, "bench", ledger.RawEntry[json.RawMessage]{
			Payload:       payload,
			OrderKey:      "key-1",
			DedupKey:      dedupKeys[i],
			SchemaVersion: 1,
		})
	}
}

func BenchmarkAppendBatch(b *testing.B) {
	for _, n := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			store := benchStore(b)
			ctx := context.Background()

			batch := make([]ledger.RawEntry[json.RawMessage], n)
			for i := range batch {
				batch[i] = ledger.RawEntry[json.RawMessage]{
					Payload:       json.RawMessage(`{"id":"bench"}`),
					SchemaVersion: 1,
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				store.Append(ctx, "bench-batch", batch...)
			}
		})
	}
}

func BenchmarkRead(b *testing.B) {
	for _, total := range []int{100, 1000} {
		b.Run(fmt.Sprintf("n=%d", total), func(b *testing.B) {
			store := benchStore(b)
			ctx := context.Background()

			for i := range total {
				store.Append(ctx, "bench-read", ledger.RawEntry[json.RawMessage]{
					Payload:       json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)),
					SchemaVersion: 1,
				})
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				store.Read(ctx, "bench-read", ledger.Limit(100))
			}
		})
	}
}

func BenchmarkReadWithCursor(b *testing.B) {
	store := benchStore(b)
	ctx := context.Background()

	for i := range 1000 {
		store.Append(ctx, "bench-cursor", ledger.RawEntry[json.RawMessage]{
			Payload:       json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)),
			SchemaVersion: 1,
		})
	}

	b.ReportAllocs()
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
