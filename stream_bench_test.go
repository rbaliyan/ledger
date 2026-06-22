package ledger_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/rbaliyan/ledger"
	"github.com/rbaliyan/ledger/sqlite"
	_ "modernc.org/sqlite"
)

// streamBenchPayload is the domain type marshalled through the generic codec.
type streamBenchPayload struct {
	OrderID  string  `json:"order_id"`
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
	Status   string  `json:"status"`
}

func newBenchStore(b *testing.B) *sqlite.Store {
	b.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	// Pin to a single connection: a :memory: database is per-connection, so a
	// pooled second connection would not see the table created on the first.
	db.SetMaxOpenConns(1)
	b.Cleanup(func() { db.Close() })
	store, err := sqlite.New(context.Background(), db)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close(context.Background()) })
	return store
}

// BenchmarkStreamAppend measures the end-to-end typed append path: codec
// Marshal + RawEntry construction + backend insert.
func BenchmarkStreamAppend(b *testing.B) {
	store := newBenchStore(b)
	ctx := context.Background()
	stream, err := ledger.NewStream(store, "bench-append", ledger.JSONCodec[streamBenchPayload]{})
	if err != nil {
		b.Fatal(err)
	}
	payload := streamBenchPayload{OrderID: "order-1", Amount: 42.5, Currency: "USD", Status: "ok"}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := stream.Append(ctx, ledger.AppendInput[streamBenchPayload]{Payload: payload}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStreamRead measures the typed read path — backend read plus the
// per-entry upcast+decode loop — with and without an upcaster registered.
// The no_upcaster case skips upcastChain entirely (entry version == stream
// version); the with_upcaster case forces a v1->v2 hop on every entry.
func BenchmarkStreamRead(b *testing.B) {
	ctx := context.Background()
	const total = 100

	b.Run("no_upcaster", func(b *testing.B) {
		store := newBenchStore(b)
		stream, err := ledger.NewStream(store, "bench-read", ledger.JSONCodec[streamBenchPayload]{})
		if err != nil {
			b.Fatal(err)
		}
		payload := streamBenchPayload{OrderID: "order-1", Amount: 42.5, Currency: "USD", Status: "ok"}
		for range total {
			if _, err := stream.Append(ctx, ledger.AppendInput[streamBenchPayload]{Payload: payload}); err != nil {
				b.Fatal(err)
			}
		}

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := stream.Read(ctx, ledger.Limit(total)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("with_upcaster", func(b *testing.B) {
		store := newBenchStore(b)
		// Writer stamps version 1; reader is at version 2 with a rename
		// upcaster, so every entry triggers an upcast on read.
		writer, err := ledger.NewStream(store, "bench-read-up", ledger.JSONCodec[streamBenchPayload]{})
		if err != nil {
			b.Fatal(err)
		}
		payload := streamBenchPayload{OrderID: "order-1", Amount: 42.5, Currency: "USD", Status: "ok"}
		for range total {
			if _, err := writer.Append(ctx, ledger.AppendInput[streamBenchPayload]{Payload: payload}); err != nil {
				b.Fatal(err)
			}
		}

		reader, err := ledger.NewStream(store, "bench-read-up", ledger.JSONCodec[streamBenchPayload]{},
			ledger.WithSchemaVersion[json.RawMessage](2),
			ledger.WithUpcaster(ledger.NewFieldMapper(1, 2).RenameField("status", "state")),
		)
		if err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := reader.Read(ctx, ledger.Limit(total)); err != nil {
				b.Fatal(err)
			}
		}
	})
}
