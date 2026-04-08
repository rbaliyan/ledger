package ledger_test

import (
	"context"
	"fmt"

	"github.com/rbaliyan/ledger"
)

// mockStore is a minimal store for examples.
type mockStore struct{}

func (mockStore) Append(_ context.Context, _ string, entries ...ledger.RawEntry) ([]int64, error) {
	ids := make([]int64, len(entries))
	for i := range entries {
		ids[i] = int64(i + 1)
	}
	return ids, nil
}

func (mockStore) Read(_ context.Context, _ string, _ ...ledger.ReadOption) ([]ledger.StoredEntry[int64], error) {
	return nil, nil
}

func (mockStore) Count(_ context.Context, _ string) (int64, error) { return 0, nil }

func (mockStore) Trim(_ context.Context, _ string, _ int64) (int64, error) { return 0, nil }

func (mockStore) Close(_ context.Context) error { return nil }

func ExampleNewStream() {
	store := mockStore{}

	type Order struct {
		ID     string  `json:"id"`
		Amount float64 `json:"amount"`
	}

	// Create a lightweight stream — cheap to create and discard.
	s := ledger.NewStream[int64, Order](store, "orders")

	ids, err := s.Append(context.Background(), ledger.AppendInput[Order]{
		Payload:  Order{ID: "o-1", Amount: 99.99},
		OrderKey: "customer-123",
		DedupKey: "evt-abc",
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("appended:", len(ids), "entries")
	// Output: appended: 1 entries
}

func ExampleNewStream_schemaVersioning() {
	store := mockStore{}

	type OrderV2 struct {
		Name   string  `json:"name"`
		Email  string  `json:"email"`
		Amount float64 `json:"amount"`
	}

	// Create a v2 stream with upcaster from v1
	s := ledger.NewStream[int64, OrderV2](store, "orders",
		ledger.WithSchemaVersion(2),
		ledger.WithUpcaster(ledger.NewFieldMapper(1, 2).
			RenameField("customer_name", "name").
			AddDefault("email", "unknown@example.com")),
	)

	fmt.Println("stream:", s.Name(), "schema version:", s.SchemaVersion())
	// Output: stream: orders schema version: 2
}
