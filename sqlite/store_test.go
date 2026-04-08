package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	"github.com/rbaliyan/ledger"
	"github.com/rbaliyan/ledger/sqlite"
	"github.com/rbaliyan/ledger/storetest"
	_ "modernc.org/sqlite"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func newTestStore(t *testing.T) *sqlite.Store {
	t.Helper()
	db := openTestDB(t)
	store, err := sqlite.New(context.Background(), db)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { store.Close(context.Background()) })
	return store
}

func TestConformance(t *testing.T) {
	store := newTestStore(t)
	storetest.RunStoreTests(t, store, ledger.After[int64])
}

func TestNew_InvalidTableName(t *testing.T) {
	db := openTestDB(t)
	_, err := sqlite.New(context.Background(), db, sqlite.WithTable("Robert'; DROP TABLE --"))
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

func TestNew_NilDB(t *testing.T) {
	_, err := sqlite.New(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil db")
	}
}

func TestClosedStoreErrors(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	store.Close(ctx)

	_, err := store.Append(ctx, "stream", ledger.RawEntry{Payload: []byte(`{}`)})
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("Append on closed: %v, want ErrStoreClosed", err)
	}

	_, err = store.Read(ctx, "stream")
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("Read on closed: %v, want ErrStoreClosed", err)
	}

	_, err = store.Count(ctx, "stream")
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("Count on closed: %v, want ErrStoreClosed", err)
	}

	_, err = store.Trim(ctx, "stream", 0)
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("Trim on closed: %v, want ErrStoreClosed", err)
	}

	err = store.Health(ctx)
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("Health on closed: %v, want ErrStoreClosed", err)
	}
}

func TestReadInvalidCursorType(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.Read(ctx, "stream", ledger.After[string]("not-an-int"))
	if err == nil {
		t.Fatal("expected error for string cursor on int64 store")
	}
}

func TestHealth(t *testing.T) {
	store := newTestStore(t)
	if err := store.Health(context.Background()); err != nil {
		t.Fatalf("Health: %v", err)
	}
}

func TestStreamTypedAppendAndRead(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	type Order struct {
		ID     string  `json:"id"`
		Amount float64 `json:"amount"`
	}

	s := ledger.NewStream[int64, Order](store, "orders")

	ids, err := s.Append(ctx, ledger.AppendInput[Order]{
		Payload:  Order{ID: "o-1", Amount: 99.99},
		OrderKey: "customer-1",
		DedupKey: "evt-1",
	})
	if err != nil {
		t.Fatalf("Stream.Append: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("got %d ids, want 1", len(ids))
	}

	entries, err := s.Read(ctx)
	if err != nil {
		t.Fatalf("Stream.Read: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}

	if entries[0].Payload.ID != "o-1" {
		t.Errorf("Payload.ID = %q, want o-1", entries[0].Payload.ID)
	}
	if entries[0].Payload.Amount != 99.99 {
		t.Errorf("Payload.Amount = %f, want 99.99", entries[0].Payload.Amount)
	}
	if entries[0].SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d, want 1", entries[0].SchemaVersion)
	}
}

func TestStreamSchemaUpcast(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	v1Payload, _ := json.Marshal(map[string]any{
		"customer_name": "John",
		"amount":        42,
	})
	store.Append(ctx, "orders", ledger.RawEntry{
		Payload:       v1Payload,
		SchemaVersion: 1,
	})

	type OrderV2 struct {
		Name   string  `json:"name"`
		Email  string  `json:"email"`
		Amount float64 `json:"amount"`
	}

	s := ledger.NewStream[int64, OrderV2](store, "orders",
		ledger.WithSchemaVersion(2),
		ledger.WithUpcaster(ledger.NewFieldMapper(1, 2).
			RenameField("customer_name", "name").
			AddDefault("email", "unknown@example.com")),
	)

	entries, err := s.Read(ctx)
	if err != nil {
		t.Fatalf("Read with upcast: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}

	e := entries[0]
	if e.Payload.Name != "John" {
		t.Errorf("Name = %q, want John", e.Payload.Name)
	}
	if e.Payload.Email != "unknown@example.com" {
		t.Errorf("Email = %q, want unknown@example.com", e.Payload.Email)
	}
	if e.SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d, want 1 (original)", e.SchemaVersion)
	}
}
