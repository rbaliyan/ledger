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
	// In-memory SQLite: each connection gets its own DB. Limit to 1 so
	// CREATE TABLE and INSERT use the same underlying database.
	db.SetMaxOpenConns(1)
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

	_, err = store.ListStreamIDs(ctx)
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("ListStreamIDs on closed: %v, want ErrStoreClosed", err)
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

// TestCrossStoreIsolation verifies that two stores on the same database but
// different tables do not share any entries: each store represents one type.
func TestCrossStoreIsolation(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	orders, err := sqlite.New(ctx, db, sqlite.WithTable("ledger_orders"))
	if err != nil {
		t.Fatalf("new orders store: %v", err)
	}
	t.Cleanup(func() { orders.Close(ctx) })

	users, err := sqlite.New(ctx, db, sqlite.WithTable("ledger_users"))
	if err != nil {
		t.Fatalf("new users store: %v", err)
	}
	t.Cleanup(func() { users.Close(ctx) })

	// Append to each store with the same stream instance name.
	if _, err := orders.Append(ctx, "alice", ledger.RawEntry{Payload: []byte(`"order"`), SchemaVersion: 1}); err != nil {
		t.Fatalf("append orders: %v", err)
	}
	if _, err := users.Append(ctx, "alice", ledger.RawEntry{Payload: []byte(`"user"`), SchemaVersion: 1}); err != nil {
		t.Fatalf("append users: %v", err)
	}

	// Each store sees only its own entries.
	o, _ := orders.Read(ctx, "alice")
	u, _ := users.Read(ctx, "alice")
	if len(o) != 1 || string(o[0].Payload) != `"order"` {
		t.Errorf("orders/alice payload = %v", o)
	}
	if len(u) != 1 || string(u[0].Payload) != `"user"` {
		t.Errorf("users/alice payload = %v", u)
	}

	// Each store's Type() returns its own table name.
	if orders.Type() != "ledger_orders" {
		t.Errorf("orders.Type() = %q", orders.Type())
	}
	if users.Type() != "ledger_users" {
		t.Errorf("users.Type() = %q", users.Type())
	}

	// ListStreamIDs is scoped per store.
	oIDs, _ := orders.ListStreamIDs(ctx)
	uIDs, _ := users.ListStreamIDs(ctx)
	if len(oIDs) != 1 || oIDs[0] != "alice" {
		t.Errorf("orders ListStreamIDs = %v", oIDs)
	}
	if len(uIDs) != 1 || uIDs[0] != "alice" {
		t.Errorf("users ListStreamIDs = %v", uIDs)
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

func TestWithTx_Commit(t *testing.T) {
	db := openTestDB(t)
	store, err := sqlite.New(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())

	ctx := context.Background()

	// Begin external tx, append via ledger, commit — entry should persist.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	txCtx := ledger.WithTx(ctx, tx)

	ids, err := store.Append(txCtx, "tx-test", ledger.RawEntry{
		Payload:       []byte(`{"txn":"commit"}`),
		SchemaVersion: 1,
	})
	if err != nil {
		t.Fatalf("Append in tx: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("got %d ids, want 1", len(ids))
	}

	// Read within same tx — should see uncommitted entry.
	entries, err := store.Read(txCtx, "tx-test")
	if err != nil {
		t.Fatalf("Read in tx: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Read in tx: got %d entries, want 1", len(entries))
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Read outside tx — should still see entry.
	entries, err = store.Read(ctx, "tx-test")
	if err != nil {
		t.Fatalf("Read after commit: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("Read after commit: got %d entries, want 1", len(entries))
	}
}

func TestWithTx_Rollback(t *testing.T) {
	db := openTestDB(t)
	store, err := sqlite.New(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())

	ctx := context.Background()

	// Begin external tx, append via ledger, rollback — entry should not persist.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	txCtx := ledger.WithTx(ctx, tx)

	_, err = store.Append(txCtx, "tx-rollback", ledger.RawEntry{
		Payload:       []byte(`{"txn":"rollback"}`),
		SchemaVersion: 1,
	})
	if err != nil {
		t.Fatalf("Append in tx: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Read outside tx — should see nothing.
	entries, err := store.Read(ctx, "tx-rollback")
	if err != nil {
		t.Fatalf("Read after rollback: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Read after rollback: got %d entries, want 0", len(entries))
	}
}
