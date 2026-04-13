package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"

	"github.com/rbaliyan/ledger"
	"github.com/rbaliyan/ledger/postgres"
	"github.com/rbaliyan/ledger/storetest"
	_ "github.com/lib/pq"
)

func newTestStore(t *testing.T) *postgres.Store {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set, skipping integration test")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	store, err := postgres.New(context.Background(), db, postgres.WithTable("ledger_test_entries"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() {
		db.Exec("DROP TABLE IF EXISTS ledger_test_entries") //nolint:errcheck
		store.Close(context.Background())
	})
	db.Exec("TRUNCATE TABLE ledger_test_entries RESTART IDENTITY") //nolint:errcheck

	return store
}

func TestConformance(t *testing.T) {
	store := newTestStore(t)
	storetest.RunStoreTests(t, store, ledger.After[int64])
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

	_, err = store.ListStreamIDs(ctx)
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("ListStreamIDs on closed: %v, want ErrStoreClosed", err)
	}
}

func TestNew_InvalidTableName(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	_, err = postgres.New(context.Background(), db, postgres.WithTable("bad table name"))
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

func TestNew_NilDB(t *testing.T) {
	_, err := postgres.New(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil db")
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
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set, skipping integration test")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	ctx := context.Background()
	orders, err := postgres.New(ctx, db, postgres.WithTable("ledger_iso_orders"))
	if err != nil {
		t.Fatalf("new orders: %v", err)
	}
	users, err := postgres.New(ctx, db, postgres.WithTable("ledger_iso_users"))
	if err != nil {
		t.Fatalf("new users: %v", err)
	}
	t.Cleanup(func() {
		db.Exec("DROP TABLE IF EXISTS ledger_iso_orders") //nolint:errcheck
		db.Exec("DROP TABLE IF EXISTS ledger_iso_users")  //nolint:errcheck
		orders.Close(ctx)
		users.Close(ctx)
	})
	db.Exec("TRUNCATE TABLE ledger_iso_orders RESTART IDENTITY") //nolint:errcheck
	db.Exec("TRUNCATE TABLE ledger_iso_users RESTART IDENTITY")  //nolint:errcheck

	if _, err := orders.Append(ctx, "alice", ledger.RawEntry{Payload: []byte(`"order"`), SchemaVersion: 1}); err != nil {
		t.Fatalf("append orders: %v", err)
	}
	if _, err := users.Append(ctx, "alice", ledger.RawEntry{Payload: []byte(`"user"`), SchemaVersion: 1}); err != nil {
		t.Fatalf("append users: %v", err)
	}

	o, _ := orders.Read(ctx, "alice")
	u, _ := users.Read(ctx, "alice")
	if len(o) != 1 || string(o[0].Payload) != `"order"` {
		t.Errorf("orders/alice payload = %v", o)
	}
	if len(u) != 1 || string(u[0].Payload) != `"user"` {
		t.Errorf("users/alice payload = %v", u)
	}

	if orders.Type() != "ledger_iso_orders" {
		t.Errorf("orders.Type() = %q", orders.Type())
	}
	if users.Type() != "ledger_iso_users" {
		t.Errorf("users.Type() = %q", users.Type())
	}

	oIDs, _ := orders.ListStreamIDs(ctx)
	uIDs, _ := users.ListStreamIDs(ctx)
	if len(oIDs) != 1 || oIDs[0] != "alice" {
		t.Errorf("orders ListStreamIDs = %v", oIDs)
	}
	if len(uIDs) != 1 || uIDs[0] != "alice" {
		t.Errorf("users ListStreamIDs = %v", uIDs)
	}
}
