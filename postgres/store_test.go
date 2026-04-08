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
