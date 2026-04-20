package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"github.com/rbaliyan/ledger"
	"github.com/rbaliyan/ledger/postgres"
	"github.com/rbaliyan/ledger/storetest"
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
	storetest.RunStoreTests(t, store, ledger.After[int64], storetest.TestConfig[json.RawMessage]{
		SamplePayload: json.RawMessage(`{}`),
	})
}

func TestClosedStoreErrors(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	store.Close(ctx)

	_, err := store.Append(ctx, "stream", ledger.RawEntry[json.RawMessage]{Payload: json.RawMessage(`{}`)})
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
// newFTSStore creates a store with WithFullTextSearch and a unique table name.
func newFTSStore(t *testing.T) *postgres.Store {
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

	store, err := postgres.New(context.Background(), db,
		postgres.WithTable("ledger_fts_test"),
		postgres.WithFullTextSearch(),
	)
	if err != nil {
		t.Fatalf("new FTS store: %v", err)
	}
	t.Cleanup(func() {
		db.Exec("DROP TABLE IF EXISTS ledger_fts_test") //nolint:errcheck
		store.Close(context.Background())
	})
	db.Exec("TRUNCATE TABLE ledger_fts_test RESTART IDENTITY") //nolint:errcheck
	return store
}

// seedSearch appends three entries across two streams and returns them.
func seedSearchEntries(ctx context.Context, t *testing.T, store interface {
	Append(context.Context, string, ...ledger.RawEntry[json.RawMessage]) ([]int64, error)
}) {
	t.Helper()
	payloads := []json.RawMessage{
		json.RawMessage(`{"event":"user_login","user":"alice"}`),
		json.RawMessage(`{"event":"user_logout","user":"alice"}`),
		json.RawMessage(`{"event":"purchase","user":"bob","item":"widget"}`),
	}
	for i, p := range payloads {
		stream := "stream-a"
		if i == 2 {
			stream = "stream-b"
		}
		if _, err := store.Append(ctx, stream, ledger.RawEntry[json.RawMessage]{Payload: p, SchemaVersion: 1}); err != nil {
			t.Fatalf("seed append: %v", err)
		}
	}
}

func TestSearch_ILIKE(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	seedSearchEntries(ctx, t, store)

	// Match across streams — no stream filter.
	results, err := store.Search(ctx, "", "login")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if !json.Valid(results[0].Payload) {
		t.Errorf("payload is not valid JSON: %s", results[0].Payload)
	}

	// Stream-scoped search.
	results, err = store.Search(ctx, "stream-a", "user")
	if err != nil {
		t.Fatalf("Search stream-a: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results for stream-a, want 2", len(results))
	}

	// No match.
	results, err = store.Search(ctx, "", "nonexistent_xyz")
	if err != nil {
		t.Fatalf("Search no-match: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("got %d results for no-match, want 0", len(results))
	}
}

func TestSearch_FTS(t *testing.T) {
	store := newFTSStore(t)
	ctx := context.Background()
	seedSearchEntries(ctx, t, store)

	// Full-text search across all streams.
	results, err := store.Search(ctx, "", "login")
	if err != nil {
		t.Fatalf("Search FTS: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}

	// Stream-scoped FTS.
	results, err = store.Search(ctx, "stream-a", "user")
	if err != nil {
		t.Fatalf("Search FTS stream-a: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
}

func TestEnsureSearchIndex_Postgres(t *testing.T) {
	ctx := context.Background()

	// Without WithFullTextSearch — must return an error.
	store := newTestStore(t)
	if err := store.EnsureSearchIndex(ctx); err == nil {
		t.Fatal("expected error calling EnsureSearchIndex without WithFullTextSearch")
	}

	// With WithFullTextSearch — must be idempotent.
	ftsStore := newFTSStore(t)
	if err := ftsStore.EnsureSearchIndex(ctx); err != nil {
		t.Fatalf("EnsureSearchIndex (first call): %v", err)
	}
	if err := ftsStore.EnsureSearchIndex(ctx); err != nil {
		t.Fatalf("EnsureSearchIndex (second call, idempotent): %v", err)
	}
}

func TestSearch_ClosedStore(t *testing.T) {
	store := newTestStore(t)
	store.Close(context.Background())
	_, err := store.Search(context.Background(), "", "anything")
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("Search on closed store: %v, want ErrStoreClosed", err)
	}
	if err := store.EnsureSearchIndex(context.Background()); !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("EnsureSearchIndex on closed store: %v, want ErrStoreClosed", err)
	}
}

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

	if _, err := orders.Append(ctx, "alice", ledger.RawEntry[json.RawMessage]{Payload: json.RawMessage(`"order"`), SchemaVersion: 1}); err != nil {
		t.Fatalf("append orders: %v", err)
	}
	if _, err := users.Append(ctx, "alice", ledger.RawEntry[json.RawMessage]{Payload: json.RawMessage(`"user"`), SchemaVersion: 1}); err != nil {
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
