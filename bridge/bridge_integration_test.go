package bridge_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"

	_ "github.com/ClickHouse/clickhouse-go/v2" // register "clickhouse" driver
	_ "github.com/lib/pq"                      // register "postgres" driver
	_ "modernc.org/sqlite"                     // register "sqlite" driver

	"github.com/rbaliyan/ledger"
	"github.com/rbaliyan/ledger/bridge"
	"github.com/rbaliyan/ledger/clickhouse"
	"github.com/rbaliyan/ledger/postgres"
	"github.com/rbaliyan/ledger/sqlite"
)

// TestBridge_PostgresToClickHouse replicates a Postgres source (with an atomic
// mutation log) to a ClickHouse sink end-to-end via bridge.Poll. It asserts the
// payload lands and that an idempotent replay collapses on the ReplacingMergeTree
// sink (the entry is not duplicated). Env-gated on POSTGRES_DSN + CLICKHOUSE_DSN.
func TestBridge_PostgresToClickHouse(t *testing.T) {
	pgDSN := os.Getenv("POSTGRES_DSN")
	chDSN := os.Getenv("CLICKHOUSE_DSN")
	if pgDSN == "" || chDSN == "" {
		t.Skip("POSTGRES_DSN or CLICKHOUSE_DSN not set, skipping cross-backend integration test")
	}
	ctx := context.Background()

	// --- Postgres source + mutation log (same DB, atomic) ---
	pgDB, err := sql.Open("postgres", pgDSN)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() { pgDB.Close() })

	const (
		srcTable = "ledger_bridge_src"
		mutTable = "ledger_bridge_src_mutations"
	)
	pgDB.Exec("DROP TABLE IF EXISTS " + srcTable) //nolint:errcheck
	pgDB.Exec("DROP TABLE IF EXISTS " + mutTable) //nolint:errcheck

	mutStore, err := postgres.New(ctx, pgDB, postgres.WithTable(mutTable))
	if err != nil {
		t.Fatalf("new pg mutation store: %v", err)
	}
	source, err := postgres.New(ctx, pgDB, postgres.WithTable(srcTable), postgres.WithMutationLog(mutStore))
	if err != nil {
		t.Fatalf("new pg source: %v", err)
	}
	t.Cleanup(func() {
		pgDB.Exec("DROP TABLE IF EXISTS " + srcTable) //nolint:errcheck
		pgDB.Exec("DROP TABLE IF EXISTS " + mutTable) //nolint:errcheck
		source.Close(ctx)
		mutStore.Close(ctx)
	})

	// --- ClickHouse sink ---
	chDB, err := sql.Open("clickhouse", chDSN)
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { chDB.Close() })

	const sinkTable = "ledger_bridge_sink"
	chDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+sinkTable)            //nolint:errcheck
	chDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+sinkTable+"_cursors") //nolint:errcheck
	sink, err := clickhouse.New(ctx, chDB, clickhouse.WithTable(sinkTable))
	if err != nil {
		t.Fatalf("new clickhouse sink: %v", err)
	}
	t.Cleanup(func() {
		chDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+sinkTable)            //nolint:errcheck
		chDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+sinkTable+"_cursors") //nolint:errcheck
		sink.Close(ctx)
	})

	// Append to the source.
	payload, _ := json.Marshal(testPayload{Value: "pg-to-ch"})
	if _, err := source.Append(ctx, "user-1", ledger.RawEntry[json.RawMessage]{
		Payload:       payload,
		SchemaVersion: 1,
	}); err != nil {
		t.Fatalf("source append: %v", err)
	}

	// ClickHouse cannot apply tag/annotation mutations — skip them at the sink.
	newBridge := func(name string) *bridge.Bridge[int64, string] {
		return mustNew[int64, string](t, mutStore, sink, bridge.Int64Codec{},
			bridge.WithName(name),
			bridge.WithSkipMutationTypes(bridge.MutationSetTags, bridge.MutationSetAnnotations),
		)
	}

	// First replication pass.
	pollBridge(t, newBridge("pg-ch"))

	entries, err := sink.Read(ctx, "user-1")
	if err != nil {
		t.Fatalf("sink read: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry in sink, got %d", len(entries))
	}
	var got testPayload
	if err := json.Unmarshal(entries[0].Payload, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Value != "pg-to-ch" {
		t.Errorf("expected value 'pg-to-ch', got %q", got.Value)
	}

	// Idempotent replay: reset the cursor and poll again. The same mutation is
	// re-applied, but the ReplacingMergeTree sink collapses on (stream, source_id)
	// via a FINAL read, so the entry count stays at 1.
	if err := sink.SetCursor(ctx, "pg-ch", "0"); err != nil {
		t.Fatalf("reset cursor: %v", err)
	}
	pollBridge(t, newBridge("pg-ch"))

	entries, err = sink.Read(ctx, "user-1")
	if err != nil {
		t.Fatalf("sink read after replay: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("expected 1 entry after idempotent replay, got %d", len(entries))
	}
}

// TestBridge_SQLiteToPostgres replicates a SQLite source (with an atomic
// mutation log) to a Postgres sink end-to-end via bridge.Poll, asserting the
// payload lands and a replay does not duplicate it (source_id unique index).
// Env-gated on POSTGRES_DSN; SQLite runs in-process.
func TestBridge_SQLiteToPostgres(t *testing.T) {
	pgDSN := os.Getenv("POSTGRES_DSN")
	if pgDSN == "" {
		t.Skip("POSTGRES_DSN not set, skipping cross-backend integration test")
	}
	ctx := context.Background()

	// --- SQLite source + mutation log (same in-memory DB) ---
	srcDB := newTestDB(t)
	mutStore, err := sqlite.New(ctx, srcDB, sqlite.WithTable("orders_mutations"))
	if err != nil {
		t.Fatalf("new sqlite mutation store: %v", err)
	}
	source, err := sqlite.New(ctx, srcDB, sqlite.WithTable("orders"), sqlite.WithMutationLog(mutStore))
	if err != nil {
		t.Fatalf("new sqlite source: %v", err)
	}
	t.Cleanup(func() {
		source.Close(ctx)
		mutStore.Close(ctx)
	})

	// --- Postgres sink ---
	pgDB, err := sql.Open("postgres", pgDSN)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() { pgDB.Close() })

	const sinkTable = "ledger_bridge_pg_sink"
	pgDB.Exec("DROP TABLE IF EXISTS " + sinkTable) //nolint:errcheck
	sink, err := postgres.New(ctx, pgDB, postgres.WithTable(sinkTable))
	if err != nil {
		t.Fatalf("new postgres sink: %v", err)
	}
	t.Cleanup(func() {
		pgDB.Exec("DROP TABLE IF EXISTS " + sinkTable) //nolint:errcheck
		sink.Close(ctx)
	})

	payload, _ := json.Marshal(testPayload{Value: "sqlite-to-pg"})
	if _, err := source.Append(ctx, "user-1", ledger.RawEntry[json.RawMessage]{
		Payload:       payload,
		SchemaVersion: 1,
	}); err != nil {
		t.Fatalf("source append: %v", err)
	}

	newBridge := func(name string) *bridge.Bridge[int64, int64] {
		return mustNew[int64, int64](t, mutStore, sink, bridge.Int64Codec{},
			bridge.WithName(name),
		)
	}

	pollBridge(t, newBridge("sqlite-pg"))

	entries, err := sink.Read(ctx, "user-1")
	if err != nil {
		t.Fatalf("sink read: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry in sink, got %d", len(entries))
	}
	var got testPayload
	if err := json.Unmarshal(entries[0].Payload, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Value != "sqlite-to-pg" {
		t.Errorf("expected value 'sqlite-to-pg', got %q", got.Value)
	}

	// Idempotent replay via cursor reset; source_id unique index prevents a duplicate.
	if err := sink.SetCursor(ctx, "sqlite-pg", "0"); err != nil {
		t.Fatalf("reset cursor: %v", err)
	}
	pollBridge(t, newBridge("sqlite-pg"))

	entries, err = sink.Read(ctx, "user-1")
	if err != nil {
		t.Fatalf("sink read after replay: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("expected 1 entry after idempotent replay, got %d", len(entries))
	}
}
