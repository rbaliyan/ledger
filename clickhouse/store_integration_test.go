package clickhouse_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"testing"

	_ "github.com/ClickHouse/clickhouse-go/v2" // register "clickhouse" driver
	"github.com/rbaliyan/ledger"
	"github.com/rbaliyan/ledger/clickhouse"
	"github.com/rbaliyan/ledger/storetest"
)

// TestConformance runs the shared backend-agnostic conformance suite against
// ClickHouse. ClickHouse does not deduplicate on DedupKey (its idempotency
// model collapses same-(stream, id) rows via ReplacingMergeTree on a stable
// SourceID), so the DedupKey subtests are skipped via NoDedupKey. SetTags and
// SetAnnotations return ErrNotSupported; the suite skips those subtests when it
// sees ErrNotSupported. Env-gated on CLICKHOUSE_DSN like the other backends.
func TestConformance(t *testing.T) {
	store, _ := newTestStore(t, "ledger_ch_conformance")
	storetest.RunStoreTests(t, store, ledger.After[string], storetest.TestConfig[json.RawMessage]{
		SamplePayload: json.RawMessage(`{}`),
		NoDedupKey:    true,
	})
}

// newTestStore opens a ClickHouse store against CLICKHOUSE_DSN, creating a
// freshly-named table per test and dropping it on cleanup. Skips when the DSN
// is unset so the unit-test run stays hermetic.
func newTestStore(t *testing.T, table string) (*clickhouse.Store, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("CLICKHOUSE_DSN not set, skipping integration test")
	}
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Drop any leftover table from a previous aborted run before (re)creating.
	db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+table)            //nolint:errcheck
	db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+table+"_cursors") //nolint:errcheck

	store, err := clickhouse.New(context.Background(), db, clickhouse.WithTable(table))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() {
		db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+table)            //nolint:errcheck
		db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+table+"_cursors") //nolint:errcheck
		store.Close(context.Background())
	})
	return store, db
}

// TestReplacingMergeTree_CollapsesDuplicateReads is the core correctness test
// for the idempotent sink. Two appends carrying the same SourceID land as two
// rows sharing (stream, id); ReplacingMergeTree + a FINAL read must collapse
// them to a single entry. Under a plain MergeTree (no FINAL) both rows survive.
func TestReplacingMergeTree_CollapsesDuplicateReads(t *testing.T) {
	store, _ := newTestStore(t, "ledger_ch_dup_test")
	ctx := context.Background()
	const stream = "case-123"

	entry := ledger.RawEntry[json.RawMessage]{
		Payload:  json.RawMessage(`{"v":1}`),
		SourceID: "src-abc", // forces a stable (stream, id) across both appends
	}

	// Two independent appends of the same source entry — the exact shape of a
	// bridge replay / multi-pod re-apply / reconciliation backfill.
	if _, err := store.Append(ctx, stream, entry); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if _, err := store.Append(ctx, stream, entry); err != nil {
		t.Fatalf("second append: %v", err)
	}

	got, err := store.Read(ctx, stream)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Read returned %d entries, want 1 (duplicates must collapse via ReplacingMergeTree+FINAL)", len(got))
	}
	if got[0].ID != "src-abc" {
		t.Fatalf("Read id = %q, want %q", got[0].ID, "src-abc")
	}

	n, err := store.Count(ctx, stream)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("Count = %d, want 1 (FINAL must dedup before counting)", n)
	}

	stat, err := store.Stat(ctx, stream)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if stat.Count != 1 {
		t.Fatalf("Stat.Count = %d, want 1 (FINAL must dedup before counting)", stat.Count)
	}
}

// TestReadPreservesDistinctAlongsideDuplicate guards against accidental
// over-collapse: a stream holding one duplicated source id plus one unique
// source id must read back as exactly two entries. This is the case that would
// fail if the engine were ever keyed on stream alone instead of (stream, id).
func TestReadPreservesDistinctAlongsideDuplicate(t *testing.T) {
	store, _ := newTestStore(t, "ledger_ch_mixed_test")
	ctx := context.Background()
	const stream = "case-mixed"

	dup := ledger.RawEntry[json.RawMessage]{Payload: json.RawMessage(`{"v":1}`), SourceID: "dup"}
	uniq := ledger.RawEntry[json.RawMessage]{Payload: json.RawMessage(`{"v":2}`), SourceID: "uniq"}

	if _, err := store.Append(ctx, stream, dup); err != nil {
		t.Fatalf("append dup #1: %v", err)
	}
	if _, err := store.Append(ctx, stream, dup); err != nil { // re-apply
		t.Fatalf("append dup #2: %v", err)
	}
	if _, err := store.Append(ctx, stream, uniq); err != nil {
		t.Fatalf("append uniq: %v", err)
	}

	got, err := store.Read(ctx, stream)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Read returned %d entries, want 2 (duplicate collapses, distinct survives)", len(got))
	}
}

// TestEmptySourceID_DoesNotCollapse pins the intended semantics that dedup only
// applies to replicated entries carrying a stable SourceID. An entry appended
// without a SourceID gets a fresh generated id each time, so two such appends
// are two distinct rows and must NOT collapse.
func TestEmptySourceID_DoesNotCollapse(t *testing.T) {
	store, _ := newTestStore(t, "ledger_ch_nosrc_test")
	ctx := context.Background()
	const stream = "case-nosrc"

	entry := ledger.RawEntry[json.RawMessage]{Payload: json.RawMessage(`{"v":1}`)} // no SourceID
	if _, err := store.Append(ctx, stream, entry); err != nil {
		t.Fatalf("append #1: %v", err)
	}
	if _, err := store.Append(ctx, stream, entry); err != nil {
		t.Fatalf("append #2: %v", err)
	}

	got, err := store.Read(ctx, stream)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Read returned %d entries, want 2 (no SourceID ⟹ generated ids ⟹ no collapse)", len(got))
	}
}

// TestEntriesTableEngine_IsReplacingMergeTree pins the DDL: the entries table
// must use ReplacingMergeTree so re-applied rows collapse at merge time, and
// must do so with NO version column — the design relies on same-(stream, id)
// rows being byte-identical, so a versioned ReplacingMergeTree(col) is wrong.
func TestEntriesTableEngine_IsReplacingMergeTree(t *testing.T) {
	_, db := newTestStore(t, "ledger_ch_engine_test")
	var ddl string
	if err := db.QueryRowContext(context.Background(),
		"SHOW CREATE TABLE ledger_ch_engine_test").Scan(&ddl); err != nil {
		t.Fatalf("show create table: %v", err)
	}
	if !strings.Contains(ddl, "ReplacingMergeTree") {
		t.Fatalf("entries table engine is not ReplacingMergeTree:\n%s", ddl)
	}
	if strings.Contains(ddl, "ReplacingMergeTree(") {
		t.Fatalf("entries table engine has a version column; design requires bare ReplacingMergeTree:\n%s", ddl)
	}
}

// TestAppendRead_HappyPath guards that adding FINAL to the read path does not
// break ordinary (non-duplicate) reads.
func TestAppendRead_HappyPath(t *testing.T) {
	store, _ := newTestStore(t, "ledger_ch_happy_test")
	ctx := context.Background()
	const stream = "case-happy"

	ids, err := store.Append(ctx, stream,
		ledger.RawEntry[json.RawMessage]{Payload: json.RawMessage(`{"n":1}`), SourceID: "a"},
		ledger.RawEntry[json.RawMessage]{Payload: json.RawMessage(`{"n":2}`), SourceID: "b"},
		ledger.RawEntry[json.RawMessage]{Payload: json.RawMessage(`{"n":3}`), SourceID: "c"},
	)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("append returned %d ids, want 3", len(ids))
	}

	got, err := store.Read(ctx, stream)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("Read returned %d entries, want 3", len(got))
	}
}
