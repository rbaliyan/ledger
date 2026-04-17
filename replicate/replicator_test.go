package replicate_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/rbaliyan/ledger"
	"github.com/rbaliyan/ledger/replicate"
	"github.com/rbaliyan/ledger/sqlite"
	_ "modernc.org/sqlite"
)

type testPayload struct {
	Value string `json:"value"`
}

func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	// In-memory SQLite: each connection gets its own DB. Limit to 1 so
	// all operations share the same connection and see the same tables.
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func newTestStore(t *testing.T, db *sql.DB, table string) *sqlite.Store {
	t.Helper()
	ctx := context.Background()
	store, err := sqlite.New(ctx, db, sqlite.WithTable(table))
	if err != nil {
		t.Fatalf("new store %s: %v", table, err)
	}
	t.Cleanup(func() { store.Close(ctx) })
	return store
}

func newSourceWithMutLog(t *testing.T, db *sql.DB) (*sqlite.Store, *sqlite.Store) {
	t.Helper()
	ctx := context.Background()
	mutStore, err := sqlite.New(ctx, db, sqlite.WithTable("orders_mutations"))
	if err != nil {
		t.Fatalf("new mut store: %v", err)
	}
	source, err := sqlite.New(ctx, db, sqlite.WithTable("orders"), sqlite.WithMutationLog(mutStore))
	if err != nil {
		t.Fatalf("new source store: %v", err)
	}
	t.Cleanup(func() {
		source.Close(ctx)
		mutStore.Close(ctx)
	})
	return source, mutStore
}

// pollReplicator runs one poll cycle using Start+Stop with a 1ms interval.
func pollReplicator[SI comparable, DI comparable](t *testing.T, r *replicate.Replicator[SI, DI]) {
	t.Helper()
	// We can't call unexported poll directly. Instead: Start with tiny interval, wait, Stop.
	// But Replicator.Start only fires after the ticker. We need to call Poll explicitly.
	// Since Poll is not exported, wrap: use a 1ms interval, Start, sleep briefly, Stop.
	r.Start(context.Background())
	time.Sleep(50 * time.Millisecond)
	r.Stop()
}

func TestReplicator_Append(t *testing.T) {
	ctx := context.Background()

	// Source uses same DB for source + mutations (atomic)
	srcDB := newTestDB(t)
	source, mutStore := newSourceWithMutLog(t, srcDB)

	// Sink uses a separate in-memory DB
	sinkDB := newTestDB(t)
	sink := newTestStore(t, sinkDB, "orders")

	// Append to source
	payload, _ := json.Marshal(testPayload{Value: "hello"})
	_, err := source.Append(ctx, "user-1", ledger.RawEntry[json.RawMessage]{
		Payload:       payload,
		SchemaVersion: 1,
	})
	if err != nil {
		t.Fatalf("source append: %v", err)
	}

	// Create and run replicator
	r := replicate.New[int64, int64](mutStore, sink, replicate.Int64Codec{},
		replicate.WithInterval(time.Millisecond),
		replicate.WithName("test"),
	)
	pollReplicator(t, r)

	// Verify sink has the entry
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
	if got.Value != "hello" {
		t.Errorf("expected value 'hello', got %q", got.Value)
	}
}

func TestReplicator_SetTags(t *testing.T) {
	ctx := context.Background()

	srcDB := newTestDB(t)
	source, mutStore := newSourceWithMutLog(t, srcDB)

	sinkDB := newTestDB(t)
	sink := newTestStore(t, sinkDB, "orders")

	r := replicate.New[int64, int64](mutStore, sink, replicate.Int64Codec{},
		replicate.WithInterval(time.Millisecond),
		replicate.WithName("test"),
	)

	// Append to source
	payload, _ := json.Marshal(testPayload{Value: "tags-test"})
	ids, err := source.Append(ctx, "user-1", ledger.RawEntry[json.RawMessage]{
		Payload:       payload,
		SchemaVersion: 1,
	})
	if err != nil {
		t.Fatalf("source append: %v", err)
	}

	// Poll to replicate append
	pollReplicator(t, r)

	// Set tags on source
	if err := source.SetTags(ctx, "user-1", ids[0], []string{"processed"}); err != nil {
		t.Fatalf("source set tags: %v", err)
	}

	// Poll to replicate set_tags
	r2 := replicate.New[int64, int64](mutStore, sink, replicate.Int64Codec{},
		replicate.WithInterval(time.Millisecond),
		replicate.WithName("test"),
	)
	pollReplicator(t, r2)

	// Verify sink has the tags
	entries, err := sink.Read(ctx, "user-1")
	if err != nil {
		t.Fatalf("sink read: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if len(entries[0].Tags) != 1 || entries[0].Tags[0] != "processed" {
		t.Errorf("expected tags [processed], got %v", entries[0].Tags)
	}
}

func TestReplicator_SetAnnotations(t *testing.T) {
	ctx := context.Background()

	srcDB := newTestDB(t)
	source, mutStore := newSourceWithMutLog(t, srcDB)

	sinkDB := newTestDB(t)
	sink := newTestStore(t, sinkDB, "orders")

	r := replicate.New[int64, int64](mutStore, sink, replicate.Int64Codec{},
		replicate.WithInterval(time.Millisecond),
		replicate.WithName("test"),
	)

	// Append to source
	payload, _ := json.Marshal(testPayload{Value: "anno-test"})
	ids, err := source.Append(ctx, "user-1", ledger.RawEntry[json.RawMessage]{
		Payload:       payload,
		SchemaVersion: 1,
	})
	if err != nil {
		t.Fatalf("source append: %v", err)
	}

	// Poll to replicate append
	pollReplicator(t, r)

	// Set annotations on source
	v := "2024-01-01"
	if err := source.SetAnnotations(ctx, "user-1", ids[0], map[string]*string{"processed_at": &v}); err != nil {
		t.Fatalf("source set annotations: %v", err)
	}

	// Poll to replicate set_annotations
	r2 := replicate.New[int64, int64](mutStore, sink, replicate.Int64Codec{},
		replicate.WithInterval(time.Millisecond),
		replicate.WithName("test"),
	)
	pollReplicator(t, r2)

	// Verify sink has the annotations
	entries, err := sink.Read(ctx, "user-1")
	if err != nil {
		t.Fatalf("sink read: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Annotations["processed_at"] != "2024-01-01" {
		t.Errorf("expected annotation processed_at=2024-01-01, got %v", entries[0].Annotations)
	}
}

func TestReplicator_Trim(t *testing.T) {
	ctx := context.Background()

	srcDB := newTestDB(t)
	source, mutStore := newSourceWithMutLog(t, srcDB)

	sinkDB := newTestDB(t)
	sink := newTestStore(t, sinkDB, "orders")

	r := replicate.New[int64, int64](mutStore, sink, replicate.Int64Codec{},
		replicate.WithInterval(time.Millisecond),
		replicate.WithName("test"),
	)

	// Append 3 entries to source
	for i := 0; i < 3; i++ {
		payload, _ := json.Marshal(testPayload{Value: "entry"})
		if _, err := source.Append(ctx, "user-1", ledger.RawEntry[json.RawMessage]{
			Payload:       payload,
			SchemaVersion: 1,
		}); err != nil {
			t.Fatalf("source append: %v", err)
		}
	}

	// Read source IDs
	sourceEntries, err := source.Read(ctx, "user-1")
	if err != nil {
		t.Fatalf("source read: %v", err)
	}
	if len(sourceEntries) != 3 {
		t.Fatalf("expected 3 source entries, got %d", len(sourceEntries))
	}

	// Poll to replicate appends
	pollReplicator(t, r)

	// Verify sink has 3 entries
	sinkEntries, err := sink.Read(ctx, "user-1")
	if err != nil {
		t.Fatalf("sink read: %v", err)
	}
	if len(sinkEntries) != 3 {
		t.Fatalf("expected 3 sink entries after replicate, got %d", len(sinkEntries))
	}

	// Trim first 2 on source (trim ID = second entry ID)
	if _, err := source.Trim(ctx, "user-1", sourceEntries[1].ID); err != nil {
		t.Fatalf("source trim: %v", err)
	}

	// Poll to replicate trim
	r2 := replicate.New[int64, int64](mutStore, sink, replicate.Int64Codec{},
		replicate.WithInterval(time.Millisecond),
		replicate.WithName("test"),
	)
	pollReplicator(t, r2)

	// Verify sink has 1 entry remaining
	sinkEntries, err = sink.Read(ctx, "user-1")
	if err != nil {
		t.Fatalf("sink read after trim: %v", err)
	}
	if len(sinkEntries) != 1 {
		t.Errorf("expected 1 sink entry after trim, got %d", len(sinkEntries))
	}
}

func TestReplicator_CursorPersistence(t *testing.T) {
	ctx := context.Background()

	srcDB := newTestDB(t)
	source, mutStore := newSourceWithMutLog(t, srcDB)

	sinkDB := newTestDB(t)
	sink := newTestStore(t, sinkDB, "orders")

	// Append 2 entries
	for i := 0; i < 2; i++ {
		payload, _ := json.Marshal(testPayload{Value: "entry"})
		if _, err := source.Append(ctx, "user-1", ledger.RawEntry[json.RawMessage]{
			Payload:       payload,
			SchemaVersion: 1,
		}); err != nil {
			t.Fatalf("source append: %v", err)
		}
	}

	// First poll
	r1 := replicate.New[int64, int64](mutStore, sink, replicate.Int64Codec{},
		replicate.WithInterval(time.Millisecond),
		replicate.WithName("cursor-test"),
	)
	pollReplicator(t, r1)

	// Verify 2 entries in sink
	entries, err := sink.Read(ctx, "user-1")
	if err != nil {
		t.Fatalf("sink read: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries after first poll, got %d", len(entries))
	}

	// Append 1 more to source
	payload, _ := json.Marshal(testPayload{Value: "new"})
	if _, err := source.Append(ctx, "user-1", ledger.RawEntry[json.RawMessage]{
		Payload:       payload,
		SchemaVersion: 1,
	}); err != nil {
		t.Fatalf("source append: %v", err)
	}

	// Second poll — should only pick up the new entry (cursor was persisted)
	r2 := replicate.New[int64, int64](mutStore, sink, replicate.Int64Codec{},
		replicate.WithInterval(time.Millisecond),
		replicate.WithName("cursor-test"),
	)
	pollReplicator(t, r2)

	// Verify 3 entries total, no duplicates
	entries, err = sink.Read(ctx, "user-1")
	if err != nil {
		t.Fatalf("sink read: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("expected 3 entries after second poll, got %d", len(entries))
	}
}

func TestReplicator_IdempotentReplay(t *testing.T) {
	ctx := context.Background()

	srcDB := newTestDB(t)
	source, mutStore := newSourceWithMutLog(t, srcDB)

	sinkDB := newTestDB(t)
	sink := newTestStore(t, sinkDB, "orders")

	// Append 1 entry
	payload, _ := json.Marshal(testPayload{Value: "idempotent"})
	_, err := source.Append(ctx, "user-1", ledger.RawEntry[json.RawMessage]{
		Payload:       payload,
		SchemaVersion: 1,
	})
	if err != nil {
		t.Fatalf("source append: %v", err)
	}

	// Poll twice without updating cursor (simulate replay)
	// Both replicators use name "no-cursor-persist" but sink doesn't advance cursor
	// because we use a fresh replicator each time with zero cursor (no persistence)
	// by NOT using a CursorStore-capable sink — but our sink IS a CursorStore.
	// Instead, poll once and verify idempotency via DedupKey or source_id unique index.

	// Poll once
	r1 := replicate.New[int64, int64](mutStore, sink, replicate.Int64Codec{},
		replicate.WithInterval(time.Millisecond),
		replicate.WithName("idempotent-test"),
	)
	pollReplicator(t, r1)

	// Verify 1 entry
	entries, err := sink.Read(ctx, "user-1")
	if err != nil {
		t.Fatalf("sink read after first poll: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after first poll, got %d", len(entries))
	}

	// Reset cursor to force replay
	if err := sink.SetCursor(ctx, "idempotent-test", "0"); err != nil {
		t.Fatalf("reset cursor: %v", err)
	}

	// Poll again — same mutation replayed, source_id unique index prevents duplicate
	r2 := replicate.New[int64, int64](mutStore, sink, replicate.Int64Codec{},
		replicate.WithInterval(time.Millisecond),
		replicate.WithName("idempotent-test"),
	)
	pollReplicator(t, r2)

	// Still 1 entry (unique index on source_id prevented duplicate)
	entries, err = sink.Read(ctx, "user-1")
	if err != nil {
		t.Fatalf("sink read after second poll: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("expected 1 entry after idempotent replay, got %d", len(entries))
	}
}

func TestReadOnlyStream(t *testing.T) {
	ctx := context.Background()

	db := newTestDB(t)
	store := newTestStore(t, db, "orders")

	stream := ledger.NewStream(store, "user-1", ledger.JSONCodec[testPayload]{})
	ros := replicate.NewReadOnlyStream(stream)

	// Write ops should return ErrReadOnly
	_, err := ros.Append(ctx, ledger.AppendInput[testPayload]{Payload: testPayload{Value: "x"}})
	if !errors.Is(err, ledger.ErrReadOnly) {
		t.Errorf("Append: expected ErrReadOnly, got %v", err)
	}

	err = ros.SetTags(ctx, 0, []string{"tag"})
	if !errors.Is(err, ledger.ErrReadOnly) {
		t.Errorf("SetTags: expected ErrReadOnly, got %v", err)
	}

	v := "v"
	err = ros.SetAnnotations(ctx, 0, map[string]*string{"k": &v})
	if !errors.Is(err, ledger.ErrReadOnly) {
		t.Errorf("SetAnnotations: expected ErrReadOnly, got %v", err)
	}

	// ID and SchemaVersion should still work
	if ros.ID() != "user-1" {
		t.Errorf("ID: expected 'user-1', got %q", ros.ID())
	}
	if ros.SchemaVersion() != 1 {
		t.Errorf("SchemaVersion: expected 1, got %d", ros.SchemaVersion())
	}

	// Read should work (empty stream is ok)
	entries, err := ros.Read(ctx)
	if err != nil {
		t.Errorf("Read: unexpected error: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Read: expected 0 entries, got %d", len(entries))
	}

	// Append directly via store, then read through ReadOnlyStream
	payload, _ := json.Marshal(testPayload{Value: "readable"})
	if _, err := store.Append(ctx, "user-1", ledger.RawEntry[json.RawMessage]{Payload: payload, SchemaVersion: 1}); err != nil {
		t.Fatalf("store append: %v", err)
	}

	entries, err = ros.Read(ctx)
	if err != nil {
		t.Errorf("Read after append: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Payload.Value != "readable" {
		t.Errorf("expected value 'readable', got %q", entries[0].Payload.Value)
	}
}
