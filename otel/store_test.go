package otel

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/rbaliyan/ledger"
)

// fakeStore is a minimal in-memory Store[int64, json.RawMessage] for tests.
type fakeStore struct {
	entries map[string][]ledger.StoredEntry[int64, json.RawMessage]
	nextID  int64
	closed  bool
	table   string
}

func newFakeStore(table string) *fakeStore {
	return &fakeStore{
		entries: make(map[string][]ledger.StoredEntry[int64, json.RawMessage]),
		table:   table,
	}
}

func (f *fakeStore) Type() string { return f.table }

func (f *fakeStore) Append(_ context.Context, stream string, entries ...ledger.RawEntry[json.RawMessage]) ([]int64, error) {
	if f.closed {
		return nil, ledger.ErrStoreClosed
	}
	ids := make([]int64, len(entries))
	for i, e := range entries {
		f.nextID++
		id := f.nextID
		f.entries[stream] = append(f.entries[stream], ledger.StoredEntry[int64, json.RawMessage]{
			ID:      id,
			Stream:  stream,
			Payload: e.Payload,
		})
		ids[i] = id
	}
	return ids, nil
}

func (f *fakeStore) Read(_ context.Context, stream string, _ ...ledger.ReadOption) ([]ledger.StoredEntry[int64, json.RawMessage], error) {
	if f.closed {
		return nil, ledger.ErrStoreClosed
	}
	return f.entries[stream], nil
}

func (f *fakeStore) Count(_ context.Context, stream string) (int64, error) {
	if f.closed {
		return 0, ledger.ErrStoreClosed
	}
	return int64(len(f.entries[stream])), nil
}

func (f *fakeStore) Stat(_ context.Context, stream string) (ledger.StreamStat[int64], error) {
	if f.closed {
		return ledger.StreamStat[int64]{}, ledger.ErrStoreClosed
	}
	entries := f.entries[stream]
	stat := ledger.StreamStat[int64]{Stream: stream, Count: int64(len(entries))}
	if len(entries) > 0 {
		stat.FirstID = entries[0].ID
		stat.LastID = entries[len(entries)-1].ID
	}
	return stat, nil
}

func (f *fakeStore) SetTags(_ context.Context, stream string, id int64, _ []string) error {
	if f.closed {
		return ledger.ErrStoreClosed
	}
	for _, e := range f.entries[stream] {
		if e.ID == id {
			return nil
		}
	}
	return ledger.ErrEntryNotFound
}

func (f *fakeStore) SetAnnotations(_ context.Context, stream string, id int64, _ map[string]*string) error {
	if f.closed {
		return ledger.ErrStoreClosed
	}
	for _, e := range f.entries[stream] {
		if e.ID == id {
			return nil
		}
	}
	return ledger.ErrEntryNotFound
}

func (f *fakeStore) Trim(_ context.Context, stream string, beforeID int64) (int64, error) {
	if f.closed {
		return 0, ledger.ErrStoreClosed
	}
	var kept []ledger.StoredEntry[int64, json.RawMessage]
	var deleted int64
	for _, e := range f.entries[stream] {
		if e.ID <= beforeID {
			deleted++
		} else {
			kept = append(kept, e)
		}
	}
	f.entries[stream] = kept
	return deleted, nil
}

func (f *fakeStore) ListStreamIDs(_ context.Context, _ ...ledger.ListOption) ([]string, error) {
	if f.closed {
		return nil, ledger.ErrStoreClosed
	}
	ids := make([]string, 0, len(f.entries))
	for k := range f.entries {
		ids = append(ids, k)
	}
	return ids, nil
}

func (f *fakeStore) Close(_ context.Context) error {
	f.closed = true
	return nil
}

func (f *fakeStore) Health(_ context.Context) error {
	if f.closed {
		return ledger.ErrStoreClosed
	}
	return nil
}

var (
	_ ledger.Store[int64, json.RawMessage] = (*fakeStore)(nil)
	_ ledger.HealthChecker                 = (*fakeStore)(nil)
)

// noHealthStore wraps fakeStore but does not expose Health.
type noHealthStore struct{ *fakeStore }

func (n *noHealthStore) Close(ctx context.Context) error { return n.fakeStore.Close(ctx) }

var _ ledger.Store[int64, json.RawMessage] = (*noHealthStore)(nil)

func rawEntry(v string) ledger.RawEntry[json.RawMessage] {
	return ledger.RawEntry[json.RawMessage]{Payload: json.RawMessage(`"` + v + `"`)}
}

// --- WrapStore ---

func TestWrapStore(t *testing.T) {
	store := newFakeStore("orders")
	wrapped, err := WrapStore(store)
	if err != nil {
		t.Fatalf("WrapStore: %v", err)
	}
	if wrapped == nil {
		t.Fatal("WrapStore returned nil")
	}
}

func TestWrapStore_CapturesStoreType(t *testing.T) {
	store := newFakeStore("orders")
	wrapped, _ := WrapStore(store)
	if wrapped.storeType != "orders" {
		t.Errorf("storeType = %q, want %q", wrapped.storeType, "orders")
	}
}

func TestWrapStore_DefaultsDisabled(t *testing.T) {
	wrapped, _ := WrapStore(newFakeStore("t"))
	if wrapped.opts.enableTraces {
		t.Error("traces should be disabled by default")
	}
	if wrapped.opts.enableMetrics {
		t.Error("metrics should be disabled by default")
	}
}

func TestWrapStore_WithOptions(t *testing.T) {
	wrapped, err := WrapStore(newFakeStore("t"),
		WithBackendName("sqlite"),
		WithTracesEnabled(true),
		WithMetricsEnabled(true),
	)
	if err != nil {
		t.Fatalf("WrapStore: %v", err)
	}
	if wrapped.opts.backendName != "sqlite" {
		t.Errorf("backendName = %q", wrapped.opts.backendName)
	}
}

func TestUnwrap(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store)
	if wrapped.Unwrap() != store {
		t.Error("Unwrap should return the original store")
	}
}

// --- Append ---

func TestAppend_TracingDisabled(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store, WithMetricsEnabled(true))

	ctx := context.Background()
	ids, err := wrapped.Append(ctx, "s1", rawEntry("a"), rawEntry("b"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(ids) != 2 {
		t.Errorf("Append returned %d IDs, want 2", len(ids))
	}
}

func TestAppend_TracingEnabled(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	ids, err := wrapped.Append(ctx, "s1", rawEntry("a"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(ids) != 1 {
		t.Errorf("got %d IDs", len(ids))
	}
}

func TestAppend_Error(t *testing.T) {
	store := newFakeStore("t")
	_ = store.Close(context.Background())
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	_, err := wrapped.Append(context.Background(), "s1", rawEntry("a"))
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("want ErrStoreClosed, got %v", err)
	}
}

// --- Read ---

func TestRead_TracingDisabled(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store, WithMetricsEnabled(true))

	ctx := context.Background()
	_, _ = wrapped.Append(ctx, "s1", rawEntry("x"))

	entries, err := wrapped.Read(ctx, "s1")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("Read returned %d entries, want 1", len(entries))
	}
}

func TestRead_TracingEnabled(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_, _ = wrapped.Append(ctx, "s1", rawEntry("x"), rawEntry("y"))
	entries, err := wrapped.Read(ctx, "s1")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("got %d entries", len(entries))
	}
}

func TestRead_Error(t *testing.T) {
	store := newFakeStore("t")
	_ = store.Close(context.Background())
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	_, err := wrapped.Read(context.Background(), "s1")
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("want ErrStoreClosed, got %v", err)
	}
}

// --- Count ---

func TestCount_TracingDisabled(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store, WithMetricsEnabled(true))

	ctx := context.Background()
	_, _ = wrapped.Append(ctx, "s1", rawEntry("a"), rawEntry("b"))

	n, err := wrapped.Count(ctx, "s1")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 2 {
		t.Errorf("Count = %d, want 2", n)
	}
}

func TestCount_TracingEnabled(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_, _ = wrapped.Append(ctx, "s1", rawEntry("a"))
	n, err := wrapped.Count(ctx, "s1")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 1 {
		t.Errorf("Count = %d, want 1", n)
	}
}

func TestCount_Error(t *testing.T) {
	store := newFakeStore("t")
	_ = store.Close(context.Background())
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	_, err := wrapped.Count(context.Background(), "s1")
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("want ErrStoreClosed, got %v", err)
	}
}

// --- SetTags ---

func TestSetTags_TracingDisabled(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store, WithMetricsEnabled(true))

	ctx := context.Background()
	ids, _ := wrapped.Append(ctx, "s1", rawEntry("a"))
	if err := wrapped.SetTags(ctx, "s1", ids[0], []string{"tag1"}); err != nil {
		t.Fatalf("SetTags: %v", err)
	}
}

func TestSetTags_TracingEnabled(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	ids, _ := wrapped.Append(ctx, "s1", rawEntry("a"))
	if err := wrapped.SetTags(ctx, "s1", ids[0], []string{"tag1"}); err != nil {
		t.Fatalf("SetTags: %v", err)
	}
}

func TestSetTags_Error(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	err := wrapped.SetTags(context.Background(), "s1", 999, nil)
	if !errors.Is(err, ledger.ErrEntryNotFound) {
		t.Errorf("want ErrEntryNotFound, got %v", err)
	}
}

// --- SetAnnotations ---

func TestSetAnnotations_TracingDisabled(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store, WithMetricsEnabled(true))

	ctx := context.Background()
	ids, _ := wrapped.Append(ctx, "s1", rawEntry("a"))
	v := "val"
	if err := wrapped.SetAnnotations(ctx, "s1", ids[0], map[string]*string{"k": &v}); err != nil {
		t.Fatalf("SetAnnotations: %v", err)
	}
}

func TestSetAnnotations_TracingEnabled(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	ids, _ := wrapped.Append(ctx, "s1", rawEntry("a"))
	if err := wrapped.SetAnnotations(ctx, "s1", ids[0], nil); err != nil {
		t.Fatalf("SetAnnotations: %v", err)
	}
}

func TestSetAnnotations_Error(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	err := wrapped.SetAnnotations(context.Background(), "s1", 999, nil)
	if !errors.Is(err, ledger.ErrEntryNotFound) {
		t.Errorf("want ErrEntryNotFound, got %v", err)
	}
}

// --- Trim ---

func TestTrim_TracingDisabled(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store, WithMetricsEnabled(true))

	ctx := context.Background()
	ids, _ := wrapped.Append(ctx, "s1", rawEntry("a"), rawEntry("b"), rawEntry("c"))
	n, err := wrapped.Trim(ctx, "s1", ids[1])
	if err != nil {
		t.Fatalf("Trim: %v", err)
	}
	if n != 2 {
		t.Errorf("Trim deleted %d, want 2", n)
	}
}

func TestTrim_TracingEnabled(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	ids, _ := wrapped.Append(ctx, "s1", rawEntry("a"), rawEntry("b"))
	n, err := wrapped.Trim(ctx, "s1", ids[0])
	if err != nil {
		t.Fatalf("Trim: %v", err)
	}
	if n != 1 {
		t.Errorf("Trim deleted %d, want 1", n)
	}
}

func TestTrim_Error(t *testing.T) {
	store := newFakeStore("t")
	_ = store.Close(context.Background())
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	_, err := wrapped.Trim(context.Background(), "s1", 1)
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("want ErrStoreClosed, got %v", err)
	}
}

// --- ListStreamIDs ---

func TestListStreamIDs_TracingDisabled(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store, WithMetricsEnabled(true))

	ctx := context.Background()
	_, _ = wrapped.Append(ctx, "s1", rawEntry("a"))
	_, _ = wrapped.Append(ctx, "s2", rawEntry("b"))

	ids, err := wrapped.ListStreamIDs(ctx)
	if err != nil {
		t.Fatalf("ListStreamIDs: %v", err)
	}
	if len(ids) != 2 {
		t.Errorf("ListStreamIDs = %d, want 2", len(ids))
	}
}

func TestListStreamIDs_TracingEnabled(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_, _ = wrapped.Append(ctx, "s1", rawEntry("a"))
	ids, err := wrapped.ListStreamIDs(ctx)
	if err != nil {
		t.Fatalf("ListStreamIDs: %v", err)
	}
	if len(ids) != 1 {
		t.Errorf("got %d stream IDs", len(ids))
	}
}

func TestListStreamIDs_Error(t *testing.T) {
	store := newFakeStore("t")
	_ = store.Close(context.Background())
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	_, err := wrapped.ListStreamIDs(context.Background())
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("want ErrStoreClosed, got %v", err)
	}
}

// --- Close ---

func TestClose_TracingDisabled(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store)
	if err := wrapped.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestClose_TracingEnabled(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store, WithTracesEnabled(true))
	if err := wrapped.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// --- Health ---

func TestHealth_WithHealthChecker_TracingDisabled(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store)
	if err := wrapped.Health(context.Background()); err != nil {
		t.Fatalf("Health: %v", err)
	}
}

func TestHealth_WithHealthChecker_TracingEnabled(t *testing.T) {
	store := newFakeStore("t")
	wrapped, _ := WrapStore(store, WithTracesEnabled(true))
	if err := wrapped.Health(context.Background()); err != nil {
		t.Fatalf("Health: %v", err)
	}
}

func TestHealth_Error(t *testing.T) {
	store := newFakeStore("t")
	_ = store.Close(context.Background())
	wrapped, _ := WrapStore(store, WithTracesEnabled(true))

	err := wrapped.Health(context.Background())
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("want ErrStoreClosed, got %v", err)
	}
}

func TestHealth_NoHealthChecker(t *testing.T) {
	store := &noHealthStore{newFakeStore("t")}
	wrapped, _ := WrapStore[int64, json.RawMessage](store, WithTracesEnabled(true))
	// Should return nil, not panic
	if err := wrapped.Health(context.Background()); err != nil {
		t.Errorf("Health with non-HealthChecker store should return nil, got %v", err)
	}
}

// --- errorType ---

func TestErrorType(t *testing.T) {
	tests := []struct {
		err      error
		expected string
	}{
		{ledger.ErrEntryNotFound, "not_found"},
		{ledger.ErrStoreClosed, "store_closed"},
		{ledger.ErrInvalidCursor, "invalid_cursor"},
		{ledger.ErrEncode, "encode"},
		{ledger.ErrDecode, "decode"},
		{errors.New("unknown"), "internal"},
	}
	for _, tt := range tests {
		got := errorType(tt.err)
		if got != tt.expected {
			t.Errorf("errorType(%v) = %q, want %q", tt.err, got, tt.expected)
		}
	}
}

// --- commonAttributes ---

func TestCommonAttributes_WithStoreType(t *testing.T) {
	store := newFakeStore("orders")
	wrapped, _ := WrapStore(store, WithBackendName("sqlite"))
	attrs := wrapped.commonAttributes()
	if len(attrs) != 2 {
		t.Errorf("want 2 attrs (backend + store_type), got %d", len(attrs))
	}
}

func TestCommonAttributes_WithoutStoreType(t *testing.T) {
	store := &noHealthStore{newFakeStore("")}
	wrapped, _ := WrapStore[int64, json.RawMessage](store, WithBackendName("sqlite"))
	attrs := wrapped.commonAttributes()
	if len(attrs) != 1 {
		t.Errorf("want 1 attr (backend only), got %d", len(attrs))
	}
}

// --- Optional-interface forwarding (CursorStore + FindBySourceID) ---

// cursorFakeStore extends fakeStore with the optional bridge-replication
// interfaces (ledger.CursorStore and FindBySourceID) so the forwarding paths
// can be exercised. The plain fakeStore deliberately omits these, serving as
// the negative (ErrNotSupported) case.
type cursorFakeStore struct {
	*fakeStore
	cursors   map[string]string
	sourceIDs map[string]int64 // key: stream + "\x00" + sourceID
}

func newCursorFakeStore(table string) *cursorFakeStore {
	return &cursorFakeStore{
		fakeStore: newFakeStore(table),
		cursors:   make(map[string]string),
		sourceIDs: make(map[string]int64),
	}
}

// Append records the SourceID->local-ID mapping so FindBySourceID can resolve
// replicated entries (StoredEntry does not retain SourceID).
func (c *cursorFakeStore) Append(ctx context.Context, stream string, entries ...ledger.RawEntry[json.RawMessage]) ([]int64, error) {
	ids, err := c.fakeStore.Append(ctx, stream, entries...)
	if err != nil {
		return ids, err
	}
	for i, e := range entries {
		if e.SourceID != "" {
			c.sourceIDs[stream+"\x00"+e.SourceID] = ids[i]
		}
	}
	return ids, nil
}

func (c *cursorFakeStore) GetCursor(_ context.Context, name string) (string, bool, error) {
	if c.closed {
		return "", false, ledger.ErrStoreClosed
	}
	v, ok := c.cursors[name]
	return v, ok, nil
}

func (c *cursorFakeStore) SetCursor(_ context.Context, name, cursor string) error {
	if c.closed {
		return ledger.ErrStoreClosed
	}
	c.cursors[name] = cursor
	return nil
}

func (c *cursorFakeStore) FindBySourceID(_ context.Context, stream, sourceID string) (int64, bool, error) {
	if c.closed {
		return 0, false, ledger.ErrStoreClosed
	}
	id, ok := c.sourceIDs[stream+"\x00"+sourceID]
	return id, ok, nil
}

var (
	_ ledger.Store[int64, json.RawMessage] = (*cursorFakeStore)(nil)
	_ ledger.CursorStore                   = (*cursorFakeStore)(nil)
)

// TestWrapStore_ForwardsCursorAndLookup verifies that wrapping a store which
// implements CursorStore and FindBySourceID preserves those capabilities: the
// type assertions the bridge performs must still succeed through the wrapper,
// and the calls must delegate to the inner store.
func TestWrapStore_ForwardsCursorAndLookup(t *testing.T) {
	ctx := context.Background()
	inner := newCursorFakeStore("orders")
	wrapped, err := WrapStore[int64, json.RawMessage](inner)
	if err != nil {
		t.Fatalf("WrapStore: %v", err)
	}

	// The bridge discovers capabilities via these exact assertions
	// (bridge.New: sink.(ledger.CursorStore), sink.(sinkLookup[DI])).
	cs, ok := ledger.Store[int64, json.RawMessage](wrapped).(ledger.CursorStore)
	if !ok {
		t.Fatal("wrapped store does not satisfy ledger.CursorStore")
	}
	lookup, ok := ledger.Store[int64, json.RawMessage](wrapped).(interface {
		FindBySourceID(context.Context, string, string) (int64, bool, error)
	})
	if !ok {
		t.Fatal("wrapped store does not satisfy the FindBySourceID lookup interface")
	}

	// SetCursor/GetCursor delegate to the inner store.
	if err := cs.SetCursor(ctx, "bridge-1", "0000000000000000042"); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}
	if got := inner.cursors["bridge-1"]; got != "0000000000000000042" {
		t.Errorf("inner cursor = %q, want the value set through the wrapper", got)
	}
	cur, found, err := cs.GetCursor(ctx, "bridge-1")
	if err != nil || !found || cur != "0000000000000000042" {
		t.Errorf("GetCursor = (%q, %v, %v), want (0000000000000000042, true, nil)", cur, found, err)
	}

	// FindBySourceID delegates and maps a replicated source ID to the local ID.
	id, err := inner.Append(ctx, "user-1", ledger.RawEntry[json.RawMessage]{
		Payload:  json.RawMessage(`"x"`),
		SourceID: "src-7",
	})
	if err != nil {
		t.Fatalf("inner append: %v", err)
	}
	localID, found, err := lookup.FindBySourceID(ctx, "user-1", "src-7")
	if err != nil || !found || localID != id[0] {
		t.Errorf("FindBySourceID = (%d, %v, %v), want (%d, true, nil)", localID, found, err, id[0])
	}
	if _, found, _ := lookup.FindBySourceID(ctx, "user-1", "missing"); found {
		t.Error("FindBySourceID found a nonexistent source ID")
	}
}

// TestWrapStore_UnsupportedOptionalInterfaces verifies that wrapping a store
// which does NOT implement the optional interfaces yields ErrNotSupported
// rather than panicking or silently succeeding.
func TestWrapStore_UnsupportedOptionalInterfaces(t *testing.T) {
	ctx := context.Background()
	wrapped, err := WrapStore[int64, json.RawMessage](newFakeStore("orders"))
	if err != nil {
		t.Fatalf("WrapStore: %v", err)
	}

	if _, _, err := wrapped.GetCursor(ctx, "x"); !errors.Is(err, ledger.ErrNotSupported) {
		t.Errorf("GetCursor err = %v, want ErrNotSupported", err)
	}
	if err := wrapped.SetCursor(ctx, "x", "1"); !errors.Is(err, ledger.ErrNotSupported) {
		t.Errorf("SetCursor err = %v, want ErrNotSupported", err)
	}
	if _, _, err := wrapped.FindBySourceID(ctx, "s", "src"); !errors.Is(err, ledger.ErrNotSupported) {
		t.Errorf("FindBySourceID err = %v, want ErrNotSupported", err)
	}
}

// TestWrapStore_ForwardsWithTracing exercises the traced branch of the
// forwarding methods to ensure the span paths also delegate correctly.
func TestWrapStore_ForwardsWithTracing(t *testing.T) {
	ctx := context.Background()
	inner := newCursorFakeStore("orders")
	wrapped, err := WrapStore[int64, json.RawMessage](inner, WithTracesEnabled(true))
	if err != nil {
		t.Fatalf("WrapStore: %v", err)
	}
	if err := wrapped.SetCursor(ctx, "n", "5"); err != nil {
		t.Fatalf("SetCursor (traced): %v", err)
	}
	if cur, found, err := wrapped.GetCursor(ctx, "n"); err != nil || !found || cur != "5" {
		t.Errorf("GetCursor (traced) = (%q, %v, %v), want (5, true, nil)", cur, found, err)
	}
}
