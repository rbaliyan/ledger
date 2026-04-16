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
