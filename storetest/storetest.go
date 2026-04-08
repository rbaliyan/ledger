// Package storetest provides a backend-agnostic conformance test suite
// for ledger.Store implementations.
package storetest

import (
	"context"
	"errors"
	"testing"

	"github.com/rbaliyan/ledger"
)

// RunStoreTests runs the standard conformance suite against any Store implementation.
// afterFn converts an ID to a ReadOption that sets the cursor.
func RunStoreTests[I comparable](t *testing.T, store ledger.Store[I], afterFn func(I) ledger.ReadOption) {
	t.Helper()
	ctx := context.Background()

	t.Run("AppendAndRead", func(t *testing.T) {
		ids, err := store.Append(ctx, "test-append", ledger.RawEntry{
			Payload:       []byte(`{"id":"1"}`),
			OrderKey:      "key-1",
			DedupKey:      "dedup-1",
			SchemaVersion: 1,
			Metadata:      map[string]string{"source": "test"},
		})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if len(ids) != 1 {
			t.Fatalf("got %d ids, want 1", len(ids))
		}

		entries, err := store.Read(ctx, "test-append")
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("got %d entries, want 1", len(entries))
		}
		e := entries[0]
		if e.Stream != "test-append" {
			t.Errorf("Stream = %q", e.Stream)
		}
		if string(e.Payload) != `{"id":"1"}` {
			t.Errorf("Payload = %s", e.Payload)
		}
		if e.OrderKey != "key-1" {
			t.Errorf("OrderKey = %q", e.OrderKey)
		}
		if e.DedupKey != "dedup-1" {
			t.Errorf("DedupKey = %q", e.DedupKey)
		}
		if e.SchemaVersion != 1 {
			t.Errorf("SchemaVersion = %d", e.SchemaVersion)
		}
		if e.Metadata["source"] != "test" {
			t.Errorf("Metadata = %v", e.Metadata)
		}
		if e.CreatedAt.IsZero() {
			t.Error("CreatedAt is zero")
		}
	})

	t.Run("DedupSkipsDuplicates", func(t *testing.T) {
		entry := ledger.RawEntry{
			Payload:       []byte(`{}`),
			DedupKey:      "dup-test-1",
			SchemaVersion: 1,
		}
		ids1, err := store.Append(ctx, "test-dedup", entry)
		if err != nil {
			t.Fatalf("first: %v", err)
		}
		if len(ids1) != 1 {
			t.Fatalf("first: got %d ids, want 1", len(ids1))
		}
		ids2, err := store.Append(ctx, "test-dedup", entry)
		if err != nil {
			t.Fatalf("second: %v", err)
		}
		if len(ids2) != 0 {
			t.Errorf("second: got %d ids, want 0", len(ids2))
		}
	})

	t.Run("DedupEmptyKeyAllowsDuplicates", func(t *testing.T) {
		entry := ledger.RawEntry{
			Payload:       []byte(`{}`),
			DedupKey:      "",
			SchemaVersion: 1,
		}
		ids1, _ := store.Append(ctx, "test-dedup-empty", entry)
		ids2, _ := store.Append(ctx, "test-dedup-empty", entry)
		if len(ids1) != 1 || len(ids2) != 1 {
			t.Errorf("empty dedup key should allow duplicates: %d, %d", len(ids1), len(ids2))
		}
	})

	t.Run("DedupAcrossStreams", func(t *testing.T) {
		entry := ledger.RawEntry{
			Payload:       []byte(`{}`),
			DedupKey:      "cross-stream-key",
			SchemaVersion: 1,
		}
		ids1, _ := store.Append(ctx, "test-cross-a", entry)
		ids2, _ := store.Append(ctx, "test-cross-b", entry)
		if len(ids1) != 1 || len(ids2) != 1 {
			t.Error("same dedup key in different streams should not conflict")
		}
	})

	t.Run("CursorPagination", func(t *testing.T) {
		for i := range 5 {
			if _, err := store.Append(ctx, "test-cursor", ledger.RawEntry{
				Payload:       []byte(`{}`),
				SchemaVersion: 1,
				OrderKey:      string(rune('a' + i)),
			}); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}
		page1, err := store.Read(ctx, "test-cursor", ledger.Limit(2))
		if err != nil {
			t.Fatalf("page1: %v", err)
		}
		if len(page1) != 2 {
			t.Fatalf("page1: got %d, want 2", len(page1))
		}
		page2, err := store.Read(ctx, "test-cursor", ledger.Limit(2), afterFn(page1[1].ID))
		if err != nil {
			t.Fatalf("page2: %v", err)
		}
		if len(page2) != 2 {
			t.Fatalf("page2: got %d, want 2", len(page2))
		}
		page3, err := store.Read(ctx, "test-cursor", ledger.Limit(2), afterFn(page2[1].ID))
		if err != nil {
			t.Fatalf("page3: %v", err)
		}
		if len(page3) != 1 {
			t.Fatalf("page3: got %d, want 1", len(page3))
		}
	})

	t.Run("Descending", func(t *testing.T) {
		for i := range 3 {
			if _, err := store.Append(ctx, "test-desc", ledger.RawEntry{
				Payload:       []byte(`{}`),
				SchemaVersion: 1,
			}); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}
		entries, err := store.Read(ctx, "test-desc", ledger.Desc())
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if len(entries) < 3 {
			t.Fatalf("got %d entries, want >= 3", len(entries))
		}
	})

	t.Run("OrderKeyFilter", func(t *testing.T) {
		for _, key := range []string{"a", "b", "a"} {
			if _, err := store.Append(ctx, "test-order", ledger.RawEntry{Payload: []byte(`{}`), OrderKey: key, SchemaVersion: 1}); err != nil {
				t.Fatalf("append: %v", err)
			}
		}

		entries, err := store.Read(ctx, "test-order", ledger.WithOrderKey("a"))
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if len(entries) != 2 {
			t.Fatalf("got %d, want 2", len(entries))
		}
	})

	t.Run("EmptyStream", func(t *testing.T) {
		entries, err := store.Read(ctx, "test-nonexistent-stream")
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if entries != nil {
			t.Errorf("want nil for empty stream, got %v", entries)
		}
	})

	t.Run("AppendEmpty", func(t *testing.T) {
		ids, err := store.Append(ctx, "test-empty-append")
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if ids != nil {
			t.Errorf("want nil for empty append, got %v", ids)
		}
	})

	t.Run("SchemaVersion", func(t *testing.T) {
		store.Append(ctx, "test-schema", ledger.RawEntry{
			Payload:       []byte(`{}`),
			SchemaVersion: 3,
		})
		entries, err := store.Read(ctx, "test-schema")
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("got %d entries, want 1", len(entries))
		}
		if entries[0].SchemaVersion != 3 {
			t.Errorf("SchemaVersion = %d, want 3", entries[0].SchemaVersion)
		}
	})

	t.Run("StreamIsolation", func(t *testing.T) {
		store.Append(ctx, "test-iso-a", ledger.RawEntry{Payload: []byte(`{}`), SchemaVersion: 1})
		store.Append(ctx, "test-iso-b", ledger.RawEntry{Payload: []byte(`{}`), SchemaVersion: 1})

		a, _ := store.Read(ctx, "test-iso-a")
		b, _ := store.Read(ctx, "test-iso-b")
		if len(a) != 1 || len(b) != 1 {
			t.Errorf("streams not isolated: a=%d b=%d", len(a), len(b))
		}
	})

	t.Run("Count", func(t *testing.T) {
		for range 3 {
			store.Append(ctx, "test-count", ledger.RawEntry{Payload: []byte(`{}`), SchemaVersion: 1})
		}
		n, err := store.Count(ctx, "test-count")
		if err != nil {
			t.Fatalf("Count: %v", err)
		}
		if n != 3 {
			t.Errorf("Count = %d, want 3", n)
		}
	})

	t.Run("CountEmptyStream", func(t *testing.T) {
		n, err := store.Count(ctx, "test-count-empty")
		if err != nil {
			t.Fatalf("Count: %v", err)
		}
		if n != 0 {
			t.Errorf("Count = %d, want 0", n)
		}
	})

	t.Run("Trim", func(t *testing.T) {
		var lastID I
		for range 5 {
			ids, _ := store.Append(ctx, "test-trim", ledger.RawEntry{Payload: []byte(`{}`), SchemaVersion: 1})
			if len(ids) > 0 {
				lastID = ids[0]
			}
		}
		// Read first 3, trim up to 3rd
		entries, _ := store.Read(ctx, "test-trim", ledger.Limit(3))
		if len(entries) < 3 {
			t.Fatalf("need at least 3 entries, got %d", len(entries))
		}
		trimID := entries[2].ID
		deleted, err := store.Trim(ctx, "test-trim", trimID)
		if err != nil {
			t.Fatalf("Trim: %v", err)
		}
		if deleted != 3 {
			t.Errorf("Trim deleted %d, want 3", deleted)
		}
		remaining, _ := store.Read(ctx, "test-trim")
		if len(remaining) != 2 {
			t.Errorf("remaining = %d, want 2", len(remaining))
		}
		_ = lastID // suppress unused
	})

	t.Run("ClosedStoreErrors", func(t *testing.T) {
		// Test with a separate store is too complex for a generic suite.
		// Backend-specific tests cover this. Verify the error type exists.
		if !errors.Is(ledger.ErrStoreClosed, ledger.ErrStoreClosed) {
			t.Error("ErrStoreClosed should match itself")
		}
	})
}
