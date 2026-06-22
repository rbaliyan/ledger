// Package storetest provides a backend-agnostic conformance test suite
// for ledger.Store implementations.
package storetest

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/rbaliyan/ledger"
)

// TestConfig provides backend-specific test data for the conformance suite.
type TestConfig[P any] struct {
	// SamplePayload is a valid payload value for round-trip testing.
	SamplePayload P
	// PayloadEqual reports whether two payloads are equal.
	// Defaults to reflect.DeepEqual if nil.
	PayloadEqual func(a, b P) bool
	// MakeSearchable, if non-nil, builds a payload that embeds the given token
	// in a way the backend's Search can match (a substring for SQL backends, a
	// whole word for MongoDB's $text tokeniser). When nil, the Search subtest is
	// skipped. Only set this for backends that implement [ledger.Searcher].
	MakeSearchable func(token string) P
	// NoDedupKey indicates the backend does not deduplicate on DedupKey at append
	// time (e.g. ClickHouse, whose idempotency model collapses on a stable
	// SourceID via ReplacingMergeTree, not on DedupKey). When true, the
	// DedupKey-based subtests are skipped because they assert semantics the
	// backend does not implement.
	NoDedupKey bool
	// ForceClose, when non-nil, closes the store under test so the
	// ForceCloseErrors subtest can assert real post-close failure modes. It is
	// invoked as the final subtest, after every other subtest has run, so
	// closing the shared store does not disturb earlier subtests. Implement it as
	// a call to the store's Close. Backends that cannot close in-process (or do
	// not surface [ledger.ErrStoreClosed]) leave it nil, and the subtest skips.
	ForceClose func()
}

// RunStoreTests runs the standard conformance suite against any Store implementation.
// afterFn converts an ID to a ReadOption that sets the cursor.
func RunStoreTests[I comparable, P any](t *testing.T, store ledger.Store[I, P], afterFn func(I) ledger.ReadOption, cfg TestConfig[P]) {
	t.Helper()
	ctx := context.Background()

	payloadEqual := cfg.PayloadEqual
	if payloadEqual == nil {
		payloadEqual = func(a, b P) bool { return reflect.DeepEqual(a, b) }
	}

	t.Run("AppendAndRead", func(t *testing.T) {
		ids, err := store.Append(ctx, "test-append", ledger.RawEntry[P]{
			Payload:       cfg.SamplePayload,
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
		if !payloadEqual(e.Payload, cfg.SamplePayload) {
			t.Errorf("Payload mismatch: got %v, want %v", e.Payload, cfg.SamplePayload)
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
		if cfg.NoDedupKey {
			t.Skip("backend does not deduplicate on DedupKey")
		}
		entry := ledger.RawEntry[P]{
			Payload:       cfg.SamplePayload,
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
		entry := ledger.RawEntry[P]{
			Payload:       cfg.SamplePayload,
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
		if cfg.NoDedupKey {
			t.Skip("backend does not deduplicate on DedupKey")
		}
		entry := ledger.RawEntry[P]{
			Payload:       cfg.SamplePayload,
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
			if _, err := store.Append(ctx, "test-cursor", ledger.RawEntry[P]{
				Payload:       cfg.SamplePayload,
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
			if _, err := store.Append(ctx, "test-desc", ledger.RawEntry[P]{
				Payload:       cfg.SamplePayload,
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
			if _, err := store.Append(ctx, "test-order", ledger.RawEntry[P]{Payload: cfg.SamplePayload, OrderKey: key, SchemaVersion: 1}); err != nil {
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
		if _, err := store.Append(ctx, "test-schema", ledger.RawEntry[P]{
			Payload:       cfg.SamplePayload,
			SchemaVersion: 3,
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
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
		if _, err := store.Append(ctx, "test-iso-a", ledger.RawEntry[P]{Payload: cfg.SamplePayload, SchemaVersion: 1}); err != nil {
			t.Fatalf("append a: %v", err)
		}
		if _, err := store.Append(ctx, "test-iso-b", ledger.RawEntry[P]{Payload: cfg.SamplePayload, SchemaVersion: 1}); err != nil {
			t.Fatalf("append b: %v", err)
		}

		a, _ := store.Read(ctx, "test-iso-a")
		b, _ := store.Read(ctx, "test-iso-b")
		if len(a) != 1 || len(b) != 1 {
			t.Errorf("streams not isolated: a=%d b=%d", len(a), len(b))
		}
	})

	t.Run("Count", func(t *testing.T) {
		for range 3 {
			store.Append(ctx, "test-count", ledger.RawEntry[P]{Payload: cfg.SamplePayload, SchemaVersion: 1})
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
			ids, _ := store.Append(ctx, "test-trim", ledger.RawEntry[P]{Payload: cfg.SamplePayload, SchemaVersion: 1})
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

	t.Run("SetTags", func(t *testing.T) {
		ids, err := store.Append(ctx, "test-tags", ledger.RawEntry[P]{
			Payload: cfg.SamplePayload, SchemaVersion: 1, Tags: []string{"initial"},
		})
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		id := ids[0]

		// Verify initial tag
		entries, _ := store.Read(ctx, "test-tags")
		if len(entries) != 1 || len(entries[0].Tags) != 1 || entries[0].Tags[0] != "initial" {
			t.Fatalf("initial tags = %v", entries[0].Tags)
		}

		// Update tags
		if err := store.SetTags(ctx, "test-tags", id, []string{"processed", "reviewed"}); err != nil {
			if errors.Is(err, ledger.ErrNotSupported) {
				t.Skip("backend does not support SetTags")
			}
			t.Fatalf("SetTags: %v", err)
		}

		entries, _ = store.Read(ctx, "test-tags")
		if len(entries[0].Tags) != 2 {
			t.Errorf("tags after update = %v, want [processed reviewed]", entries[0].Tags)
		}
		if entries[0].UpdatedAt == nil {
			t.Error("UpdatedAt should be set after SetTags")
		}
	})

	t.Run("SetAnnotations", func(t *testing.T) {
		ids, err := store.Append(ctx, "test-annot", ledger.RawEntry[P]{
			Payload: cfg.SamplePayload, SchemaVersion: 1,
		})
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		id := ids[0]

		// Set annotations
		v1 := "2026-04-08"
		v2 := "bot"
		if err := store.SetAnnotations(ctx, "test-annot", id, map[string]*string{
			"processed_at": &v1,
			"processed_by": &v2,
		}); err != nil {
			if errors.Is(err, ledger.ErrNotSupported) {
				t.Skip("backend does not support SetAnnotations")
			}
			t.Fatalf("SetAnnotations: %v", err)
		}

		entries, _ := store.Read(ctx, "test-annot")
		if entries[0].Annotations["processed_at"] != "2026-04-08" {
			t.Errorf("annotations = %v", entries[0].Annotations)
		}

		// Merge: add new key, delete existing key
		v3 := "success"
		if err := store.SetAnnotations(ctx, "test-annot", id, map[string]*string{
			"status":       &v3,
			"processed_by": nil, // delete
		}); err != nil {
			t.Fatalf("SetAnnotations merge: %v", err)
		}

		entries, _ = store.Read(ctx, "test-annot")
		a := entries[0].Annotations
		if a["status"] != "success" {
			t.Errorf("status = %q, want success", a["status"])
		}
		if _, ok := a["processed_by"]; ok {
			t.Error("processed_by should be deleted")
		}
		if a["processed_at"] != "2026-04-08" {
			t.Error("processed_at should be preserved")
		}
	})

	t.Run("SetTags_NotFound", func(t *testing.T) {
		var zeroID I
		err := store.SetTags(ctx, "test-nonexistent", zeroID, []string{"x"})
		if errors.Is(err, ledger.ErrNotSupported) {
			t.Skip("backend does not support SetTags")
		}
		if !errors.Is(err, ledger.ErrEntryNotFound) {
			t.Errorf("SetTags on missing entry: %v, want ErrEntryNotFound", err)
		}
	})

	t.Run("WithTagFilter", func(t *testing.T) {
		for _, tag := range []string{"a", "b"} {
			if _, err := store.Append(ctx, "test-tag-filter", ledger.RawEntry[P]{
				Payload: cfg.SamplePayload, SchemaVersion: 1, Tags: []string{tag, "common"},
			}); err != nil {
				t.Fatalf("append: %v", err)
			}
		}

		// Filter by single tag
		entries, err := store.Read(ctx, "test-tag-filter", ledger.WithTag("a"))
		if err != nil {
			t.Fatalf("WithTag: %v", err)
		}
		if len(entries) != 1 {
			t.Errorf("WithTag(a): got %d, want 1", len(entries))
		}

		// Filter by common tag
		entries, err = store.Read(ctx, "test-tag-filter", ledger.WithTag("common"))
		if err != nil {
			t.Fatalf("WithTag common: %v", err)
		}
		if len(entries) != 2 {
			t.Errorf("WithTag(common): got %d, want 2", len(entries))
		}

		// Filter by all tags
		entries, err = store.Read(ctx, "test-tag-filter", ledger.WithAllTags("a", "common"))
		if err != nil {
			t.Fatalf("WithAllTags: %v", err)
		}
		if len(entries) != 1 {
			t.Errorf("WithAllTags(a, common): got %d, want 1", len(entries))
		}
	})

	t.Run("WithMetadataKeyFilter", func(t *testing.T) {
		for _, meta := range []map[string]string{
			{"source": "producer-a", "env": "prod"},
			{"source": "producer-b", "env": "prod"},
			{"source": "producer-a", "env": "staging"},
		} {
			if _, err := store.Append(ctx, "test-meta-filter", ledger.RawEntry[P]{
				Payload:  cfg.SamplePayload,
				Metadata: meta,
			}); err != nil {
				t.Fatalf("append: %v", err)
			}
		}

		entries, err := store.Read(ctx, "test-meta-filter", ledger.WithMetadataKey("source", "producer-a"))
		if err != nil {
			t.Fatalf("WithMetadataKey: %v", err)
		}
		if len(entries) != 2 {
			t.Errorf("WithMetadataKey(source=producer-a): got %d, want 2", len(entries))
		}

		entries, err = store.Read(ctx, "test-meta-filter", ledger.WithMetadataKey("env", "prod"))
		if err != nil {
			t.Fatalf("WithMetadataKey env: %v", err)
		}
		if len(entries) != 2 {
			t.Errorf("WithMetadataKey(env=prod): got %d, want 2", len(entries))
		}

		// AND: both conditions, only one entry matches
		entries, err = store.Read(ctx, "test-meta-filter",
			ledger.WithMetadataKey("source", "producer-a"),
			ledger.WithMetadataKey("env", "prod"),
		)
		if err != nil {
			t.Fatalf("WithMetadataKey AND: %v", err)
		}
		if len(entries) != 1 {
			t.Errorf("WithMetadataKey AND: got %d, want 1", len(entries))
		}

		// Non-matching value returns empty
		entries, err = store.Read(ctx, "test-meta-filter", ledger.WithMetadataKey("source", "producer-c"))
		if err != nil {
			t.Fatalf("WithMetadataKey no match: %v", err)
		}
		if len(entries) != 0 {
			t.Errorf("WithMetadataKey(source=producer-c): got %d, want 0", len(entries))
		}
	})

	t.Run("WithAnnotationFilter", func(t *testing.T) {
		ids, err := store.Append(ctx, "test-annot-filter",
			ledger.RawEntry[P]{Payload: cfg.SamplePayload},
			ledger.RawEntry[P]{Payload: cfg.SamplePayload},
		)
		if err != nil {
			t.Fatalf("append: %v", err)
		}

		v1, v2 := "processed", "pending"
		if err := store.SetAnnotations(ctx, "test-annot-filter", ids[0], map[string]*string{"status": &v1}); err != nil {
			if errors.Is(err, ledger.ErrNotSupported) {
				t.Skip("backend does not support annotations")
			}
			t.Fatalf("SetAnnotations: %v", err)
		}
		if err := store.SetAnnotations(ctx, "test-annot-filter", ids[1], map[string]*string{"status": &v2}); err != nil {
			t.Fatalf("SetAnnotations id[1]: %v", err)
		}

		entries, err := store.Read(ctx, "test-annot-filter", ledger.WithAnnotation("status", "processed"))
		if err != nil {
			t.Fatalf("WithAnnotation: %v", err)
		}
		if len(entries) != 1 {
			t.Errorf("WithAnnotation(status=processed): got %d, want 1", len(entries))
		}

		entries, err = store.Read(ctx, "test-annot-filter", ledger.WithAnnotation("status", "pending"))
		if err != nil {
			t.Fatalf("WithAnnotation pending: %v", err)
		}
		if len(entries) != 1 {
			t.Errorf("WithAnnotation(status=pending): got %d, want 1", len(entries))
		}

		// Non-matching value returns empty.
		entries, err = store.Read(ctx, "test-annot-filter", ledger.WithAnnotation("status", "unknown"))
		if err != nil {
			t.Fatalf("WithAnnotation no match: %v", err)
		}
		if len(entries) != 0 {
			t.Errorf("WithAnnotation(status=unknown): got %d, want 0", len(entries))
		}
	})

	// Stream-name prefix chosen so these streams sort after all other test streams
	// in this suite, allowing ListStreamIDs tests to isolate their results via cursor.
	const listPrefix = "zzz-list-"

	t.Run("ListStreamIDs", func(t *testing.T) {
		names := []string{listPrefix + "alpha", listPrefix + "beta", listPrefix + "gamma"}
		for _, name := range names {
			if _, err := store.Append(ctx, name, ledger.RawEntry[P]{Payload: cfg.SamplePayload, SchemaVersion: 1}); err != nil {
				t.Fatalf("append %s: %v", name, err)
			}
		}

		ids, err := store.ListStreamIDs(ctx, ledger.ListAfter(listPrefix))
		if err != nil {
			t.Fatalf("ListStreamIDs: %v", err)
		}

		got := make(map[string]bool, len(ids))
		for _, id := range ids {
			got[id] = true
		}
		for _, want := range names {
			if !got[want] {
				t.Errorf("missing %q in %v", want, ids)
			}
		}

		// Verify ascending order.
		for i := 1; i < len(ids); i++ {
			if ids[i-1] >= ids[i] {
				t.Errorf("not ascending at %d: %v", i, ids)
			}
		}
	})

	t.Run("ListStreamIDs_Pagination", func(t *testing.T) {
		const pagePrefix = "zzz-page-"
		names := []string{pagePrefix + "a", pagePrefix + "b", pagePrefix + "c", pagePrefix + "d", pagePrefix + "e"}
		for _, name := range names {
			if _, err := store.Append(ctx, name, ledger.RawEntry[P]{Payload: cfg.SamplePayload, SchemaVersion: 1}); err != nil {
				t.Fatalf("append %s: %v", name, err)
			}
		}

		page1, err := store.ListStreamIDs(ctx, ledger.ListAfter(pagePrefix), ledger.ListLimit(2))
		if err != nil {
			t.Fatalf("page1: %v", err)
		}
		if len(page1) != 2 || page1[0] != pagePrefix+"a" || page1[1] != pagePrefix+"b" {
			t.Fatalf("page1 = %v, want [%sa %sb]", page1, pagePrefix, pagePrefix)
		}

		page2, err := store.ListStreamIDs(ctx, ledger.ListAfter(page1[1]), ledger.ListLimit(2))
		if err != nil {
			t.Fatalf("page2: %v", err)
		}
		if len(page2) != 2 || page2[0] != pagePrefix+"c" || page2[1] != pagePrefix+"d" {
			t.Fatalf("page2 = %v", page2)
		}

		page3, err := store.ListStreamIDs(ctx, ledger.ListAfter(page2[1]), ledger.ListLimit(2))
		if err != nil {
			t.Fatalf("page3: %v", err)
		}
		if len(page3) < 1 || page3[0] != pagePrefix+"e" {
			t.Fatalf("page3 = %v, want first element %se", page3, pagePrefix)
		}
	})

	t.Run("ListStreamIDs_TrimmedExcluded", func(t *testing.T) {
		const name = "zzz-trimmed-out"
		ids, err := store.Append(ctx, name, ledger.RawEntry[P]{Payload: cfg.SamplePayload, SchemaVersion: 1})
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		if len(ids) != 1 {
			t.Fatalf("append ids = %v", ids)
		}

		// Present before trim.
		listed, _ := store.ListStreamIDs(ctx, ledger.ListAfter("zzz-trimmed"))
		if !slices.Contains(listed, name) {
			t.Fatalf("stream %q not listed before trim: %v", name, listed)
		}

		if _, err := store.Trim(ctx, name, ids[0]); err != nil {
			t.Fatalf("Trim: %v", err)
		}

		// Absent after trim-to-empty.
		listed, _ = store.ListStreamIDs(ctx, ledger.ListAfter("zzz-trimmed"))
		if slices.Contains(listed, name) {
			t.Errorf("stream %q still listed after full trim: %v", name, listed)
		}
	})

	t.Run("ListStreamIDs_EmptyAfterCursor", func(t *testing.T) {
		// Cursor beyond any plausible stream name — should return empty.
		ids, err := store.ListStreamIDs(ctx, ledger.ListAfter("zzzzzzzzzzzzzzz"))
		if err != nil {
			t.Fatalf("ListStreamIDs: %v", err)
		}
		if ids != nil {
			t.Errorf("want nil past end-of-range, got %v", ids)
		}
	})

	t.Run("Stat", func(t *testing.T) {
		const stream = "test-stat"
		const n = 4
		var ids []I
		for range n {
			got, err := store.Append(ctx, stream, ledger.RawEntry[P]{Payload: cfg.SamplePayload, SchemaVersion: 1})
			if err != nil {
				t.Fatalf("append: %v", err)
			}
			ids = append(ids, got...)
		}
		if len(ids) != n {
			t.Fatalf("appended %d ids, want %d", len(ids), n)
		}

		stat, err := store.Stat(ctx, stream)
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		if stat.Stream != stream {
			t.Errorf("Stat.Stream = %q, want %q", stat.Stream, stream)
		}
		if stat.Count != n {
			t.Errorf("Stat.Count = %d, want %d", stat.Count, n)
		}
		if stat.FirstID != ids[0] {
			t.Errorf("Stat.FirstID = %v, want %v", stat.FirstID, ids[0])
		}
		if stat.LastID != ids[n-1] {
			t.Errorf("Stat.LastID = %v, want %v", stat.LastID, ids[n-1])
		}
	})

	t.Run("Search", func(t *testing.T) {
		searcher, ok := store.(ledger.Searcher[I, P])
		if !ok {
			t.Skip("backend does not implement Searcher")
		}
		if cfg.MakeSearchable == nil {
			t.Skip("TestConfig.MakeSearchable not provided")
		}

		// Backends with a managed search index (e.g. MongoDB $text) need the
		// index created before Search can run. Backends that search without a
		// managed index (e.g. PostgreSQL in default ILIKE mode) return
		// ErrNotSupported here, which is not a failure — Search works directly.
		if idx, ok := store.(ledger.SearchIndexer); ok {
			if err := idx.EnsureSearchIndex(ctx); err != nil && !errors.Is(err, ledger.ErrNotSupported) {
				t.Fatalf("EnsureSearchIndex: %v", err)
			}
		}

		const stream = "test-search"
		entries := []ledger.RawEntry[P]{
			{Payload: cfg.MakeSearchable("needle"), SchemaVersion: 1},
			{Payload: cfg.MakeSearchable("haystack"), SchemaVersion: 1},
			{Payload: cfg.MakeSearchable("haystack"), SchemaVersion: 1},
		}
		if _, err := store.Append(ctx, stream, entries...); err != nil {
			t.Fatalf("append: %v", err)
		}

		results, err := searcher.Search(ctx, stream, "needle")
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		if len(results) != 1 {
			t.Errorf("Search(needle): got %d, want 1", len(results))
		}

		results, err = searcher.Search(ctx, stream, "haystack")
		if err != nil {
			t.Fatalf("Search haystack: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("Search(haystack): got %d, want 2", len(results))
		}

		results, err = searcher.Search(ctx, stream, "nonexistent-xyz")
		if err != nil {
			t.Fatalf("Search no-match: %v", err)
		}
		if len(results) != 0 {
			t.Errorf("Search(nonexistent-xyz): got %d, want 0", len(results))
		}
	})

	t.Run("Cursor", func(t *testing.T) {
		cs, ok := store.(ledger.CursorStore)
		if !ok {
			t.Skip("backend does not implement CursorStore")
		}
		const name = "test-cursor-rt"

		// Unset cursor reports not-found.
		if _, found, err := cs.GetCursor(ctx, name); err != nil {
			t.Fatalf("GetCursor (unset): %v", err)
		} else if found {
			t.Error("GetCursor (unset): found = true, want false")
		}

		// Round-trip a value.
		if err := cs.SetCursor(ctx, name, "100"); err != nil {
			t.Fatalf("SetCursor: %v", err)
		}
		got, found, err := cs.GetCursor(ctx, name)
		if err != nil {
			t.Fatalf("GetCursor: %v", err)
		}
		if !found || got != "100" {
			t.Errorf("GetCursor = (%q, %v), want (%q, true)", got, found, "100")
		}

		// Advance to a higher value.
		if err := cs.SetCursor(ctx, name, "200"); err != nil {
			t.Fatalf("SetCursor advance: %v", err)
		}
		got, _, err = cs.GetCursor(ctx, name)
		if err != nil {
			t.Fatalf("GetCursor after advance: %v", err)
		}
		if got != "200" {
			t.Errorf("GetCursor after advance = %q, want 200", got)
		}
	})

	t.Run("FindBySourceID", func(t *testing.T) {
		lookup, ok := store.(interface {
			FindBySourceID(ctx context.Context, stream, sourceID string) (I, bool, error)
		})
		if !ok {
			t.Skip("backend does not implement FindBySourceID")
		}
		const stream = "test-find-source"
		const sourceID = "src-find-1"

		ids, err := store.Append(ctx, stream, ledger.RawEntry[P]{
			Payload:       cfg.SamplePayload,
			SchemaVersion: 1,
			SourceID:      sourceID,
		})
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		if len(ids) != 1 {
			t.Fatalf("append returned %d ids, want 1", len(ids))
		}

		gotID, found, err := lookup.FindBySourceID(ctx, stream, sourceID)
		if err != nil {
			t.Fatalf("FindBySourceID: %v", err)
		}
		if !found {
			t.Fatalf("FindBySourceID(%q): found = false, want true", sourceID)
		}
		if gotID != ids[0] {
			t.Errorf("FindBySourceID id = %v, want %v", gotID, ids[0])
		}

		// Not-found case.
		_, found, err = lookup.FindBySourceID(ctx, stream, "src-does-not-exist")
		if err != nil {
			t.Fatalf("FindBySourceID (missing): %v", err)
		}
		if found {
			t.Error("FindBySourceID (missing): found = true, want false")
		}
	})

	t.Run("ConcurrentAppendDedup", func(t *testing.T) {
		if cfg.NoDedupKey {
			t.Skip("backend does not deduplicate on DedupKey")
		}
		const stream = "test-concurrent-dedup"
		const goroutines = 16
		const dedupKey = "shared-dedup-key"

		var wg sync.WaitGroup
		for range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				entry := ledger.RawEntry[P]{
					Payload:       cfg.SamplePayload,
					DedupKey:      dedupKey,
					SchemaVersion: 1,
				}
				// In-memory SQLite serialises on a single connection but may still
				// surface a transient busy error under contention; retry a bounded
				// number of times so the dedup correctness assertion is what is tested.
				for attempt := 0; attempt < 50; attempt++ {
					_, err := store.Append(ctx, stream, entry)
					if err == nil {
						return
					}
					if errors.Is(err, ledger.ErrStoreClosed) {
						return
					}
				}
			}()
		}
		wg.Wait()

		n, err := store.Count(ctx, stream)
		if err != nil {
			t.Fatalf("Count: %v", err)
		}
		if n != 1 {
			t.Errorf("after concurrent dedup append: Count = %d, want 1 (dedup must hold under contention)", n)
		}
	})

	// ForceCloseErrors MUST be the last subtest: it closes the shared store and
	// asserts that subsequent operations fail with ErrStoreClosed. Earlier
	// subtests have already completed by the time this runs.
	t.Run("ForceCloseErrors", func(t *testing.T) {
		if cfg.ForceClose == nil {
			t.Skip("TestConfig.ForceClose not provided")
		}
		cfg.ForceClose()

		_, err := store.Append(ctx, "after-close", ledger.RawEntry[P]{
			Payload:       cfg.SamplePayload,
			SchemaVersion: 1,
		})
		if !errors.Is(err, ledger.ErrStoreClosed) {
			t.Errorf("Append after close = %v, want ErrStoreClosed", err)
		}

		if _, err := store.Read(ctx, "after-close"); !errors.Is(err, ledger.ErrStoreClosed) {
			t.Errorf("Read after close = %v, want ErrStoreClosed", err)
		}
	})
}
