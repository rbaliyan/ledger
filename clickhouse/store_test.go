package clickhouse

import (
	"context"
	"strings"
	"testing"
)

func TestGenerateID_Format(t *testing.T) {
	id := generateID()
	if len(id) != 32 {
		t.Fatalf("expected 32-char ID, got %d chars: %q", len(id), id)
	}
	for _, c := range id {
		if !strings.ContainsRune("0123456789abcdef", c) {
			t.Fatalf("ID contains non-hex char %q: %s", c, id)
		}
	}
}

func TestGenerateID_TimestampMonotonic(t *testing.T) {
	// The first 16 chars encode the nanosecond timestamp; timestamps must be
	// non-decreasing across sequential calls (same-nanosecond ties are allowed).
	const n = 100
	prev := generateID()[:16]
	for range n {
		cur := generateID()
		ts := cur[:16]
		if ts < prev {
			t.Fatalf("timestamp regressed: %s < %s", ts, prev)
		}
		prev = ts
	}
}

func TestGenerateID_Unique(t *testing.T) {
	const n = 10000
	seen := make(map[string]struct{}, n)
	for range n {
		id := generateID()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate ID generated: %s", id)
		}
		seen[id] = struct{}{}
	}
}

func TestWithTable_Option(t *testing.T) {
	opt := WithTable("my_table")
	o := options{table: "ledger_entries"}
	opt(&o)
	if o.table != "my_table" {
		t.Fatalf("expected table=my_table, got %q", o.table)
	}
}

func TestClosedStore(t *testing.T) {
	ctx := context.Background()
	s := &Store{}
	s.closed.Store(true)

	if _, err := s.Append(ctx, "stream"); err == nil {
		t.Error("Append on closed store should return error")
	}
	if _, err := s.Read(ctx, "stream"); err == nil {
		t.Error("Read on closed store should return error")
	}
	if _, err := s.Count(ctx, "stream"); err == nil {
		t.Error("Count on closed store should return error")
	}
	if _, err := s.Trim(ctx, "stream", "id"); err == nil {
		t.Error("Trim on closed store should return error")
	}
	if _, err := s.ListStreamIDs(ctx); err == nil {
		t.Error("ListStreamIDs on closed store should return error")
	}
}

func TestSetTagsAndAnnotations_NotSupported(t *testing.T) {
	ctx := context.Background()
	s := &Store{}
	if err := s.SetTags(ctx, "stream", "id", nil); err == nil {
		t.Error("SetTags should return ErrNotSupported")
	}
	if err := s.SetAnnotations(ctx, "stream", "id", nil); err == nil {
		t.Error("SetAnnotations should return ErrNotSupported")
	}
}
