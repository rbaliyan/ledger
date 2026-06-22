package ledger

import (
	"context"
	"encoding/json"
	"testing"
)

// unitXFakeStore is a minimal in-memory Store[int64, json.RawMessage] used to
// exercise Stream.Read / SetTags / SetAnnotations without a real backend.
// The unitX prefix avoids name collisions with helpers in other test files.
type unitXFakeStore struct {
	entries     map[string][]StoredEntry[int64, json.RawMessage]
	nextID      int64
	lastTags    []string
	lastAnnots  map[string]*string
	tagsCalled  bool
	annotCalled bool
}

func newUnitXFakeStore() *unitXFakeStore {
	return &unitXFakeStore{entries: make(map[string][]StoredEntry[int64, json.RawMessage])}
}

func (s *unitXFakeStore) Append(_ context.Context, stream string, entries ...RawEntry[json.RawMessage]) ([]int64, error) {
	ids := make([]int64, len(entries))
	for i, e := range entries {
		s.nextID++
		ids[i] = s.nextID
		s.entries[stream] = append(s.entries[stream], StoredEntry[int64, json.RawMessage]{
			ID:            s.nextID,
			Stream:        stream,
			Payload:       e.Payload,
			OrderKey:      e.OrderKey,
			DedupKey:      e.DedupKey,
			SchemaVersion: e.SchemaVersion,
			Metadata:      e.Metadata,
			Tags:          e.Tags,
		})
	}
	return ids, nil
}

func (s *unitXFakeStore) Read(_ context.Context, stream string, _ ...ReadOption) ([]StoredEntry[int64, json.RawMessage], error) {
	return s.entries[stream], nil
}

func (s *unitXFakeStore) Count(_ context.Context, stream string) (int64, error) {
	return int64(len(s.entries[stream])), nil
}

func (s *unitXFakeStore) Stat(_ context.Context, stream string) (StreamStat[int64], error) {
	es := s.entries[stream]
	st := StreamStat[int64]{Stream: stream, Count: int64(len(es))}
	if len(es) > 0 {
		st.FirstID = es[0].ID
		st.LastID = es[len(es)-1].ID
	}
	return st, nil
}

func (s *unitXFakeStore) SetTags(_ context.Context, _ string, _ int64, tags []string) error {
	s.tagsCalled = true
	s.lastTags = tags
	return nil
}

func (s *unitXFakeStore) SetAnnotations(_ context.Context, _ string, _ int64, annotations map[string]*string) error {
	s.annotCalled = true
	s.lastAnnots = annotations
	return nil
}

func (s *unitXFakeStore) Trim(_ context.Context, _ string, _ int64) (int64, error) { return 0, nil }

func (s *unitXFakeStore) ListStreamIDs(_ context.Context, _ ...ListOption) ([]string, error) {
	return nil, nil
}

func (s *unitXFakeStore) Close(_ context.Context) error { return nil }

var _ Store[int64, json.RawMessage] = (*unitXFakeStore)(nil)

type unitXDoc struct {
	V int `json:"v"`
}

func TestStream_Read_HappyPath(t *testing.T) {
	store := newUnitXFakeStore()
	s, err := NewStream[int64, json.RawMessage, unitXDoc](store, "stream-1", JSONCodec[unitXDoc]{})
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}

	ctx := context.Background()
	ids, err := s.Append(ctx, AppendInput[unitXDoc]{Payload: unitXDoc{V: 1}}, AppendInput[unitXDoc]{Payload: unitXDoc{V: 2}})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("Append returned %d ids, want 2", len(ids))
	}

	entries, err := s.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("Read returned %d entries, want 2", len(entries))
	}
	if entries[0].Payload.V != 1 || entries[1].Payload.V != 2 {
		t.Errorf("decoded payloads = %d,%d, want 1,2", entries[0].Payload.V, entries[1].Payload.V)
	}
	if entries[0].Stream != "stream-1" {
		t.Errorf("Stream = %q, want stream-1", entries[0].Stream)
	}
}

func TestStream_SetTags(t *testing.T) {
	store := newUnitXFakeStore()
	s, err := NewStream[int64, json.RawMessage, unitXDoc](store, "s", JSONCodec[unitXDoc]{})
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	tags := []string{"a", "b"}
	if err := s.SetTags(context.Background(), 1, tags); err != nil {
		t.Fatalf("SetTags: %v", err)
	}
	if !store.tagsCalled {
		t.Fatal("SetTags did not reach the store")
	}
	if len(store.lastTags) != 2 || store.lastTags[0] != "a" || store.lastTags[1] != "b" {
		t.Errorf("forwarded tags = %v, want [a b]", store.lastTags)
	}
}

func TestStream_SetAnnotations(t *testing.T) {
	store := newUnitXFakeStore()
	s, err := NewStream[int64, json.RawMessage, unitXDoc](store, "s", JSONCodec[unitXDoc]{})
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	v := "value"
	annots := map[string]*string{"k": &v, "del": nil}
	if err := s.SetAnnotations(context.Background(), 1, annots); err != nil {
		t.Fatalf("SetAnnotations: %v", err)
	}
	if !store.annotCalled {
		t.Fatal("SetAnnotations did not reach the store")
	}
	if got := store.lastAnnots["k"]; got == nil || *got != "value" {
		t.Errorf("forwarded annotation k = %v, want value", got)
	}
	if got, ok := store.lastAnnots["del"]; !ok || got != nil {
		t.Errorf("forwarded annotation del = %v, want nil (delete)", got)
	}
}

func TestApplyListOptions(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		o := ApplyListOptions()
		if o.Limit() != 100 {
			t.Errorf("default Limit = %d, want 100", o.Limit())
		}
		if o.HasAfter() {
			t.Error("default HasAfter = true, want false")
		}
		if o.After() != "" {
			t.Errorf("default After = %q, want empty", o.After())
		}
	})

	t.Run("ListAfter_and_ListLimit", func(t *testing.T) {
		o := ApplyListOptions(ListAfter("cursor-42"), ListLimit(25))
		if o.After() != "cursor-42" {
			t.Errorf("After = %q, want cursor-42", o.After())
		}
		if !o.HasAfter() {
			t.Error("HasAfter = false, want true")
		}
		if o.Limit() != 25 {
			t.Errorf("Limit = %d, want 25", o.Limit())
		}
	})

	t.Run("ListLimit_ignores_non_positive", func(t *testing.T) {
		o := ApplyListOptions(ListLimit(0), ListLimit(-5))
		if o.Limit() != 100 {
			t.Errorf("Limit = %d, want 100 (non-positive ignored)", o.Limit())
		}
	})
}
