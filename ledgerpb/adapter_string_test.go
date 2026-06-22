package ledgerpb_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/rbaliyan/ledger"
	"github.com/rbaliyan/ledger/ledgerpb"
)

// stringStore is a minimal in-memory ledger.Store[string, json.RawMessage] used
// to exercise NewStringProvider without a MongoDB/ClickHouse backend.
type stringStore struct {
	entries map[string][]ledger.StoredEntry[string, json.RawMessage]
	seq     int
}

func newStringStore() *stringStore {
	return &stringStore{entries: make(map[string][]ledger.StoredEntry[string, json.RawMessage])}
}

func (s *stringStore) Append(_ context.Context, stream string, entries ...ledger.RawEntry[json.RawMessage]) ([]string, error) {
	ids := make([]string, len(entries))
	for i, e := range entries {
		s.seq++
		id := strconv.Itoa(s.seq)
		ids[i] = id
		s.entries[stream] = append(s.entries[stream], ledger.StoredEntry[string, json.RawMessage]{
			ID:            id,
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

func (s *stringStore) Read(_ context.Context, stream string, opts ...ledger.ReadOption) ([]ledger.StoredEntry[string, json.RawMessage], error) {
	o := ledger.ApplyReadOptions(opts...)
	all := s.entries[stream]
	out := make([]ledger.StoredEntry[string, json.RawMessage], 0, len(all))
	after, hasAfter := ledger.AfterValue[string](o)
	for _, e := range all {
		if hasAfter && e.ID <= after {
			continue
		}
		out = append(out, e)
	}
	if o.Limit() > 0 && len(out) > o.Limit() {
		out = out[:o.Limit()]
	}
	return out, nil
}

func (s *stringStore) Count(_ context.Context, stream string) (int64, error) {
	return int64(len(s.entries[stream])), nil
}

func (s *stringStore) Stat(_ context.Context, stream string) (ledger.StreamStat[string], error) {
	es := s.entries[stream]
	st := ledger.StreamStat[string]{Stream: stream, Count: int64(len(es))}
	if len(es) > 0 {
		st.FirstID = es[0].ID
		st.LastID = es[len(es)-1].ID
	}
	return st, nil
}

func (s *stringStore) SetTags(_ context.Context, stream string, id string, tags []string) error {
	for i := range s.entries[stream] {
		if s.entries[stream][i].ID == id {
			s.entries[stream][i].Tags = tags
			return nil
		}
	}
	return ledger.ErrEntryNotFound
}

func (s *stringStore) SetAnnotations(_ context.Context, stream string, id string, annotations map[string]*string) error {
	for i := range s.entries[stream] {
		if s.entries[stream][i].ID == id {
			if s.entries[stream][i].Annotations == nil {
				s.entries[stream][i].Annotations = map[string]string{}
			}
			for k, v := range annotations {
				if v == nil {
					delete(s.entries[stream][i].Annotations, k)
				} else {
					s.entries[stream][i].Annotations[k] = *v
				}
			}
			return nil
		}
	}
	return ledger.ErrEntryNotFound
}

func (s *stringStore) Trim(_ context.Context, stream string, beforeID string) (int64, error) {
	es := s.entries[stream]
	kept := es[:0]
	var removed int64
	for _, e := range es {
		if e.ID <= beforeID {
			removed++
			continue
		}
		kept = append(kept, e)
	}
	s.entries[stream] = kept
	return removed, nil
}

func (s *stringStore) ListStreamIDs(_ context.Context, opts ...ledger.ListOption) ([]string, error) {
	o := ledger.ApplyListOptions(opts...)
	var ids []string
	for id, es := range s.entries {
		if len(es) == 0 {
			continue
		}
		if o.HasAfter() && id <= o.After() {
			continue
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *stringStore) Close(_ context.Context) error { return nil }

// Health makes the store satisfy ledger.HealthChecker so the provider enables Health.
func (s *stringStore) Health(_ context.Context) error { return nil }

var _ ledger.Store[string, json.RawMessage] = (*stringStore)(nil)

func TestStringProvider_AppendReadCount(t *testing.T) {
	store := newStringStore()
	p := ledgerpb.NewStringProvider(store)
	ctx := context.Background()

	ids, err := p.Append(ctx, "s1",
		ledgerpb.InputEntry{Payload: json.RawMessage(`{"v":1}`), SchemaVersion: 1},
		ledgerpb.InputEntry{Payload: json.RawMessage(`{"v":2}`), SchemaVersion: 1},
	)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("Append returned %d ids, want 2", len(ids))
	}

	entries, err := p.Read(ctx, "s1", ledgerpb.ReadOptions{})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("Read returned %d entries, want 2", len(entries))
	}
	if string(entries[0].Payload) != `{"v":1}` {
		t.Errorf("entry[0] payload = %s, want {\"v\":1}", entries[0].Payload)
	}
	if entries[0].ID == "" {
		t.Error("entry[0] ID is empty")
	}

	n, err := p.Count(ctx, "s1")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 2 {
		t.Errorf("Count = %d, want 2", n)
	}
}

func TestStringProvider_ReadAfterCursor(t *testing.T) {
	store := newStringStore()
	p := ledgerpb.NewStringProvider(store)
	ctx := context.Background()

	ids, err := p.Append(ctx, "s",
		ledgerpb.InputEntry{Payload: json.RawMessage(`{}`)},
		ledgerpb.InputEntry{Payload: json.RawMessage(`{}`)},
		ledgerpb.InputEntry{Payload: json.RawMessage(`{}`)},
	)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	entries, err := p.Read(ctx, "s", ledgerpb.ReadOptions{After: ids[0]})
	if err != nil {
		t.Fatalf("Read after cursor: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("Read after %q returned %d entries, want 2", ids[0], len(entries))
	}
}

func TestStringProvider_Stat(t *testing.T) {
	store := newStringStore()
	p := ledgerpb.NewStringProvider(store)
	ctx := context.Background()

	ids, err := p.Append(ctx, "s", ledgerpb.InputEntry{Payload: json.RawMessage(`{}`)}, ledgerpb.InputEntry{Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	st, err := p.Stat(ctx, "s")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.Count != 2 {
		t.Errorf("Stat.Count = %d, want 2", st.Count)
	}
	if st.FirstID != ids[0] || st.LastID != ids[len(ids)-1] {
		t.Errorf("Stat first/last = %q/%q, want %q/%q", st.FirstID, st.LastID, ids[0], ids[len(ids)-1])
	}
}

func TestStringProvider_SetTagsAndAnnotations(t *testing.T) {
	store := newStringStore()
	p := ledgerpb.NewStringProvider(store)
	ctx := context.Background()

	ids, err := p.Append(ctx, "s", ledgerpb.InputEntry{Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	id := ids[0]

	if err := p.SetTags(ctx, "s", id, []string{"x", "y"}); err != nil {
		t.Fatalf("SetTags: %v", err)
	}
	v := "done"
	if err := p.SetAnnotations(ctx, "s", id, map[string]*string{"state": &v}); err != nil {
		t.Fatalf("SetAnnotations: %v", err)
	}

	entries, err := p.Read(ctx, "s", ledgerpb.ReadOptions{})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	e := entries[0]
	if len(e.Tags) != 2 || e.Tags[0] != "x" {
		t.Errorf("Tags = %v, want [x y]", e.Tags)
	}
	if e.Annotations["state"] != "done" {
		t.Errorf("Annotations[state] = %q, want done", e.Annotations["state"])
	}
}

func TestStringProvider_TrimAndList(t *testing.T) {
	store := newStringStore()
	p := ledgerpb.NewStringProvider(store)
	ctx := context.Background()

	ids, err := p.Append(ctx, "s",
		ledgerpb.InputEntry{Payload: json.RawMessage(`{}`)},
		ledgerpb.InputEntry{Payload: json.RawMessage(`{}`)},
		ledgerpb.InputEntry{Payload: json.RawMessage(`{}`)},
	)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	removed, err := p.Trim(ctx, "s", ids[1])
	if err != nil {
		t.Fatalf("Trim: %v", err)
	}
	if removed != 2 {
		t.Errorf("Trim removed = %d, want 2", removed)
	}

	streams, err := p.ListStreamIDs(ctx, "", 0)
	if err != nil {
		t.Fatalf("ListStreamIDs: %v", err)
	}
	if len(streams) != 1 || streams[0] != "s" {
		t.Errorf("ListStreamIDs = %v, want [s]", streams)
	}
}

func TestStringProvider_Health(t *testing.T) {
	store := newStringStore()
	p := ledgerpb.NewStringProvider(store)
	if err := p.Health(context.Background()); err != nil {
		t.Errorf("Health = %v, want nil", err)
	}
}

func TestStringProvider_SearchUnsupported(t *testing.T) {
	// stringStore does not implement ledger.Searcher, so the provider's
	// ProviderSearcher path must report ErrNotSupported.
	store := newStringStore()
	p := ledgerpb.NewStringProvider(store)
	searcher, ok := p.(ledgerpb.ProviderSearcher)
	if !ok {
		t.Skip("provider does not expose ProviderSearcher")
	}
	_, err := searcher.Search(context.Background(), "s", "q", ledgerpb.ReadOptions{})
	if err != ledger.ErrNotSupported {
		t.Errorf("Search = %v, want ErrNotSupported", err)
	}
}
