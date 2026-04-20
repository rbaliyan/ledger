package ledgerpb_test

import (
	"context"
	"database/sql"
	"net"
	"testing"

	ledgerv1 "github.com/rbaliyan/ledger/api/ledger/v1"
	"github.com/rbaliyan/ledger/ledgerpb"
	"github.com/rbaliyan/ledger/sqlite"
	_ "modernc.org/sqlite"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20

// newTestBackend creates a fresh in-memory SQLite backend for each test.
func newTestBackend(t *testing.T) ledgerpb.Provider {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	store, err := sqlite.New(context.Background(), db, sqlite.WithTable("events"))
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	t.Cleanup(func() { store.Close(context.Background()) })

	return ledgerpb.NewInt64Provider(store)
}

// newTestServer starts an in-process gRPC server and returns a client + cleanup func.
func newTestServer(t *testing.T, backend ledgerpb.Provider, opts ...grpc.ServerOption) (ledgerv1.LedgerServiceClient, func()) {
	t.Helper()

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer(opts...)
	ledgerv1.RegisterLedgerServiceServer(srv, ledgerpb.NewServer(backend))

	go srv.Serve(lis)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}

	client := ledgerv1.NewLedgerServiceClient(conn)
	cleanup := func() {
		conn.Close()
		srv.GracefulStop()
	}
	return client, cleanup
}

func TestServer(t *testing.T) {
	backend := newTestBackend(t)
	client, cleanup := newTestServer(t, backend)
	defer cleanup()

	ctx := context.Background()
	payload := []byte(`{"x":1}`)

	t.Run("AppendAndRead", func(t *testing.T) {
		resp, err := client.Append(ctx, &ledgerv1.AppendRequest{
			Stream: "s1",
			Entries: []*ledgerv1.EntryInput{
				{Payload: payload},
				{Payload: payload},
				{Payload: payload},
			},
		})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if len(resp.Ids) != 3 {
			t.Fatalf("expected 3 IDs, got %d", len(resp.Ids))
		}
		for i, id := range resp.Ids {
			if id == "" {
				t.Errorf("ID[%d] is empty", i)
			}
		}

		readResp, err := client.Read(ctx, &ledgerv1.ReadRequest{Stream: "s1"})
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if len(readResp.Entries) != 3 {
			t.Fatalf("expected 3 entries, got %d", len(readResp.Entries))
		}
		for i, e := range readResp.Entries {
			if string(e.Payload) != string(payload) {
				t.Errorf("entry[%d] payload = %s, want %s", i, e.Payload, payload)
			}
			if e.Id == "" {
				t.Errorf("entry[%d] ID is empty", i)
			}
		}
	})

	t.Run("DedupSkipped", func(t *testing.T) {
		resp1, err := client.Append(ctx, &ledgerv1.AppendRequest{
			Stream: "dedup-stream",
			Entries: []*ledgerv1.EntryInput{
				{Payload: payload, DedupKey: "k1"},
			},
		})
		if err != nil {
			t.Fatalf("Append 1: %v", err)
		}
		if len(resp1.Ids) != 1 {
			t.Fatalf("expected 1 ID on first append, got %d", len(resp1.Ids))
		}

		resp2, err := client.Append(ctx, &ledgerv1.AppendRequest{
			Stream: "dedup-stream",
			Entries: []*ledgerv1.EntryInput{
				{Payload: payload, DedupKey: "k1"},
			},
		})
		if err != nil {
			t.Fatalf("Append 2: %v", err)
		}
		if len(resp2.Ids) != 0 {
			t.Fatalf("expected 0 IDs on dedup append, got %d: %v", len(resp2.Ids), resp2.Ids)
		}
	})

	t.Run("Count", func(t *testing.T) {
		entries := make([]*ledgerv1.EntryInput, 4)
		for i := range entries {
			entries[i] = &ledgerv1.EntryInput{Payload: payload}
		}
		_, err := client.Append(ctx, &ledgerv1.AppendRequest{
			Stream:  "count-stream",
			Entries: entries,
		})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}

		countResp, err := client.Count(ctx, &ledgerv1.CountRequest{Stream: "count-stream"})
		if err != nil {
			t.Fatalf("Count: %v", err)
		}
		if countResp.Count != 4 {
			t.Errorf("Count = %d, want 4", countResp.Count)
		}
	})

	t.Run("SetTagsAndRead", func(t *testing.T) {
		appResp, err := client.Append(ctx, &ledgerv1.AppendRequest{
			Stream:  "tag-stream",
			Entries: []*ledgerv1.EntryInput{{Payload: payload}},
		})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		id := appResp.Ids[0]

		_, err = client.SetTags(ctx, &ledgerv1.SetTagsRequest{
			Stream: "tag-stream",
			Id:     id,
			Tags:   []string{"a", "b"},
		})
		if err != nil {
			t.Fatalf("SetTags: %v", err)
		}

		readResp, err := client.Read(ctx, &ledgerv1.ReadRequest{Stream: "tag-stream"})
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if len(readResp.Entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(readResp.Entries))
		}
		e := readResp.Entries[0]
		if len(e.Tags) != 2 || e.Tags[0] != "a" || e.Tags[1] != "b" {
			t.Errorf("Tags = %v, want [a b]", e.Tags)
		}
		if e.UpdatedAt == nil {
			t.Error("UpdatedAt should be non-nil after SetTags")
		}
	})

	t.Run("SetAnnotations", func(t *testing.T) {
		appResp, err := client.Append(ctx, &ledgerv1.AppendRequest{
			Stream:  "annot-stream",
			Entries: []*ledgerv1.EntryInput{{Payload: payload}},
		})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		id := appResp.Ids[0]

		// Set annotation k=v.
		_, err = client.SetAnnotations(ctx, &ledgerv1.SetAnnotationsRequest{
			Stream: "annot-stream",
			Id:     id,
			Set:    map[string]string{"k": "v"},
		})
		if err != nil {
			t.Fatalf("SetAnnotations (set): %v", err)
		}

		readResp, err := client.Read(ctx, &ledgerv1.ReadRequest{Stream: "annot-stream"})
		if err != nil {
			t.Fatalf("Read after set: %v", err)
		}
		if len(readResp.Entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(readResp.Entries))
		}
		if got := readResp.Entries[0].Annotations["k"]; got != "v" {
			t.Errorf("annotations[k] = %q, want %q", got, "v")
		}

		// Delete annotation k.
		_, err = client.SetAnnotations(ctx, &ledgerv1.SetAnnotationsRequest{
			Stream: "annot-stream",
			Id:     id,
			Set:    map[string]string{},
			Delete: []string{"k"},
		})
		if err != nil {
			t.Fatalf("SetAnnotations (delete): %v", err)
		}

		readResp2, err := client.Read(ctx, &ledgerv1.ReadRequest{Stream: "annot-stream"})
		if err != nil {
			t.Fatalf("Read after delete: %v", err)
		}
		if _, exists := readResp2.Entries[0].Annotations["k"]; exists {
			t.Error("expected annotation 'k' to be deleted, but it still exists")
		}
	})

	t.Run("Trim", func(t *testing.T) {
		var ids []string
		for i := 0; i < 5; i++ {
			resp, err := client.Append(ctx, &ledgerv1.AppendRequest{
				Stream:  "trim-stream",
				Entries: []*ledgerv1.EntryInput{{Payload: payload}},
			})
			if err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
			ids = append(ids, resp.Ids[0])
		}

		// Trim with before_id = id of 3rd entry deletes entries 1, 2, 3 → 2 remain.
		_, err := client.Trim(ctx, &ledgerv1.TrimRequest{
			Stream:   "trim-stream",
			BeforeId: ids[2],
		})
		if err != nil {
			t.Fatalf("Trim: %v", err)
		}

		countResp, err := client.Count(ctx, &ledgerv1.CountRequest{Stream: "trim-stream"})
		if err != nil {
			t.Fatalf("Count after Trim: %v", err)
		}
		if countResp.Count != 2 {
			t.Errorf("Count after Trim = %d, want 2", countResp.Count)
		}
	})

	t.Run("ListStreamIDs", func(t *testing.T) {
		for _, stream := range []string{"z-a", "z-b", "z-c"} {
			_, err := client.Append(ctx, &ledgerv1.AppendRequest{
				Stream:  stream,
				Entries: []*ledgerv1.EntryInput{{Payload: payload}},
			})
			if err != nil {
				t.Fatalf("Append to %s: %v", stream, err)
			}
		}

		// List all — should contain at least z-a, z-b, z-c (plus streams from earlier sub-tests).
		listResp, err := client.ListStreamIDs(ctx, &ledgerv1.ListStreamIDsRequest{After: ""})
		if err != nil {
			t.Fatalf("ListStreamIDs: %v", err)
		}
		got := make(map[string]bool, len(listResp.StreamIds))
		for _, id := range listResp.StreamIds {
			got[id] = true
		}
		for _, want := range []string{"z-a", "z-b", "z-c"} {
			if !got[want] {
				t.Errorf("ListStreamIDs missing %q; got %v", want, listResp.StreamIds)
			}
		}

		// After "z-a" returns z-b and z-c (other streams are lexicographically before "z-").
		listResp2, err := client.ListStreamIDs(ctx, &ledgerv1.ListStreamIDsRequest{After: "z-a"})
		if err != nil {
			t.Fatalf("ListStreamIDs after z-a: %v", err)
		}
		if len(listResp2.StreamIds) != 2 {
			t.Fatalf("expected 2 stream IDs after z-a, got %d: %v", len(listResp2.StreamIds), listResp2.StreamIds)
		}
		if listResp2.StreamIds[0] != "z-b" || listResp2.StreamIds[1] != "z-c" {
			t.Errorf("ListStreamIDs after z-a = %v, want [z-b z-c]", listResp2.StreamIds)
		}
	})

	t.Run("Health", func(t *testing.T) {
		resp, err := client.Health(ctx, &ledgerv1.HealthRequest{})
		if err != nil {
			t.Fatalf("Health: %v", err)
		}
		if resp.Status != "ok" {
			t.Errorf("Health status = %q, want %q", resp.Status, "ok")
		}
	})
}

// ---------------------------------------------------------------------------
// Security guard stubs
// ---------------------------------------------------------------------------

// stubIdentity is a minimal Identity implementation for tests.
type stubIdentity struct {
	userID string
}

func (s stubIdentity) UserID() string         { return s.userID }
func (s stubIdentity) Claims() map[string]any { return nil }

// allowGuard always authenticates and authorises requests.
type allowGuard struct{}

func (allowGuard) Authenticate(_ context.Context) (ledgerpb.Identity, error) {
	return stubIdentity{userID: "test-user"}, nil
}

func (allowGuard) Authorize(_ context.Context, _ ledgerpb.Identity, _, _ string) (ledgerpb.Decision, error) {
	return ledgerpb.Decision{Allowed: true}, nil
}

// denyGuard authenticates successfully but always denies authorisation.
type denyGuard struct{}

func (denyGuard) Authenticate(_ context.Context) (ledgerpb.Identity, error) {
	return stubIdentity{userID: "test-user"}, nil
}

func (denyGuard) Authorize(_ context.Context, _ ledgerpb.Identity, _, _ string) (ledgerpb.Decision, error) {
	return ledgerpb.Decision{Allowed: false, Reason: "test denial"}, nil
}

// captureGuard records the UserID observed in Authorize so the test can inspect it.
type captureGuard struct {
	capturedUserID string
}

func (g *captureGuard) Authenticate(_ context.Context) (ledgerpb.Identity, error) {
	return stubIdentity{userID: "captured-user"}, nil
}

func (g *captureGuard) Authorize(_ context.Context, id ledgerpb.Identity, _, _ string) (ledgerpb.Decision, error) {
	g.capturedUserID = id.UserID()
	return ledgerpb.Decision{Allowed: true}, nil
}

// ---------------------------------------------------------------------------
// Interceptor / identity tests
// ---------------------------------------------------------------------------

func TestServer_SecurityGuard_UnaryInterceptor(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	payload := []byte(`{"x":1}`)

	t.Run("Allow", func(t *testing.T) {
		client, cleanup := newTestServer(t, backend,
			grpc.UnaryInterceptor(ledgerpb.UnaryInterceptor(allowGuard{})),
		)
		defer cleanup()

		_, err := client.Append(ctx, &ledgerv1.AppendRequest{
			Stream:  "sec-allow",
			Entries: []*ledgerv1.EntryInput{{Payload: payload}},
		})
		if err != nil {
			t.Fatalf("Append with allow guard should succeed: %v", err)
		}
	})

	t.Run("Deny", func(t *testing.T) {
		client, cleanup := newTestServer(t, backend,
			grpc.UnaryInterceptor(ledgerpb.UnaryInterceptor(denyGuard{})),
		)
		defer cleanup()

		_, err := client.Append(ctx, &ledgerv1.AppendRequest{
			Stream:  "sec-deny",
			Entries: []*ledgerv1.EntryInput{{Payload: payload}},
		})
		if err == nil {
			t.Fatal("expected PermissionDenied error with deny guard, got nil")
		}
		st, ok := status.FromError(err)
		if !ok {
			t.Fatalf("expected gRPC status error, got: %v", err)
		}
		if st.Code() != codes.PermissionDenied {
			t.Errorf("status code = %v, want %v", st.Code(), codes.PermissionDenied)
		}
	})
}

func TestServer_IdentityFromContext(t *testing.T) {
	backend := newTestBackend(t)
	guard := &captureGuard{}

	client, cleanup := newTestServer(t, backend,
		grpc.UnaryInterceptor(ledgerpb.UnaryInterceptor(guard)),
	)
	defer cleanup()

	ctx := context.Background()
	_, err := client.Append(ctx, &ledgerv1.AppendRequest{
		Stream:  "identity-stream",
		Entries: []*ledgerv1.EntryInput{{Payload: []byte(`{"x":1}`)}},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	if guard.capturedUserID == "" {
		t.Fatal("expected capturedUserID to be non-empty after RPC")
	}
	if guard.capturedUserID != "captured-user" {
		t.Errorf("capturedUserID = %q, want %q", guard.capturedUserID, "captured-user")
	}
}
