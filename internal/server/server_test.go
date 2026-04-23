package server_test

import (
	"context"
	"encoding/json"
	"testing"

	ledgerv1 "github.com/rbaliyan/ledger/api/ledger/v1"
	"github.com/rbaliyan/ledger/internal/config"
	"github.com/rbaliyan/ledger/internal/server"
	"github.com/rbaliyan/ledger/ledgerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	_ "modernc.org/sqlite"
)

// newTestServer boots a real gRPC server on 127.0.0.1:0 using SQLite backed
// by a temp directory. Returns the client and calls t.Cleanup for shutdown.
func newTestServer(t *testing.T) ledgerv1.LedgerServiceClient {
	t.Helper()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		DB: config.DBConfig{
			Type:   "sqlite",
			SQLite: config.SQLiteConfig{Path: ":memory:"},
		},
	}

	srv, err := server.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	t.Cleanup(func() { srv.Stop(context.Background()) })

	go func() { _ = srv.Serve() }()

	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", srv.Addr(), err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return ledgerv1.NewLedgerServiceClient(conn)
}

// storeCtx returns a context with the store metadata header set.
func storeCtx(ctx context.Context, store string) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(ledgerpb.StoreMetadataHeader, store))
}

func TestServerAppendRead(t *testing.T) {
	client := newTestServer(t)
	ctx := storeCtx(t.Context(), "orders")

	payload, _ := json.Marshal(map[string]string{"event": "created"})
	appendResp, err := client.Append(ctx, &ledgerv1.AppendRequest{
		Stream:  "order-1",
		Entries: []*ledgerv1.EntryInput{{Payload: payload}},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(appendResp.Ids) != 1 {
		t.Fatalf("expected 1 ID, got %d", len(appendResp.Ids))
	}

	readResp, err := client.Read(ctx, &ledgerv1.ReadRequest{Stream: "order-1"})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(readResp.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(readResp.Entries))
	}
	if string(readResp.Entries[0].Payload) != string(payload) {
		t.Errorf("payload mismatch: got %s, want %s", readResp.Entries[0].Payload, payload)
	}
}

func TestServerCount(t *testing.T) {
	client := newTestServer(t)
	ctx := storeCtx(t.Context(), "events")

	payload, _ := json.Marshal(map[string]int{"n": 1})
	for range 3 {
		if _, err := client.Append(ctx, &ledgerv1.AppendRequest{
			Stream:  "stream-a",
			Entries: []*ledgerv1.EntryInput{{Payload: payload}},
		}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	resp, err := client.Count(ctx, &ledgerv1.CountRequest{Stream: "stream-a"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if resp.Count != 3 {
		t.Errorf("expected count=3, got %d", resp.Count)
	}
}

func TestServerListStreamIDs(t *testing.T) {
	client := newTestServer(t)
	ctx := storeCtx(t.Context(), "users")

	payload, _ := json.Marshal(struct{}{})
	for _, id := range []string{"user-1", "user-2", "user-3"} {
		if _, err := client.Append(ctx, &ledgerv1.AppendRequest{
			Stream:  id,
			Entries: []*ledgerv1.EntryInput{{Payload: payload}},
		}); err != nil {
			t.Fatalf("Append %s: %v", id, err)
		}
	}

	resp, err := client.ListStreamIDs(ctx, &ledgerv1.ListStreamIDsRequest{})
	if err != nil {
		t.Fatalf("ListStreamIDs: %v", err)
	}
	if len(resp.StreamIds) != 3 {
		t.Errorf("expected 3 stream IDs, got %d: %v", len(resp.StreamIds), resp.StreamIds)
	}
}

func TestServerStreamNaming(t *testing.T) {
	client := newTestServer(t)
	ctx := storeCtx(t.Context(), "naming_test")

	payload, _ := json.Marshal("hello")
	if _, err := client.Append(ctx, &ledgerv1.AppendRequest{
		Stream:  "my-stream",
		Entries: []*ledgerv1.EntryInput{{Payload: payload}},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Read entries back — stream field must be the human-readable name, not a UUID.
	readResp, err := client.Read(ctx, &ledgerv1.ReadRequest{Stream: "my-stream"})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(readResp.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(readResp.Entries))
	}
	if readResp.Entries[0].Stream != "my-stream" {
		t.Errorf("entry.stream = %q, want %q", readResp.Entries[0].Stream, "my-stream")
	}

	// ListStreamIDs must return the human-readable name.
	listResp, err := client.ListStreamIDs(ctx, &ledgerv1.ListStreamIDsRequest{})
	if err != nil {
		t.Fatalf("ListStreamIDs: %v", err)
	}
	if len(listResp.StreamIds) != 1 || listResp.StreamIds[0] != "my-stream" {
		t.Errorf("ListStreamIDs = %v, want [my-stream]", listResp.StreamIds)
	}
}

func TestServerRenameStream(t *testing.T) {
	client := newTestServer(t)
	ctx := storeCtx(t.Context(), "rename_test")

	payload, _ := json.Marshal("entry")
	if _, err := client.Append(ctx, &ledgerv1.AppendRequest{
		Stream:  "old-name",
		Entries: []*ledgerv1.EntryInput{{Payload: payload}},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Rename the stream.
	if _, err := client.RenameStream(ctx, &ledgerv1.RenameStreamRequest{
		Name:    "old-name",
		NewName: "new-name",
	}); err != nil {
		t.Fatalf("RenameStream: %v", err)
	}

	// Entries are accessible under the new name.
	readResp, err := client.Read(ctx, &ledgerv1.ReadRequest{Stream: "new-name"})
	if err != nil {
		t.Fatalf("Read after rename: %v", err)
	}
	if len(readResp.Entries) != 1 {
		t.Fatalf("expected 1 entry after rename, got %d", len(readResp.Entries))
	}

	// Old name returns nothing (it's a fresh stream with no entries).
	readOld, err := client.Read(ctx, &ledgerv1.ReadRequest{Stream: "old-name"})
	if err != nil {
		t.Fatalf("Read old-name: %v", err)
	}
	if len(readOld.Entries) != 0 {
		t.Errorf("expected 0 entries for old-name after rename, got %d", len(readOld.Entries))
	}

	// ListStreamIDs reflects the new name only. Read-only operations on the
	// non-existent old-name must not create a phantom mapping.
	listResp, err := client.ListStreamIDs(ctx, &ledgerv1.ListStreamIDsRequest{})
	if err != nil {
		t.Fatalf("ListStreamIDs: %v", err)
	}
	if len(listResp.StreamIds) != 1 || listResp.StreamIds[0] != "new-name" {
		t.Errorf("ListStreamIDs = %v, want [new-name]", listResp.StreamIds)
	}
}

func TestServerAllowedStores(t *testing.T) {
	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		APIKey: "secret",
		DB: config.DBConfig{
			Type:   "sqlite",
			SQLite: config.SQLiteConfig{Path: ":memory:"},
		},
		AllowedStores: []string{"allowed"},
	}

	srv, err := server.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	t.Cleanup(func() { srv.Stop(context.Background()) })
	go func() { _ = srv.Serve() }()

	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := ledgerv1.NewLedgerServiceClient(conn)

	authMD := metadata.Pairs(ledgerpb.APIKeyMetadataHeader, "secret")

	// Access to an allowed store should succeed.
	allowedCtx := metadata.NewOutgoingContext(t.Context(), metadata.Join(
		authMD, metadata.Pairs(ledgerpb.StoreMetadataHeader, "allowed"),
	))
	if _, err := client.Count(allowedCtx, &ledgerv1.CountRequest{Stream: "s"}); err != nil {
		t.Fatalf("expected success for allowed store, got: %v", err)
	}

	// Access to a denied store should return PermissionDenied.
	deniedCtx := metadata.NewOutgoingContext(t.Context(), metadata.Join(
		authMD, metadata.Pairs(ledgerpb.StoreMetadataHeader, "denied"),
	))
	_, err = client.Count(deniedCtx, &ledgerv1.CountRequest{Stream: "s"})
	if err == nil {
		t.Fatal("expected permission denied for unlisted store, got nil")
	}
	if status, ok := status.FromError(err); !ok || status.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got: %v", err)
	}
}

func TestServerAPIKeyRequired(t *testing.T) {
	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		APIKey: "secret",
		DB: config.DBConfig{
			Type:   "sqlite",
			SQLite: config.SQLiteConfig{Path: ":memory:"},
		},
	}

	srv, err := server.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	t.Cleanup(func() { srv.Stop(context.Background()) })
	go func() { _ = srv.Serve() }()

	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := ledgerv1.NewLedgerServiceClient(conn)

	// Without API key — should be rejected.
	ctx := storeCtx(t.Context(), "test")
	_, err = client.Count(ctx, &ledgerv1.CountRequest{Stream: "s"})
	if err == nil {
		t.Fatal("expected auth error, got nil")
	}

	// With correct API key — should succeed.
	ctx = metadata.NewOutgoingContext(t.Context(), metadata.Pairs(
		ledgerpb.StoreMetadataHeader, "test",
		ledgerpb.APIKeyMetadataHeader, "secret",
	))
	_, err = client.Count(ctx, &ledgerv1.CountRequest{Stream: "s"})
	if err != nil {
		t.Fatalf("expected success with valid key, got: %v", err)
	}
}
