package server_test

import (
	"context"
	"encoding/json"
	"testing"

	ledgerv1 "github.com/rbaliyan/ledger/api/ledger/v1"
	"github.com/rbaliyan/ledger/internal/config"
	"github.com/rbaliyan/ledger/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

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

// storeCtx returns a context with the x-ledger-store header set.
func storeCtx(ctx context.Context, store string) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("x-ledger-store", store))
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
		"x-ledger-store", "test",
		"x-api-key", "secret",
	))
	_, err = client.Count(ctx, &ledgerv1.CountRequest{Stream: "s"})
	if err != nil {
		t.Fatalf("expected success with valid key, got: %v", err)
	}
}
