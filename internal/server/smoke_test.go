package server_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	ledgerv1 "github.com/rbaliyan/ledger/api/ledger/v1"
	"github.com/rbaliyan/ledger/internal/config"
	"github.com/rbaliyan/ledger/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	_ "modernc.org/sqlite"
)

// newSmokeServer boots a real in-process gRPC server over :memory: SQLite and
// returns the server handle plus a connected client. Unlike newTestServer it
// does not register a Stop cleanup, so the test controls shutdown explicitly.
func newSmokeServer(t *testing.T) (*server.Server, ledgerv1.LedgerServiceClient) {
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

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()
	t.Cleanup(func() {
		select {
		case err := <-serveErr:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		default:
		}
	})

	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", srv.Addr(), err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return srv, ledgerv1.NewLedgerServiceClient(conn)
}

// TestSmokeServerStartStopIdempotent boots the server, does one successful
// round-trip, then stops it twice and asserts a follow-up RPC fails because the
// server is no longer serving.
func TestSmokeServerStartStopIdempotent(t *testing.T) {
	srv, client := newSmokeServer(t)
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

	// Stop is idempotent: a second Stop must not panic or block.
	srv.Stop(context.Background())
	done := make(chan struct{})
	go func() {
		srv.Stop(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("second Stop blocked")
	}

	// A follow-up RPC against the stopped server must fail. After GracefulStop
	// the listener is closed; the client cannot reach the server, which surfaces
	// as Unavailable.
	rpcCtx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, err = client.Count(storeCtx(rpcCtx, "orders"), &ledgerv1.CountRequest{Stream: "order-1"})
	if err == nil {
		t.Fatal("expected error after Stop, got nil")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unavailable {
		t.Errorf("expected Unavailable after Stop, got: %v", err)
	}
}

// TestSmokeServerErrorPath is the gRPC-side twin of the gateway NotFound test:
// SetTags on a non-existent entry must surface codes.NotFound.
func TestSmokeServerErrorPath(t *testing.T) {
	client := newTestServer(t)
	ctx := storeCtx(t.Context(), "errors_store")

	// Create the stream so the store/table exists, then target a missing entry.
	payload, _ := json.Marshal(map[string]string{"event": "created"})
	if _, err := client.Append(ctx, &ledgerv1.AppendRequest{
		Stream:  "s",
		Entries: []*ledgerv1.EntryInput{{Payload: payload}},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	_, err := client.SetTags(ctx, &ledgerv1.SetTagsRequest{
		Stream: "s",
		Id:     "999999",
		Tags:   []string{"foo"},
	})
	if err == nil {
		t.Fatal("expected NotFound for non-existent entry, got nil")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got: %v", err)
	}
}
