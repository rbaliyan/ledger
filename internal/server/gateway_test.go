package server_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/rbaliyan/ledger/internal/config"
	"github.com/rbaliyan/ledger/internal/server"
	"github.com/rbaliyan/ledger/ledgerpb"
	"context"

	_ "modernc.org/sqlite"
)

// newGatewayServer starts a server with both gRPC and HTTP gateway listeners.
func newGatewayServer(t *testing.T) (grpcAddr, httpAddr string) {
	t.Helper()
	cfg := &config.Config{
		Listen:     "127.0.0.1:0",
		HTTPListen: "127.0.0.1:0",
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
	return srv.Addr(), srv.HTTPAddr()
}

func TestGatewayHealth(t *testing.T) {
	_, httpAddr := newGatewayServer(t)

	resp, err := http.Get(fmt.Sprintf("http://%s/v1/health", httpAddr)) //nolint:noctx
	if err != nil {
		t.Fatalf("GET /v1/health: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status=ok, got %q", body["status"])
	}
}

func TestGatewayAppendRead(t *testing.T) {
	_, httpAddr := newGatewayServer(t)
	base := fmt.Sprintf("http://%s", httpAddr)

	payload, _ := json.Marshal(map[string]string{"event": "created"})
	body, _ := json.Marshal(map[string]any{
		"entries": []map[string]any{
			{"payload": payload},
		},
	})

	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost,
		base+"/v1/streams/order-1/entries", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(ledgerpb.StoreMetadataHeader, "orders")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST entries: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, raw)
	}

	var appendResp map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&appendResp); err != nil {
		t.Fatalf("decode append response: %v", err)
	}
	ids, _ := appendResp["ids"].([]any)
	if len(ids) != 1 {
		t.Fatalf("expected 1 ID, got %v", appendResp)
	}

	// Read back via GET.
	req2, _ := http.NewRequestWithContext(t.Context(), http.MethodGet,
		base+"/v1/streams/order-1/entries", nil)
	req2.Header.Set(ledgerpb.StoreMetadataHeader, "orders")

	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("GET entries: %v", err)
	}
	defer resp2.Body.Close() //nolint:errcheck
	if resp2.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp2.Body)
		t.Fatalf("expected 200, got %d: %s", resp2.StatusCode, raw)
	}
	var readResp map[string]any
	if err := json.NewDecoder(resp2.Body).Decode(&readResp); err != nil {
		t.Fatalf("decode read response: %v", err)
	}
	entries, _ := readResp["entries"].([]any)
	if len(entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(entries))
	}
}

func TestGatewayAPIKeyAuth(t *testing.T) {
	cfg := &config.Config{
		Listen:     "127.0.0.1:0",
		HTTPListen: "127.0.0.1:0",
		APIKey:     "secret",
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

	base := fmt.Sprintf("http://%s", srv.HTTPAddr())

	// No API key — should be rejected.
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet,
		base+"/v1/streams/s/entries:count", nil)
	req.Header.Set(ledgerpb.StoreMetadataHeader, "test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}

	// With correct API key — should succeed.
	req2, _ := http.NewRequestWithContext(t.Context(), http.MethodGet,
		base+"/v1/streams/s/entries:count", nil)
	req2.Header.Set(ledgerpb.StoreMetadataHeader, "test")
	req2.Header.Set(ledgerpb.APIKeyMetadataHeader, "secret")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("request with key: %v", err)
	}
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp2.StatusCode)
	}
}
