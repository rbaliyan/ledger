package server_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rbaliyan/ledger/internal/config"
	"github.com/rbaliyan/ledger/internal/server"
	"github.com/rbaliyan/ledger/ledgerpb"

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

	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet,
		fmt.Sprintf("http://%s/v1/health", httpAddr), nil)
	resp, err := http.DefaultClient.Do(req)
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

	// No API key — should be rejected with 401.
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

// TestGatewayErrorMapping verifies that gRPC status codes are translated to
// the correct HTTP status codes by the grpc-gateway error handler.
func TestGatewayErrorMapping(t *testing.T) {
	_, httpAddr := newGatewayServer(t)
	base := fmt.Sprintf("http://%s", httpAddr)

	// SetTags on a non-existent entry → gRPC NotFound → HTTP 404.
	body, _ := json.Marshal(map[string]any{"tags": []string{"foo"}})
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPut,
		base+"/v1/streams/s/entries/nonexistent/tags", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(ledgerpb.StoreMetadataHeader, "test")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT tags: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusNotFound {
		raw, _ := io.ReadAll(resp.Body)
		t.Errorf("expected 404 for non-existent entry, got %d: %s", resp.StatusCode, raw)
	}
}

// TestGatewayTLS verifies that when gRPC is configured with TLS the gateway
// correctly dials back using the server certificate as the trusted CA.
func TestGatewayTLS(t *testing.T) {
	certPEM, keyPEM := generateSelfSignedCert(t)
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	cfg := &config.Config{
		Listen:     "127.0.0.1:0",
		HTTPListen: "127.0.0.1:0",
		TLS: config.TLSConfig{
			Cert: certFile,
			Key:  keyFile,
			// No CA: gateway falls back to using Cert as CA (self-signed).
		},
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

	// The HTTP gateway itself serves plain HTTP; only the gRPC loopback uses TLS.
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet,
		fmt.Sprintf("http://%s/v1/health", srv.HTTPAddr()), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET health over TLS-backed gateway: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, raw)
	}
}

// generateSelfSignedCert returns PEM-encoded certificate and private key for
// a certificate valid for localhost / 127.0.0.1.
func generateSelfSignedCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	privDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER})
	return certPEM, keyPEM
}
