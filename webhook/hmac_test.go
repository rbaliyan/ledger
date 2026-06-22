package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"testing"
)

// recordingTransport captures the request it receives and returns a 200.
// headerName selects which header value to record; it defaults to
// "X-Ledger-Signature" when empty.
type recordingTransport struct {
	headerName string
	gotHeader  string
	gotBody    []byte
}

func (rt *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		rt.gotBody = body
	}
	name := rt.headerName
	if name == "" {
		name = "X-Ledger-Signature"
	}
	rt.gotHeader = req.Header.Get(name)
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Header:     make(http.Header),
	}, nil
}

func TestNewHMACSigner_GoldenSignature(t *testing.T) {
	secret := []byte("my-secret-key")
	body := []byte(`{"stream":"s","entries":[]}`)

	// Compute the expected signature independently with the stdlib.
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	want := hex.EncodeToString(mac.Sum(nil))

	rt := &recordingTransport{}
	signer := NewHMACSigner(secret, "X-Ledger-Signature", rt)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		"http://example.test/hook", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	resp, err := signer.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	resp.Body.Close()

	if rt.gotHeader != want {
		t.Errorf("signature header = %q, want %q", rt.gotHeader, want)
	}
	if !bytes.Equal(rt.gotBody, body) {
		t.Errorf("forwarded body = %q, want %q", rt.gotBody, body)
	}
}

func TestNewHMACSigner_CustomHeaderName(t *testing.T) {
	rt := &recordingTransport{headerName: "X-My-Sig"}
	signer := NewHMACSigner([]byte("k"), "X-My-Sig", rt)

	body := []byte("payload")
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost,
		"http://example.test", bytes.NewReader(body))

	resp, err := signer.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	resp.Body.Close()

	mac := hmac.New(sha256.New, []byte("k"))
	mac.Write(body)
	want := hex.EncodeToString(mac.Sum(nil))

	// The signer clones the request and sets the signature on the clone the
	// transport receives; assert against the value the transport observed.
	if rt.gotHeader != want {
		t.Errorf("custom header signature = %q, want %q", rt.gotHeader, want)
	}
}

func TestNewHMACSigner_NilBodyPassthrough(t *testing.T) {
	rt := &recordingTransport{}
	signer := NewHMACSigner([]byte("k"), "X-Ledger-Signature", rt)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet,
		"http://example.test", nil)

	resp, err := signer.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	resp.Body.Close()

	if rt.gotHeader != "" {
		t.Errorf("expected no signature for nil-body request, got %q", rt.gotHeader)
	}
}

func TestNewHMACSigner_NilTransportDefaults(t *testing.T) {
	// A nil transport must default to http.DefaultTransport rather than panic.
	signer := NewHMACSigner([]byte("k"), "X-Ledger-Signature", nil)
	if signer == nil {
		t.Fatal("NewHMACSigner returned nil")
	}
}
