package webhook

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
)

// NewHMACSigner returns an [http.RoundTripper] that computes an HMAC-SHA256
// signature over the request body and attaches it as a hex-encoded value in
// the named header (e.g. "X-Ledger-Signature").
//
// The underlying transport defaults to [http.DefaultTransport] when nil.
//
// Example:
//
//	signer := webhook.NewHMACSigner([]byte("my-secret"), "X-Ledger-Signature", nil)
//	sink, _ := webhook.New(url, webhook.WithHTTPClient(&http.Client{Transport: signer}))
func NewHMACSigner(secret []byte, header string, transport http.RoundTripper) http.RoundTripper {
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &hmacSigner{secret: secret, header: header, next: transport}
}

type hmacSigner struct {
	secret []byte
	header string
	next   http.RoundTripper
}

func (s *hmacSigner) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body = io.NopCloser(bytes.NewReader(body))

		mac := hmac.New(sha256.New, s.secret)
		mac.Write(body)
		sig := hex.EncodeToString(mac.Sum(nil))

		req = req.Clone(req.Context())
		req.Header.Set(s.header, sig)
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.ContentLength = int64(len(body))
	}
	return s.next.RoundTrip(req)
}
