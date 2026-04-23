// Package webhook provides a ledger.Store[string, json.RawMessage] sink that
// delivers appended entries to a remote HTTP endpoint via POST.
//
// Only [Sink.Append] performs real work; all other Store methods return
// [ledger.ErrNotSupported]. Use this as a [bridge.Bridge] sink together with
// [bridge.WithSkipMutationTypes](MutationSetTags, MutationSetAnnotations).
//
// Delivery is retried with exponential back-off on transient errors only.
// Permanent HTTP errors (4xx except 408/425/429) are not retried (see [RetryPolicy]).
//
// # HMAC signing
//
// Use [NewHMACSigner] to wrap the default HTTP client with HMAC-SHA256 request
// signing, then supply it via [WithHTTPClient]. Receivers verify the signature
// against the value in the configured header.
package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rbaliyan/ledger"
)

var _ ledger.Store[string, json.RawMessage] = (*Sink)(nil)

// Sink is a write-only ledger Store that POSTs appended entries to a URL.
type Sink struct {
	url    string
	opts   options
	closed atomic.Bool
}

// New creates a Sink that POSTs entries to url.
func New(url string, opts ...Option) (*Sink, error) {
	if url == "" {
		return nil, fmt.Errorf("webhook: url must not be empty")
	}
	o := defaultOptions()
	for _, fn := range opts {
		fn(&o)
	}
	return &Sink{url: url, opts: o}, nil
}

// payload is the JSON body sent on each Append call.
type payload struct {
	Stream  string         `json:"stream"`
	Entries []payloadEntry `json:"entries"`
}

type payloadEntry struct {
	Payload       json.RawMessage   `json:"payload"`
	OrderKey      string            `json:"order_key,omitempty"`
	DedupKey      string            `json:"dedup_key,omitempty"`
	SchemaVersion int               `json:"schema_version"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	Tags          []string          `json:"tags,omitempty"`
	SourceID      string            `json:"source_id,omitempty"`
}

// HookEntry is the full entry shape delivered by daemon event hooks via [Sink.Deliver].
// It mirrors ledgerpb.StoredEntry 1:1, including mutable fields not present in [ledger.RawEntry].
type HookEntry struct {
	ID            string            `json:"id"`
	Payload       json.RawMessage   `json:"payload"`
	OrderKey      string            `json:"order_key,omitempty"`
	DedupKey      string            `json:"dedup_key,omitempty"`
	SchemaVersion int               `json:"schema_version"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	Tags          []string          `json:"tags,omitempty"`
	Annotations   map[string]string `json:"annotations,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     *time.Time        `json:"updated_at,omitempty"`
}

// hookPayload is the JSON body sent by [Sink.Deliver].
type hookPayload struct {
	Stream  string      `json:"stream"`
	Entries []HookEntry `json:"entries"`
}

// Append POSTs entries to the webhook URL and returns generated IDs.
// It retries on transient failures per the configured [RetryPolicy].
func (s *Sink) Append(ctx context.Context, stream string, entries ...ledger.RawEntry[json.RawMessage]) ([]string, error) {
	if s.closed.Load() {
		return nil, ledger.ErrStoreClosed
	}
	if len(entries) == 0 {
		return nil, nil
	}

	pe := make([]payloadEntry, len(entries))
	ids := make([]string, len(entries))
	for i, e := range entries {
		ids[i] = uuid.New().String()
		pe[i] = payloadEntry{
			Payload:       e.Payload,
			OrderKey:      e.OrderKey,
			DedupKey:      e.DedupKey,
			SchemaVersion: e.SchemaVersion,
			Metadata:      e.Metadata,
			Tags:          e.Tags,
			SourceID:      e.SourceID,
		}
	}

	body, err := json.Marshal(payload{Stream: stream, Entries: pe})
	if err != nil {
		return nil, fmt.Errorf("%w: marshal payload: %v", ledger.ErrEncode, err)
	}

	if err := s.postWithRetry(ctx, body); err != nil {
		return nil, err
	}
	return ids, nil
}

// Deliver POSTs full hook entries (including IDs, annotations, and timestamps)
// to the webhook URL. Used by the daemon's hook runner to deliver complete
// StoredEntry data rather than the stripped-down RawEntry shape.
func (s *Sink) Deliver(ctx context.Context, stream string, entries []HookEntry) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
	}
	if len(entries) == 0 {
		return nil
	}
	body, err := json.Marshal(hookPayload{Stream: stream, Entries: entries})
	if err != nil {
		return fmt.Errorf("%w: marshal hook payload: %v", ledger.ErrEncode, err)
	}
	return s.postWithRetry(ctx, body)
}

func (s *Sink) postWithRetry(ctx context.Context, body []byte) error {
	policy := s.opts.retry
	var lastErr error
	for attempt := 0; attempt < policy.MaxAttempts; attempt++ {
		if attempt > 0 {
			if err := sleep(ctx, backoffDelay(policy, attempt-1)); err != nil {
				return err
			}
		}
		err := s.post(ctx, body)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetryable(err) {
			return err
		}
	}
	return fmt.Errorf("webhook: delivery failed after %d attempts: %w", policy.MaxAttempts, lastErr)
}

// isRetryable reports whether a delivery error warrants another attempt.
// Transport errors are always retried. Among HTTP status errors, only
// 408, 425, 429, and 5xx are retried; all other 4xx are permanent.
type httpStatusError struct{ code int }

func (e httpStatusError) Error() string { return fmt.Sprintf("webhook: server returned %d", e.code) }

func isRetryable(err error) bool {
	var se httpStatusError
	if !errors.As(err, &se) {
		// Network / transport error — always retry.
		return true
	}
	switch se.code {
	case http.StatusRequestTimeout,     // 408
		425,                            // Too Early
		http.StatusTooManyRequests,     // 429
		http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
		http.StatusGatewayTimeout:      // 504
		return true
	default:
		return false
	}
}

func (s *Sink) post(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("webhook: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range s.opts.headers {
		req.Header.Set(k, v)
	}

	resp, err := s.opts.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook: http: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return httpStatusError{code: resp.StatusCode}
	}
	return nil
}

// Read is not supported; returns [ledger.ErrNotSupported].
func (s *Sink) Read(_ context.Context, _ string, _ ...ledger.ReadOption) ([]ledger.StoredEntry[string, json.RawMessage], error) {
	return nil, ledger.ErrNotSupported
}

// Count is not supported; returns [ledger.ErrNotSupported].
func (s *Sink) Count(_ context.Context, _ string) (int64, error) {
	return 0, ledger.ErrNotSupported
}

// Stat is not supported; returns [ledger.ErrNotSupported].
func (s *Sink) Stat(_ context.Context, _ string) (ledger.StreamStat[string], error) {
	return ledger.StreamStat[string]{}, ledger.ErrNotSupported
}

// SetTags is not supported; returns [ledger.ErrNotSupported].
func (s *Sink) SetTags(_ context.Context, _ string, _ string, _ []string) error {
	return ledger.ErrNotSupported
}

// SetAnnotations is not supported; returns [ledger.ErrNotSupported].
func (s *Sink) SetAnnotations(_ context.Context, _ string, _ string, _ map[string]*string) error {
	return ledger.ErrNotSupported
}

// Trim is not supported; returns [ledger.ErrNotSupported].
func (s *Sink) Trim(_ context.Context, _ string, _ string) (int64, error) {
	return 0, ledger.ErrNotSupported
}

// ListStreamIDs is not supported; returns [ledger.ErrNotSupported].
func (s *Sink) ListStreamIDs(_ context.Context, _ ...ledger.ListOption) ([]string, error) {
	return nil, ledger.ErrNotSupported
}

// Close marks the sink as closed, preventing further deliveries.
func (s *Sink) Close(_ context.Context) error {
	s.closed.Store(true)
	return nil
}
