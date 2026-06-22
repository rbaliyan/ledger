package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/ledger"
)

// zeroBackoff returns a RetryPolicy whose delays collapse to the minimum floor
// so retries happen near-instantly without sleeping for real durations.
func zeroBackoff(maxAttempts int) RetryPolicy {
	return RetryPolicy{MaxAttempts: maxAttempts, BaseDelay: 0, MaxDelay: time.Nanosecond}
}

func TestSink_Append_SuccessfulDelivery(t *testing.T) {
	var gotBody []byte
	var gotSig string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		gotBody = body
		gotSig = r.Header.Get("X-Ledger-Signature")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	signer := NewHMACSigner([]byte("secret"), "X-Ledger-Signature", nil)
	sink, err := New(srv.URL, WithHTTPClient(&http.Client{Transport: signer}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ids, err := sink.Append(context.Background(), "orders",
		ledger.RawEntry[json.RawMessage]{Payload: json.RawMessage(`{"x":1}`), SchemaVersion: 1})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(ids) != 1 || ids[0] == "" {
		t.Fatalf("Append ids = %v, want one non-empty id", ids)
	}

	// The delivered body must be a parseable payload referencing the stream.
	var p struct {
		Stream  string `json:"stream"`
		Entries []struct {
			Payload json.RawMessage `json:"payload"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(gotBody, &p); err != nil {
		t.Fatalf("delivered body not valid JSON: %v (body=%q)", err, gotBody)
	}
	if p.Stream != "orders" {
		t.Errorf("delivered stream = %q, want orders", p.Stream)
	}
	if len(p.Entries) != 1 || string(p.Entries[0].Payload) != `{"x":1}` {
		t.Errorf("delivered entries = %+v, want one entry with payload {\"x\":1}", p.Entries)
	}
	if gotSig == "" {
		t.Error("expected X-Ledger-Signature header on delivery, got empty")
	}
}

func TestSink_Append_RetriesOn5xx(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable) // 503 -> retryable
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink, err := New(srv.URL, WithRetryPolicy(zeroBackoff(5)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = sink.Append(context.Background(), "s",
		ledger.RawEntry[json.RawMessage]{Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("Append should eventually succeed after retries: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("server received %d calls, want 3 (two 503s then success)", got)
	}
}

func TestSink_Append_GivesUpAfterMaxAttempts(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError) // always 500 -> retryable
	}))
	defer srv.Close()

	sink, err := New(srv.URL, WithRetryPolicy(zeroBackoff(3)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = sink.Append(context.Background(), "s",
		ledger.RawEntry[json.RawMessage]{Payload: json.RawMessage(`{}`)})
	if err == nil {
		t.Fatal("expected error after exhausting retries, got nil")
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("server received %d calls, want 3 (MaxAttempts)", got)
	}
}

func TestSink_Append_NoRetryOn4xx(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest) // 400 -> permanent
	}))
	defer srv.Close()

	sink, err := New(srv.URL, WithRetryPolicy(zeroBackoff(5)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = sink.Append(context.Background(), "s",
		ledger.RawEntry[json.RawMessage]{Payload: json.RawMessage(`{}`)})
	if err == nil {
		t.Fatal("expected error on 4xx, got nil")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("server received %d calls, want 1 (no retry on permanent 4xx)", got)
	}
}

func TestSink_WithHTTPClientHonored(t *testing.T) {
	var used atomic.Bool
	// A custom client whose transport flips a flag, proving WithHTTPClient is used.
	custom := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		used.Store(true)
		return http.DefaultTransport.RoundTrip(r)
	})}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink, err := New(srv.URL, WithHTTPClient(custom))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := sink.Append(context.Background(), "s",
		ledger.RawEntry[json.RawMessage]{Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if !used.Load() {
		t.Error("custom HTTP client transport was not used")
	}
}

func TestSink_New_RejectsEmptyURL(t *testing.T) {
	if _, err := New(""); err == nil {
		t.Fatal("New(\"\") = nil error, want error")
	}
}

func TestSink_Append_AfterCloseReturnsErrStoreClosed(t *testing.T) {
	sink, err := New("http://example.test")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := sink.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = sink.Append(context.Background(), "s",
		ledger.RawEntry[json.RawMessage]{Payload: json.RawMessage(`{}`)})
	if err != ledger.ErrStoreClosed {
		t.Errorf("Append after Close = %v, want ErrStoreClosed", err)
	}
}

func TestSink_UnsupportedReadMethods(t *testing.T) {
	sink, err := New("http://example.test")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	if _, err := sink.Read(ctx, "s"); err != ledger.ErrNotSupported {
		t.Errorf("Read = %v, want ErrNotSupported", err)
	}
	if _, err := sink.Count(ctx, "s"); err != ledger.ErrNotSupported {
		t.Errorf("Count = %v, want ErrNotSupported", err)
	}
	if _, err := sink.Stat(ctx, "s"); err != ledger.ErrNotSupported {
		t.Errorf("Stat = %v, want ErrNotSupported", err)
	}
	if err := sink.SetTags(ctx, "s", "1", nil); err != ledger.ErrNotSupported {
		t.Errorf("SetTags = %v, want ErrNotSupported", err)
	}
	if err := sink.SetAnnotations(ctx, "s", "1", nil); err != ledger.ErrNotSupported {
		t.Errorf("SetAnnotations = %v, want ErrNotSupported", err)
	}
	if _, err := sink.Trim(ctx, "s", "1"); err != ledger.ErrNotSupported {
		t.Errorf("Trim = %v, want ErrNotSupported", err)
	}
	if _, err := sink.ListStreamIDs(ctx); err != ledger.ErrNotSupported {
		t.Errorf("ListStreamIDs = %v, want ErrNotSupported", err)
	}
}

func TestSink_Append_EmptyEntriesNoOp(t *testing.T) {
	sink, err := New("http://example.test")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ids, err := sink.Append(context.Background(), "s")
	if err != nil {
		t.Fatalf("Append with no entries = %v, want nil", err)
	}
	if ids != nil {
		t.Errorf("Append with no entries returned %v, want nil", ids)
	}
}

func TestSink_Deliver(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		gotBody = body
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink, err := New(srv.URL, WithRetryPolicy(zeroBackoff(3)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	err = sink.Deliver(context.Background(), "s", []HookEntry{
		{ID: "abc", Payload: json.RawMessage(`{"k":1}`), SchemaVersion: 1},
	})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	var p struct {
		Stream  string `json:"stream"`
		Entries []struct {
			ID string `json:"id"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(gotBody, &p); err != nil {
		t.Fatalf("delivered body not valid JSON: %v (body=%q)", err, gotBody)
	}
	if p.Stream != "s" || len(p.Entries) != 1 || p.Entries[0].ID != "abc" {
		t.Errorf("delivered hook payload = %+v, want stream=s entry id=abc", p)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
