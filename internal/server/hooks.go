package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/rbaliyan/ledger/internal/config"
	"github.com/rbaliyan/ledger/ledgerpb"
	"github.com/rbaliyan/ledger/webhook"
)

// hookRunner polls a store and delivers new entries to a configured webhook URL.
// Each hookRunner owns a background goroutine that is started via Start and
// terminates when the context passed to Start is cancelled.
//
// Per-stream cursors are stored in memory and reset on daemon restart, causing
// re-delivery from the beginning of every stream. Consumers should use the
// entry DedupKey or treat deliveries idempotently to handle this safely.
type hookRunner struct {
	name      string
	storeName string
	stream    string // empty = all streams
	mux       *muxProvider
	sink      *webhook.Sink
	interval  time.Duration

	mu      sync.Mutex
	cursors map[string]string // stream ID → last delivered entry ID

	stopped chan struct{}
}

// newHookRunner constructs a hookRunner from cfg. Returns an error if any
// required field is missing, the interval is unparseable, or the Sink cannot
// be created (e.g. invalid URL or TLS config).
func newHookRunner(cfg config.HookConfig, mux *muxProvider) (*hookRunner, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("hook: name must not be empty")
	}
	if cfg.Store == "" {
		return nil, fmt.Errorf("hook %q: store must not be empty", cfg.Name)
	}
	if cfg.URL == "" {
		return nil, fmt.Errorf("hook %q: url must not be empty", cfg.Name)
	}

	interval := 5 * time.Second
	if cfg.Interval != "" {
		d, err := time.ParseDuration(cfg.Interval)
		if err != nil {
			return nil, fmt.Errorf("hook %q: invalid interval %q: %w", cfg.Name, cfg.Interval, err)
		}
		if d > 0 {
			interval = d
		}
	}

	transport, err := buildHookTransport(cfg)
	if err != nil {
		return nil, fmt.Errorf("hook %q: tls: %w", cfg.Name, err)
	}

	httpClient := &http.Client{Transport: transport}
	if cfg.Secret != "" {
		hmacTransport := webhook.NewHMACSigner([]byte(cfg.Secret), "X-Ledger-Signature", transport)
		httpClient = &http.Client{Transport: hmacTransport}
	}

	var wOpts []webhook.Option
	wOpts = append(wOpts, webhook.WithHTTPClient(httpClient))
	if cfg.MaxRetries > 0 {
		wOpts = append(wOpts, webhook.WithRetryPolicy(webhook.RetryPolicy{
			MaxAttempts: cfg.MaxRetries,
			BaseDelay:   200 * time.Millisecond,
			MaxDelay:    30 * time.Second,
		}))
	}

	sink, err := webhook.New(cfg.URL, wOpts...)
	if err != nil {
		return nil, fmt.Errorf("hook %q: %w", cfg.Name, err)
	}

	return &hookRunner{
		name:      cfg.Name,
		storeName: cfg.Store,
		stream:    cfg.Stream,
		mux:       mux,
		sink:      sink,
		interval:  interval,
		cursors:   make(map[string]string),
		stopped:   make(chan struct{}),
	}, nil
}

// buildHookTransport constructs an http.RoundTripper from the hook's TLS config.
func buildHookTransport(cfg config.HookConfig) (http.RoundTripper, error) {
	tlsCfg := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify, //nolint:gosec // operator opt-in
	}
	if cfg.CA != "" {
		pem, err := os.ReadFile(cfg.CA) // #nosec G304 -- explicit config path
		if err != nil {
			return nil, fmt.Errorf("read CA %q: %w", cfg.CA, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("invalid CA certificate at %q", cfg.CA)
		}
		tlsCfg.RootCAs = pool
	}
	return &http.Transport{TLSClientConfig: tlsCfg}, nil
}

// Start launches the polling goroutine. The goroutine exits when ctx is cancelled.
func (h *hookRunner) Start(ctx context.Context) {
	go func() {
		defer close(h.stopped)
		t := time.NewTicker(h.interval)
		defer t.Stop()
		slog.Info("hook started", "hook", h.name, "store", h.storeName, "interval", h.interval)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := h.poll(ctx); err != nil {
					slog.Warn("hook poll failed", "hook", h.name, "err", err)
				}
			}
		}
	}()
}

// Wait blocks until the background goroutine has exited.
func (h *hookRunner) Wait() { <-h.stopped }

func (h *hookRunner) poll(ctx context.Context) error {
	provider, err := h.mux.providerFor(ctx, h.storeName)
	if err != nil {
		return fmt.Errorf("open store %q: %w", h.storeName, err)
	}

	var streams []string
	if h.stream != "" {
		streams = []string{h.stream}
	} else {
		after := ""
		for {
			batch, err := provider.ListStreamIDs(ctx, after, 100)
			if err != nil {
				return fmt.Errorf("list streams: %w", err)
			}
			streams = append(streams, batch...)
			if len(batch) < 100 {
				break
			}
			after = batch[len(batch)-1]
		}
	}

	for _, stream := range streams {
		if err := h.pollStream(ctx, provider, stream); err != nil {
			slog.Warn("hook stream poll failed",
				"hook", h.name, "store", h.storeName, "stream", stream, "err", err)
		}
	}

	// Prune cursors for streams that no longer appear in the store, preventing
	// unbounded growth when streams are renamed or deleted.
	if h.stream == "" && len(streams) > 0 {
		active := make(map[string]struct{}, len(streams))
		for _, s := range streams {
			active[s] = struct{}{}
		}
		h.mu.Lock()
		for s := range h.cursors {
			if _, ok := active[s]; !ok {
				delete(h.cursors, s)
			}
		}
		h.mu.Unlock()
	}

	return nil
}

func (h *hookRunner) pollStream(ctx context.Context, provider ledgerpb.Provider, stream string) error {
	h.mu.Lock()
	after := h.cursors[stream]
	h.mu.Unlock()

	entries, err := provider.Read(ctx, stream, ledgerpb.ReadOptions{After: after, Limit: 500})
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}

	hookEntries := make([]webhook.HookEntry, len(entries))
	for i, e := range entries {
		hookEntries[i] = webhook.HookEntry{
			ID:            e.ID,
			Payload:       e.Payload,
			OrderKey:      e.OrderKey,
			DedupKey:      e.DedupKey,
			SchemaVersion: e.SchemaVersion,
			Metadata:      e.Metadata,
			Tags:          e.Tags,
			Annotations:   e.Annotations,
			CreatedAt:     e.CreatedAt,
			UpdatedAt:     e.UpdatedAt,
		}
	}
	if err := h.sink.Deliver(ctx, stream, hookEntries); err != nil {
		return err
	}

	h.mu.Lock()
	h.cursors[stream] = entries[len(entries)-1].ID
	h.mu.Unlock()
	return nil
}
