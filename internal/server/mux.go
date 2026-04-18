package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/rbaliyan/ledger/ledgerpb"
	"google.golang.org/grpc/metadata"
)

// BackendFactory creates a new Backend for the named store (table/collection).
type BackendFactory func(ctx context.Context, name string) (ledgerpb.Backend, error)

// muxBackend dispatches gRPC calls to per-store backends selected via the
// x-ledger-store metadata header. Backends are lazily created and cached.
type muxBackend struct {
	factory BackendFactory
	mu      sync.RWMutex
	backends map[string]ledgerpb.Backend
}

// newMuxBackend returns a muxBackend that creates backends using factory.
func newMuxBackend(factory BackendFactory) *muxBackend {
	return &muxBackend{
		factory:  factory,
		backends: make(map[string]ledgerpb.Backend),
	}
}

// storeName extracts the x-ledger-store metadata header from ctx.
func storeName(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", fmt.Errorf("missing gRPC metadata")
	}
	vals := md.Get("x-ledger-store")
	if len(vals) == 0 || vals[0] == "" {
		return "", fmt.Errorf("missing x-ledger-store metadata header")
	}
	return vals[0], nil
}

// backend returns (creating if necessary) the Backend for the store named in ctx.
func (m *muxBackend) backend(ctx context.Context) (ledgerpb.Backend, error) {
	name, err := storeName(ctx)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	b, ok := m.backends[name]
	m.mu.RUnlock()
	if ok {
		return b, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if b, ok = m.backends[name]; ok {
		return b, nil
	}
	b, err = m.factory(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("open store %q: %w", name, err)
	}
	m.backends[name] = b
	return b, nil
}

func (m *muxBackend) Append(ctx context.Context, stream string, entries ...ledgerpb.InputEntry) ([]string, error) {
	b, err := m.backend(ctx)
	if err != nil {
		return nil, err
	}
	return b.Append(ctx, stream, entries...)
}

func (m *muxBackend) Read(ctx context.Context, stream string, opts ledgerpb.ReadOptions) ([]ledgerpb.StoredEntry, error) {
	b, err := m.backend(ctx)
	if err != nil {
		return nil, err
	}
	return b.Read(ctx, stream, opts)
}

func (m *muxBackend) Count(ctx context.Context, stream string) (int64, error) {
	b, err := m.backend(ctx)
	if err != nil {
		return 0, err
	}
	return b.Count(ctx, stream)
}

func (m *muxBackend) SetTags(ctx context.Context, stream, id string, tags []string) error {
	b, err := m.backend(ctx)
	if err != nil {
		return err
	}
	return b.SetTags(ctx, stream, id, tags)
}

func (m *muxBackend) SetAnnotations(ctx context.Context, stream, id string, annotations map[string]*string) error {
	b, err := m.backend(ctx)
	if err != nil {
		return err
	}
	return b.SetAnnotations(ctx, stream, id, annotations)
}

func (m *muxBackend) Trim(ctx context.Context, stream, beforeID string) (int64, error) {
	b, err := m.backend(ctx)
	if err != nil {
		return 0, err
	}
	return b.Trim(ctx, stream, beforeID)
}

func (m *muxBackend) ListStreamIDs(ctx context.Context, after string, limit int) ([]string, error) {
	b, err := m.backend(ctx)
	if err != nil {
		return nil, err
	}
	return b.ListStreamIDs(ctx, after, limit)
}

func (m *muxBackend) Health(ctx context.Context) error {
	// Health check all open backends.
	m.mu.RLock()
	backends := make(map[string]ledgerpb.Backend, len(m.backends))
	for k, v := range m.backends {
		backends[k] = v
	}
	m.mu.RUnlock()
	for name, b := range backends {
		if err := b.Health(ctx); err != nil {
			return fmt.Errorf("store %q: %w", name, err)
		}
	}
	return nil
}
