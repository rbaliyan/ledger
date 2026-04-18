package server

import (
	"context"
	"fmt"
	"maps"
	"sync"

	"github.com/rbaliyan/ledger/ledgerpb"
	"google.golang.org/grpc/metadata"
)

// compile-time checks
var (
	_ ledgerpb.Backend       = (*muxBackend)(nil)
	_ ledgerpb.StreamRenamer = (*muxBackend)(nil)
)

// BackendFactory creates a new Backend for the named store (table/collection).
type BackendFactory func(ctx context.Context, name string) (ledgerpb.Backend, error)

// muxBackend dispatches gRPC calls to per-store backends selected via the
// x-ledger-store metadata header. Stream names are transparently resolved to
// internal UUIDs via the metadata store. Backends are lazily created and cached.
type muxBackend struct {
	factory  BackendFactory
	meta     *streamMetaStore
	mu       sync.RWMutex
	backends map[string]ledgerpb.Backend
}

// newMuxBackend returns a muxBackend that creates backends using factory and
// resolves stream names via meta.
func newMuxBackend(factory BackendFactory, meta *streamMetaStore) *muxBackend {
	return &muxBackend{
		factory:  factory,
		meta:     meta,
		backends: make(map[string]ledgerpb.Backend),
	}
}

// Close releases all open backends. Should be called after the gRPC server
// has stopped accepting new requests.
func (m *muxBackend) Close(ctx context.Context) {
	m.mu.Lock()
	backends := make(map[string]ledgerpb.Backend, len(m.backends))
	maps.Copy(backends, m.backends)
	m.backends = make(map[string]ledgerpb.Backend)
	m.mu.Unlock()
	_ = backends
}

// storeNameFromCtx extracts the x-ledger-store metadata header from ctx.
func storeNameFromCtx(ctx context.Context) (string, error) {
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

// getOrCreateBackend returns (creating if necessary) the Backend for storeName.
func (m *muxBackend) getOrCreateBackend(ctx context.Context, storeName string) (ledgerpb.Backend, error) {
	m.mu.RLock()
	b, ok := m.backends[storeName]
	m.mu.RUnlock()
	if ok {
		return b, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if b, ok = m.backends[storeName]; ok {
		return b, nil
	}
	var err error
	b, err = m.factory(ctx, storeName)
	if err != nil {
		return nil, fmt.Errorf("open store %q: %w", storeName, err)
	}
	m.backends[storeName] = b
	return b, nil
}

// backendAndID resolves the human-readable stream name to an internal UUID and
// returns the appropriate backend. The internal UUID is what the backend stores.
func (m *muxBackend) backendAndID(ctx context.Context, name string) (ledgerpb.Backend, string, error) {
	sn, err := storeNameFromCtx(ctx)
	if err != nil {
		return nil, "", err
	}
	b, err := m.getOrCreateBackend(ctx, sn)
	if err != nil {
		return nil, "", err
	}
	internalID, err := m.meta.resolveOrCreate(ctx, sn, name)
	if err != nil {
		return nil, "", err
	}
	return b, internalID, nil
}

func (m *muxBackend) Append(ctx context.Context, stream string, entries ...ledgerpb.InputEntry) ([]string, error) {
	b, internalID, err := m.backendAndID(ctx, stream)
	if err != nil {
		return nil, err
	}
	return b.Append(ctx, internalID, entries...)
}

func (m *muxBackend) Read(ctx context.Context, stream string, opts ledgerpb.ReadOptions) ([]ledgerpb.StoredEntry, error) {
	b, internalID, err := m.backendAndID(ctx, stream)
	if err != nil {
		return nil, err
	}
	entries, err := b.Read(ctx, internalID, opts)
	if err != nil {
		return nil, err
	}
	// Replace internal UUIDs in the stream field with the human-readable name.
	for i := range entries {
		entries[i].Stream = stream
	}
	return entries, nil
}

func (m *muxBackend) Count(ctx context.Context, stream string) (int64, error) {
	b, internalID, err := m.backendAndID(ctx, stream)
	if err != nil {
		return 0, err
	}
	return b.Count(ctx, internalID)
}

func (m *muxBackend) SetTags(ctx context.Context, stream, id string, tags []string) error {
	b, internalID, err := m.backendAndID(ctx, stream)
	if err != nil {
		return err
	}
	return b.SetTags(ctx, internalID, id, tags)
}

func (m *muxBackend) SetAnnotations(ctx context.Context, stream, id string, annotations map[string]*string) error {
	b, internalID, err := m.backendAndID(ctx, stream)
	if err != nil {
		return err
	}
	return b.SetAnnotations(ctx, internalID, id, annotations)
}

func (m *muxBackend) Trim(ctx context.Context, stream, beforeID string) (int64, error) {
	b, internalID, err := m.backendAndID(ctx, stream)
	if err != nil {
		return 0, err
	}
	return b.Trim(ctx, internalID, beforeID)
}

// ListStreamIDs returns the paginated list of human-readable stream names for
// the store in ctx. Names come from the metadata table, not the backend.
func (m *muxBackend) ListStreamIDs(ctx context.Context, after string, limit int) ([]string, error) {
	sn, err := storeNameFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return m.meta.listNames(ctx, sn, after, limit)
}

// RenameStream changes the human-readable name for a stream without touching
// its entries. Implements [ledgerpb.StreamRenamer].
func (m *muxBackend) RenameStream(ctx context.Context, oldName, newName string) error {
	sn, err := storeNameFromCtx(ctx)
	if err != nil {
		return err
	}
	return m.meta.rename(ctx, sn, oldName, newName)
}

func (m *muxBackend) Health(ctx context.Context) error {
	m.mu.RLock()
	backends := make(map[string]ledgerpb.Backend, len(m.backends))
	maps.Copy(backends, m.backends)
	m.mu.RUnlock()
	for name, b := range backends {
		if err := b.Health(ctx); err != nil {
			return fmt.Errorf("store %q: %w", name, err)
		}
	}
	return nil
}
