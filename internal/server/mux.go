package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"sync"

	"github.com/rbaliyan/ledger"
	"github.com/rbaliyan/ledger/ledgerpb"
	"google.golang.org/grpc/metadata"
)

// compile-time checks
var (
	_ ledgerpb.Provider         = (*muxProvider)(nil)
	_ ledgerpb.StreamRenamer    = (*muxProvider)(nil)
	_ ledgerpb.ProviderSearcher = (*muxProvider)(nil)
)

// muxProvider dispatches gRPC calls to per-store backends selected via the
// x-ledger-store metadata header. Stream names are transparently resolved to
// internal UUIDs via the metadata store. Backends are lazily created and cached.
type muxProvider struct {
	factory  ProviderFactory
	meta     metaStore
	mu       sync.RWMutex
	backends map[string]ledgerpb.Provider
	closed   bool
}

// newMuxProvider returns a muxProvider that creates backends using factory and
// resolves stream names via meta.
func newMuxProvider(factory ProviderFactory, meta metaStore) *muxProvider {
	return &muxProvider{
		factory:  factory,
		meta:     meta,
		backends: make(map[string]ledgerpb.Provider),
	}
}

// Close closes all cached backends that implement io.Closer and prevents
// creation of new ones. Should be called after the gRPC server has stopped
// accepting new requests.
func (m *muxProvider) Close(ctx context.Context) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	backends := m.backends
	m.backends = nil
	m.mu.Unlock()

	for name, b := range backends {
		if c, ok := b.(io.Closer); ok {
			if err := c.Close(); err != nil {
				slog.Warn("close backend", "store", name, "err", err)
			}
		}
	}
}

// storeNameFromCtx extracts the store metadata header from ctx.
func storeNameFromCtx(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", fmt.Errorf("missing gRPC metadata")
	}
	vals := md.Get(ledgerpb.StoreMetadataHeader)
	if len(vals) == 0 || vals[0] == "" {
		return "", fmt.Errorf("missing %s metadata header", ledgerpb.StoreMetadataHeader)
	}
	return vals[0], nil
}

// getOrCreateBackend returns (creating if necessary) the Provider for storeName.
// Returns an error when the mux has been closed.
func (m *muxProvider) getOrCreateBackend(ctx context.Context, storeName string) (ledgerpb.Provider, error) {
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return nil, ledger.ErrStoreClosed
	}
	b, ok := m.backends[storeName]
	m.mu.RUnlock()
	if ok {
		return b, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ledger.ErrStoreClosed
	}
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

// backendForAppend resolves the human-readable stream name to an internal UUID,
// creating a mapping if one does not exist. Used only by [muxProvider.Append] so
// read-only operations do not pollute the metadata table with phantom streams.
func (m *muxProvider) backendForAppend(ctx context.Context, name string) (ledgerpb.Provider, string, error) {
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

// backendForRead resolves the human-readable stream name to an internal UUID
// without creating a mapping. Returns [ledger.ErrStreamNotFound] if the stream
// has never been written to.
func (m *muxProvider) backendForRead(ctx context.Context, name string) (ledgerpb.Provider, string, error) {
	sn, err := storeNameFromCtx(ctx)
	if err != nil {
		return nil, "", err
	}
	b, err := m.getOrCreateBackend(ctx, sn)
	if err != nil {
		return nil, "", err
	}
	internalID, ok, err := m.meta.resolve(ctx, sn, name)
	if err != nil {
		return nil, "", err
	}
	if !ok {
		return nil, "", fmt.Errorf("%w: %q in store %q", ledger.ErrStreamNotFound, name, sn)
	}
	return b, internalID, nil
}

func (m *muxProvider) Append(ctx context.Context, stream string, entries ...ledgerpb.InputEntry) ([]string, error) {
	b, internalID, err := m.backendForAppend(ctx, stream)
	if err != nil {
		return nil, err
	}
	return b.Append(ctx, internalID, entries...)
}

func (m *muxProvider) Read(ctx context.Context, stream string, opts ledgerpb.ReadOptions) ([]ledgerpb.StoredEntry, error) {
	b, internalID, err := m.backendForRead(ctx, stream)
	if err != nil {
		if errors.Is(err, ledger.ErrStreamNotFound) {
			return nil, nil
		}
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

func (m *muxProvider) Count(ctx context.Context, stream string) (int64, error) {
	b, internalID, err := m.backendForRead(ctx, stream)
	if err != nil {
		if errors.Is(err, ledger.ErrStreamNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return b.Count(ctx, internalID)
}

func (m *muxProvider) Stat(ctx context.Context, stream string) (ledgerpb.StreamStat, error) {
	b, internalID, err := m.backendForRead(ctx, stream)
	if err != nil {
		if errors.Is(err, ledger.ErrStreamNotFound) {
			return ledgerpb.StreamStat{Stream: stream}, nil
		}
		return ledgerpb.StreamStat{}, err
	}
	stat, err := b.Stat(ctx, internalID)
	if err != nil {
		return ledgerpb.StreamStat{}, err
	}
	stat.Stream = stream // return human-readable name
	return stat, nil
}

func (m *muxProvider) SetTags(ctx context.Context, stream, id string, tags []string) error {
	b, internalID, err := m.backendForRead(ctx, stream)
	if err != nil {
		return err
	}
	return b.SetTags(ctx, internalID, id, tags)
}

func (m *muxProvider) SetAnnotations(ctx context.Context, stream, id string, annotations map[string]*string) error {
	b, internalID, err := m.backendForRead(ctx, stream)
	if err != nil {
		return err
	}
	return b.SetAnnotations(ctx, internalID, id, annotations)
}

func (m *muxProvider) Trim(ctx context.Context, stream, beforeID string) (int64, error) {
	b, internalID, err := m.backendForRead(ctx, stream)
	if err != nil {
		if errors.Is(err, ledger.ErrStreamNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return b.Trim(ctx, internalID, beforeID)
}

// ListStreamIDs returns the paginated list of human-readable stream names for
// the store in ctx. Names come from the metadata table, not the backend.
func (m *muxProvider) ListStreamIDs(ctx context.Context, after string, limit int) ([]string, error) {
	sn, err := storeNameFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return m.meta.listNames(ctx, sn, after, limit)
}

// RenameStream changes the human-readable name for a stream without touching
// its entries. Implements [ledgerpb.StreamRenamer].
func (m *muxProvider) RenameStream(ctx context.Context, oldName, newName string) error {
	sn, err := storeNameFromCtx(ctx)
	if err != nil {
		return err
	}
	return m.meta.rename(ctx, sn, oldName, newName)
}

// Search implements [ledgerpb.ProviderSearcher]. It resolves the stream name
// to its internal UUID when non-empty, then delegates to the underlying
// provider's ProviderSearcher. Returns [ledger.ErrNotSupported] if the backend
// does not implement search.
func (m *muxProvider) Search(ctx context.Context, stream string, query string, opts ledgerpb.ReadOptions) ([]ledgerpb.StoredEntry, error) {
	sn, err := storeNameFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	b, err := m.getOrCreateBackend(ctx, sn)
	if err != nil {
		return nil, err
	}
	searcher, ok := b.(ledgerpb.ProviderSearcher)
	if !ok {
		return nil, ledger.ErrNotSupported
	}
	internalStream := stream
	if stream != "" {
		id, ok, err := m.meta.resolve(ctx, sn, stream)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil // unknown stream → empty results
		}
		internalStream = id
	}
	entries, err := searcher.Search(ctx, internalStream, query, opts)
	if err != nil {
		return nil, err
	}
	for i := range entries {
		entries[i].Stream = stream
	}
	return entries, nil
}

// providerFor returns the Provider for storeName, creating it if necessary.
// Unlike the gRPC path, it does not read the store name from context metadata.
// Used by the hook runner.
func (m *muxProvider) providerFor(ctx context.Context, storeName string) (ledgerpb.Provider, error) {
	return m.getOrCreateBackend(ctx, storeName)
}

func (m *muxProvider) Health(ctx context.Context) error {
	m.mu.RLock()
	backends := make(map[string]ledgerpb.Provider, len(m.backends))
	maps.Copy(backends, m.backends)
	m.mu.RUnlock()
	for name, b := range backends {
		if err := b.Health(ctx); err != nil {
			return fmt.Errorf("store %q: %w", name, err)
		}
	}
	return nil
}
