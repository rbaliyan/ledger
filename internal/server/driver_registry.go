package server

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/rbaliyan/ledger/internal/config"
	"github.com/rbaliyan/ledger/ledgerpb"
)

// ProviderFactory creates a [ledgerpb.Provider] for a given store name
// (a table or collection).
type ProviderFactory func(ctx context.Context, name string) (ledgerpb.Provider, error)

// DriverResources holds the resources opened by a [Driver].
// The caller is responsible for calling Closer.Close when done.
type DriverResources struct {
	Meta    metaStore
	Factory ProviderFactory
	Closer  io.Closer
}

// Driver opens a database connection, constructs its metadata store,
// and returns the resources needed to serve requests.
type Driver func(ctx context.Context, cfg *config.Config) (DriverResources, error)

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]Driver)
)

// RegisterDriver registers a driver for a database type. It is intended
// to be called from a package init() so drivers self-register. Panics on
// duplicate registration — init() code is expected to catch this early.
func RegisterDriver(dbType string, fn Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if _, dup := drivers[dbType]; dup {
		panic(fmt.Sprintf("server: RegisterDriver called twice for %q", dbType))
	}
	drivers[dbType] = fn
}

// registeredDrivers returns the sorted list of registered database type names.
// Useful for config validation and error messages.
func registeredDrivers() []string {
	driversMu.RLock()
	defer driversMu.RUnlock()
	names := make([]string, 0, len(drivers))
	for name := range drivers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// openDriver resolves the Driver for cfg.DB.Type and invokes it.
func openDriver(ctx context.Context, cfg *config.Config) (DriverResources, error) {
	driversMu.RLock()
	fn, ok := drivers[cfg.DB.Type]
	driversMu.RUnlock()
	if !ok {
		return DriverResources{}, fmt.Errorf("server: unsupported database type %q (registered: %v)", cfg.DB.Type, registeredDrivers())
	}
	return fn(ctx, cfg)
}
