package server

// This file wires the built-in drivers into the registry. Drivers that
// require additional imports register themselves here so the daemon binary
// stays minimal when a driver is not used.

import (
	"context"

	_ "github.com/lib/pq"  // postgres driver
	_ "modernc.org/sqlite" // sqlite driver

	"github.com/rbaliyan/ledger/internal/config"
	"github.com/rbaliyan/ledger/ledgerpb"
	"github.com/rbaliyan/ledger/postgres"
	"github.com/rbaliyan/ledger/sqlite"
)

func init() {
	RegisterDriver("sqlite", sqliteDriver)
	RegisterDriver("postgres", postgresDriver)
}

func sqliteDriver(ctx context.Context, cfg *config.Config) (DriverResources, error) {
	db, err := openDB(ctx, "sqlite", cfg.DB.SQLite.Path)
	if err != nil {
		return DriverResources{}, err
	}
	meta, err := newSQLMetaStore(ctx, db, "sqlite")
	if err != nil {
		_ = db.Close()
		return DriverResources{}, err
	}
	factory := func(ctx context.Context, name string) (ledgerpb.Provider, error) {
		store, err := sqlite.New(ctx, db, sqlite.WithTable(name))
		if err != nil {
			return nil, err
		}
		return ledgerpb.NewInt64Provider(store), nil
	}
	return DriverResources{Meta: meta, Factory: factory, Closer: db}, nil
}

func postgresDriver(ctx context.Context, cfg *config.Config) (DriverResources, error) {
	db, err := openDB(ctx, "postgres", cfg.DB.Postgres.DSN)
	if err != nil {
		return DriverResources{}, err
	}
	meta, err := newSQLMetaStore(ctx, db, "postgres")
	if err != nil {
		_ = db.Close()
		return DriverResources{}, err
	}
	factory := func(ctx context.Context, name string) (ledgerpb.Provider, error) {
		store, err := postgres.New(ctx, db, postgres.WithTable(name))
		if err != nil {
			return nil, err
		}
		return ledgerpb.NewInt64Provider(store), nil
	}
	return DriverResources{Meta: meta, Factory: factory, Closer: db}, nil
}
