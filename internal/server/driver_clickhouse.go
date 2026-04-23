package server

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/ClickHouse/clickhouse-go/v2" // register "clickhouse" driver

	"github.com/rbaliyan/ledger/clickhouse"
	"github.com/rbaliyan/ledger/internal/config"
	"github.com/rbaliyan/ledger/ledgerpb"
)

func init() { RegisterDriver("clickhouse", clickhouseDriver) }

func clickhouseDriver(ctx context.Context, cfg *config.Config) (DriverResources, error) {
	db, err := sql.Open("clickhouse", cfg.DB.ClickHouse.DSN)
	if err != nil {
		return DriverResources{}, fmt.Errorf("server: clickhouse open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return DriverResources{}, fmt.Errorf("server: clickhouse ping: %w", err)
	}

	meta, err := newCHMetaStore(ctx, db)
	if err != nil {
		_ = db.Close()
		return DriverResources{}, fmt.Errorf("server: clickhouse meta store: %w", err)
	}

	factory := func(ctx context.Context, name string) (ledgerpb.Provider, error) {
		store, err := clickhouse.New(ctx, db, clickhouse.WithTable(name))
		if err != nil {
			return nil, fmt.Errorf("open clickhouse store %q: %w", name, err)
		}
		return ledgerpb.NewStringProvider(store), nil
	}

	return DriverResources{Meta: meta, Factory: factory, Closer: db}, nil
}
