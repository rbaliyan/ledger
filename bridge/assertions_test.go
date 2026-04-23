package bridge

import (
	"github.com/rbaliyan/ledger/clickhouse"
	"github.com/rbaliyan/ledger/mongodb"
	"github.com/rbaliyan/ledger/postgres"
	"github.com/rbaliyan/ledger/sqlite"
)

// Compile-time checks: all backends satisfy sinkLookup without importing bridge.
var (
	_ sinkLookup[int64]  = (*sqlite.Store)(nil)
	_ sinkLookup[int64]  = (*postgres.Store)(nil)
	_ sinkLookup[string] = (*mongodb.Store)(nil)
	_ sinkLookup[string] = (*clickhouse.Store)(nil)
)
