# ledger

[![CI](https://github.com/rbaliyan/ledger/actions/workflows/ci.yml/badge.svg)](https://github.com/rbaliyan/ledger/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/rbaliyan/ledger.svg)](https://pkg.go.dev/github.com/rbaliyan/ledger)
[![Go Report Card](https://goreportcard.com/badge/github.com/rbaliyan/ledger)](https://goreportcard.com/report/github.com/rbaliyan/ledger)
[![Release](https://img.shields.io/github/v/release/rbaliyan/ledger)](https://github.com/rbaliyan/ledger/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/rbaliyan/ledger/badge)](https://scorecard.dev/viewer/?uri=github.com/rbaliyan/ledger)

Append-only log library for Go with typed generic entries, schema versioning, and pluggable storage backends.

## Features

- **Generic typed streams** — `Stream[I, T]` provides compile-time type safety for entry payloads
- **Lightweight streams** — create per operation and discard, no lifecycle management
- **Schema versioning** — entries are stamped with a version; upcasters transform old entries on read
- **Deduplication** — storage-level dedup via partial unique indexes, silently skips duplicates
- **Ordering keys** — filter entries by an ordering key (e.g., aggregate ID)
- **Cursor-based pagination** — efficient reads with `After(id)`, `Limit(n)`, `Desc()`
- **Retention management** — `Trim(ctx, stream, beforeID)` for log compaction
- **Health checks** — all backends implement `HealthChecker` interface
- **Structured logging** — all backends log via `slog.Default()`; override with `WithLogger()`
- **Three backends** — SQLite, PostgreSQL, MongoDB

## Installation

```bash
go get github.com/rbaliyan/ledger
```

## Quick Start

```go
package main

import (
    "context"
    "database/sql"
    "fmt"

    "github.com/rbaliyan/ledger"
    "github.com/rbaliyan/ledger/sqlite"
    _ "modernc.org/sqlite"
)

type Order struct {
    ID     string  `json:"id"`
    Amount float64 `json:"amount"`
}

func main() {
    ctx := context.Background()

    // Open a store (once, shared across your application)
    db, _ := sql.Open("sqlite", "app.db")
    store, _ := sqlite.New(ctx, db)
    defer store.Close(ctx)

    // Create a lightweight stream (per operation, then discard)
    s := ledger.NewStream[int64, Order](store, "orders")

    // Append entries with ordering key, dedup key, and metadata
    ids, _ := s.Append(ctx, ledger.AppendInput[Order]{
        Payload:  Order{ID: "o-1", Amount: 99.99},
        OrderKey: "customer-123",
        DedupKey: "evt-abc",
        Metadata: map[string]string{"source": "api"},
    })
    fmt.Println("appended", len(ids), "entries")

    // Read entries (default limit is 100)
    entries, _ := s.Read(ctx)

    // Continue from where you left off
    if len(entries) > 0 {
        more, _ := s.Read(ctx, ledger.After(entries[len(entries)-1].ID), ledger.Limit(50))
        _ = more
    }

    // Filter by ordering key
    byCustomer, _ := s.Read(ctx, ledger.WithOrderKey("customer-123"))
    _ = byCustomer
}
```

## Schema Versioning

When your payload type evolves, register upcasters to transform old entries on read:

```go
type OrderV2 struct {
    Name   string  `json:"name"`
    Email  string  `json:"email"`
    Amount float64 `json:"amount"`
}

s := ledger.NewStream[int64, OrderV2](store, "orders",
    ledger.WithSchemaVersion(2),
    ledger.WithUpcaster(ledger.NewFieldMapper(1, 2).
        RenameField("customer_name", "name").
        AddDefault("email", "unknown@example.com")),
)

// Old v1 entries are automatically upcasted to v2 before decoding
entries, _ := s.Read(ctx)
```

## Custom Codec

Payloads are encoded with JSON by default. Provide a custom `Codec` implementation
for alternative formats (protobuf, msgpack, etc.):

```go
s := ledger.NewStream[int64, Order](store, "orders",
    ledger.WithCodec(myProtobufCodec{}),
)
```

## Backends

### SQLite

```go
import "github.com/rbaliyan/ledger/sqlite"

// db is a *sql.DB opened with a SQLite driver (e.g., modernc.org/sqlite)
store, err := sqlite.New(ctx, db,
    sqlite.WithTable("my_ledger"),
    sqlite.WithLogger(slog.Default()),
)
```

### PostgreSQL

```go
import "github.com/rbaliyan/ledger/postgres"

// db is a *sql.DB opened with a PostgreSQL driver (e.g., github.com/lib/pq)
store, err := postgres.New(ctx, db,
    postgres.WithTable("my_ledger"),
)
```

### MongoDB

```go
import "github.com/rbaliyan/ledger/mongodb"

// db is a *mongo.Database from an already-connected mongo.Client
store, err := mongodb.New(ctx, db,
    mongodb.WithCollection("my_ledger"),
)
```

**Atomicity note:** SQL backends (sqlite, postgres) use transactions for atomic batch inserts. MongoDB uses `InsertMany` with `ordered:false` — partial success is possible on non-dedup errors.

## Store Interface

```go
type Store[I comparable] interface {
    Append(ctx context.Context, stream string, entries ...RawEntry) ([]I, error)
    Read(ctx context.Context, stream string, opts ...ReadOption) ([]StoredEntry[I], error)
    Count(ctx context.Context, stream string) (int64, error)
    Trim(ctx context.Context, stream string, beforeID I) (int64, error)
    Close(ctx context.Context) error
}
```

ID types: `int64` for SQLite/PostgreSQL, `string` (hex ObjectID) for MongoDB.

Read defaults to ascending order with a limit of 100.

## Deduplication

Entries with a non-empty `DedupKey` are subject to per-stream dedup via a partial unique index. Duplicates are silently skipped. Empty `DedupKey` means no dedup — the entry is always appended.

## Metadata

Attach arbitrary key-value metadata to entries:

```go
s.Append(ctx, ledger.AppendInput[Order]{
    Payload:  order,
    Metadata: map[string]string{"trace_id": "abc123", "source": "api"},
})
```

Metadata is stored as JSON (SQL backends) or a BSON subdocument (MongoDB).

## Custom Backends

Implement the `Store[I]` interface and validate with the conformance test suite:

```go
import "github.com/rbaliyan/ledger/storetest"

func TestConformance(t *testing.T) {
    store := myBackend.New(ctx, db)
    storetest.RunStoreTests(t, store, ledger.After[int64])
}
```

## Testing

```bash
# Unit tests (SQLite only, no external deps)
go test ./...

# Integration tests (requires Docker)
just test-integration

# Individual backends
just test-sqlite
just test-pg
just test-mongo

# Benchmarks
just bench
```

## License

MIT
