# ledger

[![CI](https://github.com/rbaliyan/ledger/actions/workflows/ci.yml/badge.svg)](https://github.com/rbaliyan/ledger/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/rbaliyan/ledger.svg)](https://pkg.go.dev/github.com/rbaliyan/ledger)
[![Go Report Card](https://goreportcard.com/badge/github.com/rbaliyan/ledger)](https://goreportcard.com/report/github.com/rbaliyan/ledger)
[![Release](https://img.shields.io/github/v/release/rbaliyan/ledger)](https://github.com/rbaliyan/ledger/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/rbaliyan/ledger/badge)](https://scorecard.dev/viewer/?uri=github.com/rbaliyan/ledger)

**ledger** is a high-performance, append-only log library for Go featuring typed generic streams, schema versioning, and pluggable storage backends. It also includes a standalone daemon with a gRPC API and a rich CLI for stream management.

## Key Features

- Multiple Backends — Native support for SQLite, PostgreSQL, MongoDB, and ClickHouse.
- Type Safety — Generic `Stream[I, T]` ensures compile-time safety for your entry payloads.
- Lightweight Streams — Create thousands of streams dynamically without configuration or lifecycle management.
- Schema Versioning — Built-in `Upcaster` and `FieldMapper` to evolve your data models safely over time.
- Pluggable Architecture — Easily implement custom backends or codecs (JSON, Protobuf, MsgPack).
- CLI & Daemon — Use it as a library in your Go apps or run it as a standalone service with the `ledger` CLI.
- Observability — First-class OpenTelemetry support for Traces and Metrics.
- Replication Bridge — Replicate mutations between different stores (e.g., Postgres to ClickHouse).
- Advanced Querying — Cursor-based pagination, ordering keys, and mutable tags/annotations for filtering.

---

## Core Concept: Store = type, Stream = instance

A **Store** maps to one table (SQL) or collection (MongoDB) and represents a single entity type (e.g., `orders`, `events`). A **Stream** is one instance within that type, identified by a string ID (e.g., `"user-123"`). Streams are implicit — they come into existence on first append and require no lifecycle management.

```go
// One store per entity type
ordersStore, _ := sqlite.New(ctx, db, sqlite.WithTable("orders"))

// One stream per instance — create on demand, discard after use
s, _ := ledger.NewStream(ordersStore, "user-123", ledger.JSONCodec[Order]{})
```

---

## Library Quick Start

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
    db, _ := sql.Open("sqlite", "app.db")

    // Open a store (maps to a table)
    ordersStore, _ := sqlite.New(ctx, db, sqlite.WithTable("orders"))
    defer ordersStore.Close(ctx)

    // Create a typed stream for a specific user — codec is required
    s, err := ledger.NewStream(ordersStore, "user-123", ledger.JSONCodec[Order]{})
    if err != nil {
        panic(err)
    }

    // Append an entry
    ids, _ := s.Append(ctx, ledger.AppendInput[Order]{
        Payload:  Order{ID: "o-1", Amount: 99.99},
        OrderKey: "customer-123",
        Tags:     []string{"pending"},
    })
    fmt.Printf("Appended entry ID: %v\n", ids[0])

    // Read entries
    entries, _ := s.Read(ctx, ledger.Limit(10))
    for _, e := range entries {
        fmt.Printf("[%d] Order: %s, Amount: %.2f\n", e.ID, e.Payload.ID, e.Payload.Amount)
    }
}
```

---

## CLI & Daemon

The `ledger` CLI can run as a background daemon (SQLite or PostgreSQL) and provides subcommands for all stream operations.

### Installation

```bash
go install github.com/rbaliyan/ledger/cmd/ledger@latest
```

### Running the Daemon

```bash
# Start in the background — creates ~/.ledger/config.yaml and ~/.ledger/ledger.db on first run
ledger start

# Start in the foreground with an explicit config file
ledger start --foreground --config /etc/ledger/config.yaml

# Check status
ledger status

# Stop
ledger stop
```

On first run, `ledger start` creates `~/.ledger/config.yaml` (fully annotated) and
`~/.ledger/ledger.db` (SQLite) and listens on `localhost:50051`.

**API key protection** — set `api_key` in `config.yaml` and pass `--api-key <key>` on
every CLI call (or set it in the config file used by the client).

**TLS** — set `tls.cert`, `tls.key`, and optionally `tls.ca` (mutual TLS) in the
config. Pass the same paths with `--config` on the client side.

### Stream Subcommands

All stream subcommands accept `--store` (table/collection name, default `ledger_entries`)
and `--stream` (stream ID within the store, default `default`).

```bash
# Append entries
ledger stream append "plain text message"
ledger stream append --json '{"event":"login","user":"alice"}'
ledger stream append --stream orders --json '{"amount":42}' --tag pending --meta env=prod

# Read entries (plain text by default; --json for full entry JSON)
ledger stream read --stream orders
ledger stream read --stream orders --desc --limit 20 --tag pending
ledger stream read --stream orders --after 42 --order-key customer-1

# Search payloads (substring match; SQLite and PostgreSQL only via daemon)
ledger stream search "login"
ledger stream search --stream orders "failed"

# Count entries
ledger stream count --stream orders

# Stream list and stats
ledger stream list
ledger stream stat --stream orders

# Mutable tags and annotations
ledger stream tag   --stream orders --id 7 --tag shipped --tag paid
ledger stream annotate --stream orders --id 7 --set tracking=1Z999 --set carrier=UPS
ledger stream annotate --stream orders --id 7 --set tracking=   # delete key

# Rename a stream (metadata-only; entries are unchanged)
ledger stream rename --stream orders --to fulfilled-orders

# Trim old entries
ledger stream trim --stream orders --before 100

# Tail (continuous polling)
ledger stream tail --stream orders --interval 1s
```

---

## Backend Support Matrix

| Backend | Go Library | CLI / Daemon | Search | Atomicity |
| :--- | :---: | :---: | :---: | :--- |
| **SQLite** | Yes | Yes | Yes | Transactional |
| **PostgreSQL** | Yes | Yes | Yes | Transactional |
| **MongoDB** | Yes | No | Yes | Batch (Partial) |
| **ClickHouse** | Yes | No | No | Append-only (Async Trim) |

MongoDB and ClickHouse are library-only backends; the daemon (`ledger start`) supports
only SQLite and PostgreSQL. Use the Go library directly to write to MongoDB or ClickHouse.

---

## Advanced Features

### Schema Evolution

Upcasters transparently migrate old entries when they are read back, without touching
stored data:

```go
type OrderV2 struct {
    Name   string  `json:"name"`
    Email  string  `json:"email"`
    Amount float64 `json:"amount"`
}

s, err := ledger.NewStream(store, "orders", ledger.JSONCodec[OrderV2]{},
    ledger.WithSchemaVersion[json.RawMessage](2),
    ledger.WithUpcaster(ledger.NewFieldMapper(1, 2).
        RenameField("customer_name", "name").
        AddDefault("email", "unknown@example.com")),
)
```

### OpenTelemetry Instrumentation

Enable tracing and metrics by wrapping your store:

```go
import "github.com/rbaliyan/ledger/otel"

instrumentedStore, _ := otel.WrapStore(baseStore,
    otel.WithTracesEnabled(true),
    otel.WithMetricsEnabled(true),
)
```

### Replication Bridge

The `bridge` package replicates mutations from one store to another — useful for CDC
(Change Data Capture) or syncing operational data into an analytics backend:

```go
import "github.com/rbaliyan/ledger/bridge"

// mutations is the source mutation log store (opened against the same DB as the source).
// sink is the ClickHouse (or other) destination store.
// bridge.Int64Codec{} encodes the source int64 IDs for cursor storage.
b, err := bridge.New(mutations, sinkStore, bridge.Int64Codec{},
    bridge.WithSkipMutationTypes(bridge.MutationSetTags, bridge.MutationSetAnnotations),
)
if err != nil {
    panic(err)
}
b.Start(ctx)
```

---

## Development & Testing

```bash
# Run unit tests
go test ./...

# Run integration tests (requires Docker)
just test-integration

# Run benchmarks
just bench
```

## License

MIT
