# CLAUDE.md

This file provides guidance for AI assistants working on this codebase.

## Project Overview

Append-only log library for Go with typed generic entries, schema versioning, deduplication, and pluggable storage backends (SQLite, PostgreSQL, MongoDB, ClickHouse). Includes a gRPC API layer (`ledgerpb`) with pluggable authn/authz, a standalone daemon, and a CLI.

## Architecture

### Store = type, Stream = instance

- A **Store** represents one entity type — one table (SQL) or collection (Mongo). The table/collection name IS the type. Open one Store per type (e.g., `orders`, `users`) against the same database.
- A **Stream** is one instance within a type, identified by a string stream ID (e.g., `"user-123"`). Streams are implicit — created on first append, no lifecycle. `ListStreamIDs` enumerates the streams of a type.
- Cross-type queries are deliberately out of scope: they violate the "store = type" invariant. Users who need to correlate across types iterate one store at a time.

### Two-Level Generic Design

- **`Store[I comparable, P any]`** (`store.go`): Backend interface, generic over ID type `I` (int64 for SQL, string for MongoDB/ClickHouse) and store-native payload type `P` (json.RawMessage for SQL, bson.Raw for MongoDB).
- **`Stream[I comparable, P any, T any]`** (`stream.go`): Lightweight typed handle. Bridges user domain type `T` and store-native type `P` via `PayloadCodec[T, P]`. Applies schema upcasting on read.

### Package Structure

```
ledger/
├── store.go          # Store[I,P] interface, RawEntry[P], StoredEntry[I,P], ReadOptions, ListOptions, HealthChecker, Searcher[I,P]
├── stream.go         # Stream[I,P,T], Entry[I,T], AppendInput[T], Option[P]
├── codec.go          # PayloadCodec[T,P] interface, JSONCodec[T]
├── schema.go         # Upcaster[P] interface, FieldMapper (json.RawMessage), UpcasterFunc[P], upcastChain[P]
├── errors.go         # Sentinel errors (ErrStoreClosed, ErrStreamNotFound, ErrStreamExists, ErrNotSupported, …)
├── tx.go             # WithTx, TxFromContext — context-based external transactions
├── validate.go       # ValidateName for table/collection names
├── storetest/        # Backend-agnostic conformance test suite
│   └── storetest.go  # RunStoreTests[I,P](t, store, afterFn, cfg)
├── sqlite/           # SQLite backend — Store[int64, json.RawMessage]
│   └── store.go
├── postgres/         # PostgreSQL backend — Store[int64, json.RawMessage]
│   └── store.go
├── mongodb/          # MongoDB backend — Store[string, bson.Raw] + BSONCodec[T]
│   └── store.go
├── clickhouse/       # ClickHouse backend — Store[string, json.RawMessage]; SetTags/SetAnnotations unsupported
│   └── store.go
├── otel/             # Opt-in OpenTelemetry tracing + metrics wrapper
│   └── store.go      # WrapStore, WithTracesEnabled, WithMetricsEnabled
├── bridge/           # Replication: poll source mutation log → apply to sink store
│   └── bridge.go     # Bridge[SI,DI], New, IDCodec, WithSkipMutationTypes
├── proto/            # Protobuf definitions
│   └── ledger/v1/
│       └── ledger.proto
├── api/              # Generated protobuf Go code (committed, regenerate with `just proto`)
│   └── ledger/v1/
│       ├── ledger.pb.go
│       └── ledger_grpc.pb.go
├── ledgerpb/         # gRPC server, authn/authz, provider adapters
│   ├── server.go     # Server (implements LedgerServiceServer)
│   ├── security.go   # Identity, Decision, SecurityGuard interfaces
│   ├── interceptors.go # UnaryInterceptor, StreamInterceptor, HTTP Middleware
│   ├── provider.go   # Provider interface, ReadOptions, InputEntry, StoredEntry, StreamRenamer, ProviderSearcher
│   ├── adapter.go    # NewInt64Provider, NewStringProvider — adapt Store to Provider
│   └── errors.go     # toGRPCStatus — maps ledger errors to gRPC codes
├── cmd/              # ledger binary entry point
│   └── main.go
└── internal/
    ├── cli/          # Cobra commands: stream (append/read/search/count/list/stat/tag/annotate/trim/rename/tail), start/stop/status
    ├── config/       # Config struct, LoadFrom, Defaults — no discovery, explicit path only
    ├── daemon/       # PID file management (AcquirePID, ReadPID, IsAlive, RemovePID)
    └── server/       # gRPC server wiring: driver registry, muxProvider, streamMetaStore, apiKeyGuard
        ├── driver_registry.go  # RegisterDriver, DriverResources, openDriver
        ├── drivers.go          # init() registers sqlite and postgres drivers
        ├── meta.go             # streamMetaStore: human-readable name ↔ internal UUID mapping
        ├── mux.go              # muxProvider: routes calls to per-store backends, resolves stream names
        ├── guard.go            # apiKeyGuard: SecurityGuard using shared-secret API key
        └── server.go           # Server: New, Serve, Stop
```

### Key Design Patterns

- **Functional options**: `New(ctx, db, ...Option)` with unexported `options` structs
- **Compile-time checks**: `var _ ledger.Store[int64, json.RawMessage] = (*Store)(nil)`
- **Lightweight streams**: Value type (not pointer), create per operation, discard after use
- **Per-store native payload type**: MongoDB stores `bson.Raw` (embedded BSON doc, not binary); SQL stores `json.RawMessage` (native JSONB in Postgres, JSON text in SQLite)
- **PayloadCodec[T, P]**: Required 3rd argument to `NewStream` — bridges user type `T` and store type `P`. `JSONCodec[T]` for SQL, `mongodb.BSONCodec[T]` for MongoDB.
- **Schema versioning**: `SchemaVersion` stored per-entry as column/field, `Upcaster[P]` applied on read
- **Dedup**: Partial unique index on `(stream, dedup_key) WHERE dedup_key != ''`
- **Cursor-based pagination**: `After[I](id)` + `Limit(n)` (default 100) + `Desc()`
- **Metadata**: `map[string]string` on all entry types, stored as JSON (SQL) or BSON subdocument (MongoDB)
- **Optional interfaces**: `Searcher[I,P]`, `HealthChecker`, `CursorStore`, `SourceIDLookup` — type-assert to check capability

### Stream Construction

```go
// SQL backends (sqlite, postgres) — codec is required, not optional
s, err := ledger.NewStream(store, "user-123", ledger.JSONCodec[MyType]{})

// MongoDB
s, err := ledger.NewStream(store, "user-123", mongodb.BSONCodec[MyType]{})

// With schema versioning
s, err := ledger.NewStream(store, "user-123", ledger.JSONCodec[MyType]{},
    ledger.WithSchemaVersion[json.RawMessage](2),
    ledger.WithUpcaster(ledger.NewFieldMapper(1, 2).RenameField("old", "new")),
)
```

`NewStream` returns `(Stream[I,P,T], error)` — it returns an error (never panics) if the
store or codec is nil.

### Stream Options

- `WithSchemaVersion[P](v int)` — version stamped on new entries (default: 1)
- `WithUpcaster[P](u Upcaster[P])` — register version migration (v1→v2, v2→v3, etc.)

Note: `WithCodec` was removed — the codec is now a required positional argument to `NewStream`.

### Read Options

- `After[I](id)` — cursor-based pagination
- `Limit(n)` — max entries (default 100)
- `Desc()` — newest first
- `WithOrderKey(key)` — filter by ordering key
- `WithTag(tag)` / `WithAllTags(tags...)` — tag filters

### List Options (for `ListStreamIDs`)

- `ListAfter(streamID)` — cursor; returns IDs strictly greater
- `ListLimit(n)` — max IDs per page (default 100)

Note: `ListOption` is deliberately separate from `ReadOption` so the `ListAfter(string)` cursor can't be confused with `After[I](I)` (entry ID cursor).

### Store Interface

```go
type Store[I comparable, P any] interface {
    Append(ctx, stream, ...RawEntry[P]) ([]I, error)
    Read(ctx, stream, ...ReadOption) ([]StoredEntry[I, P], error)
    Count(ctx, stream) (int64, error)
    Stat(ctx, stream) (StreamStat[I], error)
    SetTags(ctx, stream, id, tags) error
    SetAnnotations(ctx, stream, id, annotations) error
    Trim(ctx, stream, beforeID) (int64, error)
    ListStreamIDs(ctx, ...ListOption) ([]string, error)
    Close(ctx) error
}
```

Each backend's concrete `*Store` also exposes `Type() string` returning its table/collection name (not on the interface — for logging/tracing).

### Optional Interfaces

- `HealthChecker`: `Health(ctx) error` — all backends implement this
- `Searcher[I,P]`: `Search(ctx, stream, query, ...ReadOption)` — SQLite, PostgreSQL, MongoDB
- `CursorStore`: `GetCursor` / `SetCursor` — ClickHouse (for bridge replication)
- `SourceIDLookup[I]`: `FindBySourceID` — ClickHouse (for bridge replication)

### Atomicity Note

SQL backends (sqlite, postgres) use transactions for atomic batch inserts. MongoDB uses `InsertMany` with `ordered:false` — partial success is possible on non-dedup errors.

## gRPC API (`ledgerpb`)

### Architecture

```
client
  │  (must set "x-ledger-store" metadata header on every request)
  ▼  gRPC (proto/ledger/v1/ledger.proto)
[UnaryInterceptor(SecurityGuard)] → authenticate + authorise
  ▼
[Server] → [Provider] interface (string IDs, json.RawMessage payloads)
  ▼
[NewInt64Provider(store)]   wraps Store[int64, json.RawMessage] (SQLite, PostgreSQL)
[NewStringProvider(store)]  wraps Store[string, json.RawMessage] (MongoDB)
```

The `x-ledger-store` header is read by `ledgerpb.StoreMetadataHeader` and
`x-api-key` by `ledgerpb.APIKeyMetadataHeader`.

### SecurityGuard

```go
type SecurityGuard interface {
    Authenticate(ctx context.Context) (Identity, error)
    Authorize(ctx context.Context, id Identity, resource, action string) (Decision, error)
}
```

- `resource` is always `"ledger"` in this library.
- `action` is the fully-qualified RPC method for gRPC (`"/ledger.v1.LedgerService/Append"`) or `"METHOD:PATH"` for HTTP middleware.

### Provider Adapters

- `NewInt64Provider(store)` — wraps `Store[int64, json.RawMessage]` (SQLite, PostgreSQL). Integer IDs serialised as decimal strings on the wire.
- `NewStringProvider(store)` — wraps `Store[string, json.RawMessage]` (MongoDB, ClickHouse). String IDs passed through.

### Proto → Go Code Generation

Generated code lives in `api/ledger/v1/` and is committed. Regenerate with:

```bash
just proto       # runs `buf generate`
just proto-lint  # runs `buf lint`
```

## Build Commands

```bash
go build ./...          # Build all packages (including ledgerpb)
go test ./...           # Unit tests (SQLite + ledgerpb gRPC tests)
just proto              # Regenerate protobuf Go code
just proto-lint         # Lint protobuf definitions
just test-integration   # All backends with Docker
just test-sqlite        # SQLite only
just test-pg            # PostgreSQL only
just test-mongo         # MongoDB only
just bench              # Benchmarks
just lint               # golangci-lint
```

## Tool Management (mise)

All tools are managed via `.mise.toml`:

```bash
mise install            # Install all tools
```

Key tools: `go`, `golangci-lint`, `buf`, `protoc-gen-go`, `protoc-gen-go-grpc`, `just`, `govulncheck`, `gosec`.

## Integration Tests

```bash
# Environment variables for manual testing
POSTGRES_DSN=postgres://ledger_test:ledger_test@localhost:5434/ledger_test?sslmode=disable
MONGO_URI=mongodb://localhost:27020/?directConnection=true
```

## Error Handling

- Sentinel errors checked with `errors.Is()`: `ErrStoreClosed`, `ErrEncode`, `ErrDecode`, `ErrNoUpcaster`, `ErrInvalidCursor`, `ErrInvalidName`, `ErrEntryNotFound`, `ErrStreamNotFound`, `ErrStreamExists`, `ErrNotSupported`
- Backend errors wrapped with context: `fmt.Errorf("ledger/sqlite: ...: %w", err)`
- gRPC errors mapped via `toGRPCStatus`: `ErrEntryNotFound` → `NotFound`, `ErrStoreClosed` → `Unavailable`, `ErrInvalidCursor` → `InvalidArgument`, `ErrStreamExists` → `AlreadyExists`, etc.

## Code Style

- `ctx context.Context` as first parameter
- Return value types, not pointers to value types
- Unexport internal types; only expose Option functions and interfaces
- `NewStream` returns `(Stream, error)` — returns an error on nil store or nil codec, never panics
