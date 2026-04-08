# CLAUDE.md

This file provides guidance for AI assistants working on this codebase.

## Project Overview

Append-only log library for Go with typed generic entries, schema versioning, deduplication, and pluggable storage backends (SQLite, PostgreSQL, MongoDB).

## Architecture

### Two-Level Generic Design

- **`Store[I comparable]`** (`store.go`): Backend interface, generic over ID type (int64 for SQL, string for MongoDB). Handles raw bytes.
- **`Stream[I comparable, T any]`** (`stream.go`): Lightweight typed handle. Encodes/decodes payloads via Codec, applies schema upcasting on read.

### Package Structure

```
ledger/
├── store.go          # Store interface, RawEntry, StoredEntry, ReadOptions, HealthChecker
├── stream.go         # Stream[I,T], Entry, AppendInput, options
├── codec.go          # Codec interface, JSONCodec
├── schema.go         # Upcaster interface, FieldMapper, UpcasterFunc, upcastChain
├── errors.go         # Sentinel errors
├── validate.go       # ValidateName for table/collection names
├── storetest/        # Backend-agnostic conformance test suite
│   └── storetest.go  # RunStoreTests[I](t, store, afterFn)
├── sqlite/           # SQLite backend (Store[int64])
│   └── store.go
├── postgres/         # PostgreSQL backend (Store[int64])
│   └── store.go
└── mongodb/          # MongoDB backend (Store[string])
    └── store.go
```

### Key Design Patterns

- **Functional options**: `New(ctx, db, ...Option)` with unexported `options` structs
- **Compile-time checks**: `var _ ledger.Store[int64] = (*Store)(nil)`
- **Lightweight streams**: Value type (not pointer), create per operation, discard after use
- **Schema versioning**: `SchemaVersion` stored per-entry as column/field, upcasters applied on read
- **Dedup**: Partial unique index on `(stream, dedup_key) WHERE dedup_key != ''`
- **Cursor-based pagination**: `After[I](id)` + `Limit(n)` (default 100) + `Desc()`
- **Metadata**: `map[string]string` on all entry types, stored as JSON (SQL) or BSON subdocument (MongoDB)

### Stream Options

- `WithCodec(c Codec)` — custom payload encoder/decoder (default: JSONCodec)
- `WithSchemaVersion(v int)` — version stamped on new entries (default: 1)
- `WithUpcaster(u Upcaster)` — register version migration (v1→v2, v2→v3, etc.)

### Read Options

- `After[I](id)` — cursor-based pagination
- `Limit(n)` — max entries (default 100)
- `Desc()` — newest first
- `WithOrderKey(key)` — filter by ordering key

### Store Interface

```go
type Store[I comparable] interface {
    Append(ctx, stream, ...RawEntry) ([]I, error)
    Read(ctx, stream, ...ReadOption) ([]StoredEntry[I], error)
    Count(ctx, stream) (int64, error)
    Trim(ctx, stream, beforeID) (int64, error)
    Close(ctx) error
}
```

### Optional Interfaces

- `HealthChecker`: `Health(ctx) error` — all backends implement this

### Atomicity Note

SQL backends (sqlite, postgres) use transactions for atomic batch inserts. MongoDB uses `InsertMany` with `ordered:false` — partial success is possible on non-dedup errors.

## Build Commands

```bash
go build ./...          # Build
go test ./...           # Unit tests (SQLite only)
just test-integration   # All backends with Docker
just test-sqlite        # SQLite only
just test-pg            # PostgreSQL only
just test-mongo         # MongoDB only
just bench              # Benchmarks
just lint               # golangci-lint
```

## Integration Tests

```bash
# Environment variables for manual testing
POSTGRES_DSN=postgres://ledger_test:ledger_test@localhost:5434/ledger_test?sslmode=disable
MONGO_URI=mongodb://localhost:27020/?directConnection=true
```

## Error Handling

- Sentinel errors checked with `errors.Is()`: `ErrStoreClosed`, `ErrEncode`, `ErrDecode`, `ErrNoUpcaster`, `ErrInvalidCursor`, `ErrInvalidName`
- Backend errors wrapped with context: `fmt.Errorf("ledger/sqlite: ...: %w", err)`

## Code Style

- `ctx context.Context` as first parameter
- Return value types, not pointers to value types
- Unexport internal types; only expose Option functions and interfaces
- No panics except in initialization (`NewStream` panics on nil store)
