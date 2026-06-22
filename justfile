# Container runtime: docker or podman
DOCKER := env("DOCKER", "docker")

# MongoDB container settings
MONGO_CONTAINER := "ledger-mongo-test"
MONGO_PORT := "27020"
MONGO_IMAGE := "mongo:7.0"

# PostgreSQL container settings
PG_CONTAINER := "ledger-postgres-test"
PG_PORT := "5434"
PG_IMAGE := "postgres:16"
PG_USER := "ledger_test"
PG_PASS := "ledger_test"
PG_DB := "ledger_test"

# ClickHouse container settings
CH_CONTAINER := "ledger-clickhouse-test"
CH_PORT := "9100"
CH_IMAGE := "clickhouse/clickhouse-server:24.8"
CH_USER := "ledger_test"
CH_PASS := "ledger_test"
CH_DB := "ledger_test"

# Default recipe
default:
    @just --list

# Generate protobuf code (requires buf via mise)
proto:
    buf generate

# Lint protobuf definitions
proto-lint:
    buf lint

# Build all packages and output the ledger binary to bin/ledger
build:
    go build ./...
    go build -o bin/ledger ./cmd

# Run tests
test:
    go test ./...

# Run tests with verbose output
test-v:
    go test -v ./...

# Run tests with race detector
test-race:
    go test -race ./...

# Run tests with coverage
test-cover:
    go test -cover ./...

# Run the fast smoke and example subset (no external services)
smoke:
    go test -run 'Smoke|Example|TestServer|TestGateway' -count=1 ./internal/server/... ./internal/daemon/... ./internal/cli/... .

# Run all integration tests (MongoDB + PostgreSQL + ClickHouse + SQLite)
test-integration: mongo-start pg-start clickhouse-start
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Running integration tests..."
    MONGO_URI="mongodb://localhost:{{MONGO_PORT}}/?directConnection=true" \
    POSTGRES_DSN="postgres://{{PG_USER}}:{{PG_PASS}}@localhost:{{PG_PORT}}/{{PG_DB}}?sslmode=disable" \
    CLICKHOUSE_DSN="clickhouse://{{CH_USER}}:{{CH_PASS}}@127.0.0.1:{{CH_PORT}}/{{CH_DB}}" \
    go test -tags=integration -v -race -count=1 ./...
    just mongo-stop
    just pg-stop
    just clickhouse-stop

# Run MongoDB integration tests only
test-mongo: mongo-start
    #!/usr/bin/env bash
    set -euo pipefail
    MONGO_URI="mongodb://localhost:{{MONGO_PORT}}/?directConnection=true" go test -v -count=1 ./mongodb/...
    just mongo-stop

# Run PostgreSQL integration tests only
test-pg: pg-start
    #!/usr/bin/env bash
    set -euo pipefail
    POSTGRES_DSN="postgres://{{PG_USER}}:{{PG_PASS}}@localhost:{{PG_PORT}}/{{PG_DB}}?sslmode=disable" go test -v -count=1 ./postgres/...
    just pg-stop

# Run ClickHouse integration tests only
test-clickhouse: clickhouse-start
    #!/usr/bin/env bash
    set -euo pipefail
    CLICKHOUSE_DSN="clickhouse://{{CH_USER}}:{{CH_PASS}}@127.0.0.1:{{CH_PORT}}/{{CH_DB}}" go test -v -count=1 ./clickhouse/...
    just clickhouse-stop

# Run SQLite tests (no external services needed)
test-sqlite:
    go test -v -count=1 ./sqlite/...

# Start MongoDB replica set for testing
mongo-start:
    #!/usr/bin/env bash
    set -euo pipefail
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{MONGO_CONTAINER}}$"; then
        echo "Removing existing container {{MONGO_CONTAINER}}..."
        {{DOCKER}} rm -f {{MONGO_CONTAINER}} > /dev/null
    fi
    echo "Starting MongoDB replica set on port {{MONGO_PORT}}..."
    {{DOCKER}} run -d --name {{MONGO_CONTAINER}} -p {{MONGO_PORT}}:27017 {{MONGO_IMAGE}} --replSet rs0
    echo "Waiting for MongoDB to be ready..."
    for i in $(seq 1 30); do
        if {{DOCKER}} exec {{MONGO_CONTAINER}} mongosh --quiet --eval "db.adminCommand('ping').ok" 2>/dev/null | grep -q "1"; then
            break
        fi
        sleep 1
    done
    echo "Initialising replica set..."
    {{DOCKER}} exec {{MONGO_CONTAINER}} mongosh --quiet --eval \
        "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]})"
    echo "Waiting for primary election..."
    for i in $(seq 1 30); do
        if {{DOCKER}} exec {{MONGO_CONTAINER}} mongosh --quiet --eval "rs.isMaster().ismaster" 2>/dev/null | grep -q "true"; then
            break
        fi
        sleep 1
    done
    echo "MongoDB replica set ready on port {{MONGO_PORT}}"

# Stop MongoDB container
mongo-stop:
    #!/usr/bin/env bash
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{MONGO_CONTAINER}}$"; then
        {{DOCKER}} rm -f {{MONGO_CONTAINER}} > /dev/null
        echo "MongoDB container stopped"
    fi

# Start PostgreSQL for testing
pg-start:
    #!/usr/bin/env bash
    set -euo pipefail
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{PG_CONTAINER}}$"; then
        echo "Removing existing container {{PG_CONTAINER}}..."
        {{DOCKER}} rm -f {{PG_CONTAINER}} > /dev/null
    fi
    echo "Starting PostgreSQL on port {{PG_PORT}}..."
    {{DOCKER}} run -d --name {{PG_CONTAINER}} \
        -p {{PG_PORT}}:5432 \
        -e POSTGRES_USER={{PG_USER}} \
        -e POSTGRES_PASSWORD={{PG_PASS}} \
        -e POSTGRES_DB={{PG_DB}} \
        {{PG_IMAGE}}
    echo "Waiting for PostgreSQL to be ready..."
    for i in $(seq 1 30); do
        if {{DOCKER}} exec {{PG_CONTAINER}} pg_isready -U {{PG_USER}} > /dev/null 2>&1; then
            echo "PostgreSQL ready on port {{PG_PORT}}"
            exit 0
        fi
        sleep 1
    done
    echo "PostgreSQL failed to start within 30 seconds"
    exit 1

# Stop PostgreSQL container
pg-stop:
    #!/usr/bin/env bash
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{PG_CONTAINER}}$"; then
        {{DOCKER}} rm -f {{PG_CONTAINER}} > /dev/null
        echo "PostgreSQL container stopped"
    fi

# Start ClickHouse for testing
clickhouse-start:
    #!/usr/bin/env bash
    set -euo pipefail
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{CH_CONTAINER}}$"; then
        echo "Removing existing container {{CH_CONTAINER}}..."
        {{DOCKER}} rm -f {{CH_CONTAINER}} > /dev/null
    fi
    echo "Starting ClickHouse on port {{CH_PORT}}..."
    {{DOCKER}} run -d --name {{CH_CONTAINER}} \
        -p {{CH_PORT}}:9000 \
        --ulimit nofile=262144:262144 \
        -e CLICKHOUSE_USER={{CH_USER}} \
        -e CLICKHOUSE_PASSWORD={{CH_PASS}} \
        -e CLICKHOUSE_DB={{CH_DB}} \
        {{CH_IMAGE}} > /dev/null
    echo "Waiting for ClickHouse to be ready..."
    for i in $(seq 1 60); do
        if {{DOCKER}} exec {{CH_CONTAINER}} clickhouse-client --user {{CH_USER}} --password {{CH_PASS}} --query "SELECT 1" > /dev/null 2>&1; then
            echo "ClickHouse ready on port {{CH_PORT}}"
            exit 0
        fi
        sleep 1
    done
    echo "ClickHouse failed to start within 60 seconds"
    exit 1

# Stop ClickHouse container
clickhouse-stop:
    #!/usr/bin/env bash
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{CH_CONTAINER}}$"; then
        {{DOCKER}} rm -f {{CH_CONTAINER}} > /dev/null
        echo "ClickHouse container stopped"
    fi

# Format code
fmt:
    go fmt ./...

# Lint code
lint:
    golangci-lint run ./...

# Tidy dependencies
tidy:
    go mod tidy

# Clean up containers and test cache
clean: mongo-stop pg-stop clickhouse-stop
    go clean -testcache

# Install mise tools
tools:
    mise install

# Run vulnerability check
vulncheck:
    go run golang.org/x/vuln/cmd/govulncheck@latest ./...

# Check for outdated dependencies
depcheck:
    go list -m -u all | grep '\[' || echo "All dependencies are up to date"

# Run benchmarks
bench:
    go test -bench=. -benchmem ./sqlite/...

# Create and push a new release tag (bumps patch version)
release:
    ./scripts/release.sh
