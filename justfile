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

# Default recipe
default:
    @just --list

# Build all packages
build:
    go build ./...

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

# Run all integration tests (MongoDB + PostgreSQL + SQLite)
test-integration: mongo-start pg-start
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Running integration tests..."
    MONGO_URI="mongodb://localhost:{{MONGO_PORT}}/?directConnection=true" \
    POSTGRES_DSN="postgres://{{PG_USER}}:{{PG_PASS}}@localhost:{{PG_PORT}}/{{PG_DB}}?sslmode=disable" \
    go test -v -race -count=1 ./...
    just mongo-stop
    just pg-stop

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

# Run SQLite tests (no external services needed)
test-sqlite:
    go test -v -count=1 ./sqlite/...

# Start MongoDB for testing
mongo-start:
    #!/usr/bin/env bash
    set -euo pipefail
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{MONGO_CONTAINER}}$"; then
        echo "Removing existing container {{MONGO_CONTAINER}}..."
        {{DOCKER}} rm -f {{MONGO_CONTAINER}} > /dev/null
    fi
    echo "Starting MongoDB on port {{MONGO_PORT}}..."
    {{DOCKER}} run -d --name {{MONGO_CONTAINER}} -p {{MONGO_PORT}}:27017 {{MONGO_IMAGE}}
    echo "Waiting for MongoDB to start..."
    sleep 3
    echo "MongoDB ready on port {{MONGO_PORT}}"

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
clean: mongo-stop pg-stop
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
