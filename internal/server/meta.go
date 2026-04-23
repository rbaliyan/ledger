package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/rbaliyan/ledger"
)

// metaStore maps human-readable stream names to internal UUIDs.
// Each driver supplies an implementation via [DriverResources.Meta] so that
// metadata and entries live in the same database — no sidecar required.
type metaStore interface {
	resolve(ctx context.Context, storeName, name string) (string, bool, error)
	resolveOrCreate(ctx context.Context, storeName, name string) (string, error)
	rename(ctx context.Context, storeName, oldName, newName string) error
	listNames(ctx context.Context, storeName, after string, limit int) ([]string, error)
}

// sqlMetaStore implements metaStore for SQL backends (sqlite, postgres).
// A single shared table (ledger_stream_metadata) is used across all stores;
// the store_name column provides per-store namespacing.
type sqlMetaStore struct {
	db     *sql.DB
	driver string // "sqlite" or "postgres"
}

func newSQLMetaStore(ctx context.Context, db *sql.DB, driver string) (*sqlMetaStore, error) {
	if driver != "sqlite" && driver != "postgres" {
		return nil, fmt.Errorf("server: sql meta store: unsupported driver %q", driver)
	}
	m := &sqlMetaStore{db: db, driver: driver}
	return m, m.init(ctx)
}

func (m *sqlMetaStore) init(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS ledger_stream_metadata (
			store_name TEXT      NOT NULL,
			name       TEXT      NOT NULL,
			stream_id  TEXT      NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (store_name, name),
			UNIQUE (store_name, stream_id)
		)
	`)
	return err
}

// resolve returns the internal UUID for name. Returns ("", false, nil) when
// the stream does not exist. Used for read-only operations that must not
// create phantom streams.
func (m *sqlMetaStore) resolve(ctx context.Context, storeName, name string) (string, bool, error) {
	var q string
	if m.driver == "postgres" {
		q = `SELECT stream_id FROM ledger_stream_metadata WHERE store_name=$1 AND name=$2`
	} else {
		q = `SELECT stream_id FROM ledger_stream_metadata WHERE store_name=? AND name=?`
	}
	var id string
	err := m.db.QueryRowContext(ctx, q, storeName, name).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("meta: resolve stream %q: %w", name, err)
	}
	return id, true, nil
}

// resolveOrCreate returns the internal UUID for name, creating a mapping when
// one does not exist. Used only by Append.
func (m *sqlMetaStore) resolveOrCreate(ctx context.Context, storeName, name string) (string, error) {
	if id, ok, err := m.resolve(ctx, storeName, name); err != nil {
		return "", err
	} else if ok {
		return id, nil
	}

	newID := uuid.New().String()
	var insertSQL string
	if m.driver == "postgres" {
		insertSQL = `INSERT INTO ledger_stream_metadata (store_name, name, stream_id)
		             VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`
	} else {
		insertSQL = `INSERT OR IGNORE INTO ledger_stream_metadata (store_name, name, stream_id)
		             VALUES (?, ?, ?)`
	}
	if _, err := m.db.ExecContext(ctx, insertSQL, storeName, name, newID); err != nil {
		return "", fmt.Errorf("meta: insert stream %q: %w", name, err)
	}

	// Re-select to pick up any concurrently-inserted ID.
	id, ok, err := m.resolve(ctx, storeName, name)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("meta: stream %q not found after insert", name)
	}
	return id, nil
}

// rename changes the human-readable name of an existing stream within a store.
// Returns [ledger.ErrStreamNotFound] when oldName does not exist, and
// [ledger.ErrStreamExists] when newName is already in use.
func (m *sqlMetaStore) rename(ctx context.Context, storeName, oldName, newName string) error {
	if oldName == newName {
		return nil
	}
	if _, ok, err := m.resolve(ctx, storeName, newName); err != nil {
		return fmt.Errorf("meta: rename check target %q: %w", newName, err)
	} else if ok {
		return fmt.Errorf("%w: %q in store %q", ledger.ErrStreamExists, newName, storeName)
	}
	var q string
	if m.driver == "postgres" {
		q = `UPDATE ledger_stream_metadata SET name=$1 WHERE store_name=$2 AND name=$3`
	} else {
		q = `UPDATE ledger_stream_metadata SET name=? WHERE store_name=? AND name=?`
	}
	res, err := m.db.ExecContext(ctx, q, newName, storeName, oldName)
	if err != nil {
		return fmt.Errorf("meta: rename stream %q: %w", oldName, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("meta: rename stream %q: %w", oldName, err)
	}
	if rows == 0 {
		return fmt.Errorf("%w: %q in store %q", ledger.ErrStreamNotFound, oldName, storeName)
	}
	return nil
}

// listNames returns paginated human-readable stream names for a store.
func (m *sqlMetaStore) listNames(ctx context.Context, storeName, after string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	var q string
	if m.driver == "postgres" {
		q = `SELECT name FROM ledger_stream_metadata WHERE store_name=$1 AND name > $2 ORDER BY name ASC LIMIT $3`
	} else {
		q = `SELECT name FROM ledger_stream_metadata WHERE store_name=? AND name > ? ORDER BY name ASC LIMIT ?`
	}
	rows, err := m.db.QueryContext(ctx, q, storeName, after, limit)
	if err != nil {
		return nil, fmt.Errorf("meta: list streams: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("meta: list streams scan: %w", err)
		}
		names = append(names, name)
	}
	return names, rows.Err()
}
