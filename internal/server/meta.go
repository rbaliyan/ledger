package server

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
	"github.com/rbaliyan/ledger"
)

// streamMetaStore maps human-readable stream names to internal UUIDs.
// A single table (ledger_stream_metadata) is shared across all stores; the
// store_name column provides per-store namespacing.
type streamMetaStore struct {
	db     *sql.DB
	driver string // "sqlite" or "postgres"
}

func newStreamMetaStore(ctx context.Context, db *sql.DB, driver string) (*streamMetaStore, error) {
	m := &streamMetaStore{db: db, driver: driver}
	return m, m.init(ctx)
}

func (m *streamMetaStore) init(ctx context.Context) error {
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

// resolveOrCreate returns the internal UUID for the given human-readable name,
// creating a new mapping when one does not exist.
func (m *streamMetaStore) resolveOrCreate(ctx context.Context, storeName, name string) (string, error) {
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

	var actualID string
	var selectSQL string
	if m.driver == "postgres" {
		selectSQL = `SELECT stream_id FROM ledger_stream_metadata WHERE store_name=$1 AND name=$2`
	} else {
		selectSQL = `SELECT stream_id FROM ledger_stream_metadata WHERE store_name=? AND name=?`
	}
	if err := m.db.QueryRowContext(ctx, selectSQL, storeName, name).Scan(&actualID); err != nil {
		return "", fmt.Errorf("meta: resolve stream %q: %w", name, err)
	}
	return actualID, nil
}

// rename changes the human-readable name of an existing stream within a store.
// Returns [ledger.ErrStreamNotFound] when oldName does not exist.
func (m *streamMetaStore) rename(ctx context.Context, storeName, oldName, newName string) error {
	if oldName == newName {
		return nil
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
func (m *streamMetaStore) listNames(ctx context.Context, storeName, after string, limit int) ([]string, error) {
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
