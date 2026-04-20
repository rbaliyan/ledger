package server

import (
	"context"
	"database/sql"
	"errors"
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

// supportedMetaDrivers enumerates database drivers the metadata store knows
// how to dialect-specialise. Backends outside this set cannot be used with the
// daemon mux today.
var supportedMetaDrivers = map[string]bool{
	"sqlite":   true,
	"postgres": true,
}

func newStreamMetaStore(ctx context.Context, db *sql.DB, driver string) (*streamMetaStore, error) {
	if !supportedMetaDrivers[driver] {
		return nil, fmt.Errorf("server: stream metadata driver %q not supported", driver)
	}
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

// resolve returns the internal UUID for name. Returns ("", false, nil) when
// the stream does not exist in the metadata table. Used for read-only
// operations that must not create phantom streams.
func (m *streamMetaStore) resolve(ctx context.Context, storeName, name string) (string, bool, error) {
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

// resolveOrCreate returns the internal UUID for the given human-readable name,
// creating a new mapping when one does not exist. Used only by Append.
func (m *streamMetaStore) resolveOrCreate(ctx context.Context, storeName, name string) (string, error) {
	// Fast path: existing mapping.
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
func (m *streamMetaStore) rename(ctx context.Context, storeName, oldName, newName string) error {
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
