package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rbaliyan/ledger"
)

var _ metaStore = (*chMetaStore)(nil)

// chMetaStore implements metaStore using a ClickHouse ReplacingMergeTree table.
//
// ReplacingMergeTree deduplicates rows sharing the same ORDER BY key at merge
// time; SELECT FINAL performs the deduplication at query time. The version
// column (UnixMilli timestamp) determines which row wins: higher wins.
//
// Rename is implemented via two INSERTs: a new row for the new name and a
// row with is_deleted=1 for the old name at a higher version.
//
// Concurrent writers from multiple processes may briefly see different UUIDs
// until ClickHouse completes a merge — the daemon is a single process so this
// is not a concern in practice.
type chMetaStore struct {
	db *sql.DB
}

func newCHMetaStore(ctx context.Context, db *sql.DB) (*chMetaStore, error) {
	m := &chMetaStore{db: db}
	return m, m.createTable(ctx)
}

func (m *chMetaStore) createTable(ctx context.Context) error {
	q := `
CREATE TABLE IF NOT EXISTS ledger_stream_metadata (
    store_name String,
    name       String,
    stream_id  String,
    version    UInt64,
    is_deleted UInt8  DEFAULT 0,
    created_at DateTime64(3, 'UTC') DEFAULT now64()
) ENGINE = ReplacingMergeTree(version)
ORDER BY (store_name, name)
SETTINGS index_granularity = 8192`
	_, err := m.db.ExecContext(ctx, q)
	return err
}

func (m *chMetaStore) resolve(ctx context.Context, storeName, name string) (string, bool, error) {
	q := `SELECT stream_id FROM ledger_stream_metadata FINAL
          WHERE store_name=? AND name=? AND is_deleted=0 LIMIT 1`
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

func (m *chMetaStore) resolveOrCreate(ctx context.Context, storeName, name string) (string, error) {
	if id, ok, err := m.resolve(ctx, storeName, name); err != nil {
		return "", err
	} else if ok {
		return id, nil
	}

	newID := uuid.New().String()
	version := uint64(time.Now().UnixMilli())
	q := `INSERT INTO ledger_stream_metadata (store_name, name, stream_id, version, is_deleted) VALUES (?, ?, ?, ?, 0)`
	if _, err := m.db.ExecContext(ctx, q, storeName, name, newID, version); err != nil {
		return "", fmt.Errorf("meta: insert stream %q: %w", name, err)
	}

	// Re-read FINAL to get the winning row in case of a concurrent insert.
	id, ok, err := m.resolve(ctx, storeName, name)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("meta: stream %q not found after insert", name)
	}
	return id, nil
}

func (m *chMetaStore) rename(ctx context.Context, storeName, oldName, newName string) error {
	if oldName == newName {
		return nil
	}

	oldID, ok, err := m.resolve(ctx, storeName, oldName)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: %q in store %q", ledger.ErrStreamNotFound, oldName, storeName)
	}

	if _, ok, err := m.resolve(ctx, storeName, newName); err != nil {
		return fmt.Errorf("meta: rename check target %q: %w", newName, err)
	} else if ok {
		return fmt.Errorf("%w: %q in store %q", ledger.ErrStreamExists, newName, storeName)
	}

	now := uint64(time.Now().UnixMilli())

	insertNew := `INSERT INTO ledger_stream_metadata (store_name, name, stream_id, version, is_deleted) VALUES (?, ?, ?, ?, 0)`
	if _, err := m.db.ExecContext(ctx, insertNew, storeName, newName, oldID, now); err != nil {
		return fmt.Errorf("meta: rename insert new name %q: %w", newName, err)
	}

	// Mark old name deleted with a higher version so ReplacingMergeTree picks it.
	deleteOld := `INSERT INTO ledger_stream_metadata (store_name, name, stream_id, version, is_deleted) VALUES (?, ?, ?, ?, 1)`
	if _, err := m.db.ExecContext(ctx, deleteOld, storeName, oldName, oldID, now+1); err != nil {
		return fmt.Errorf("meta: rename delete old name %q: %w", oldName, err)
	}
	return nil
}

func (m *chMetaStore) listNames(ctx context.Context, storeName, after string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	q := `SELECT name FROM ledger_stream_metadata FINAL
          WHERE store_name=? AND name>? AND is_deleted=0
          ORDER BY name ASC
          LIMIT ?`
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
