// Package sqlite provides a SQLite-backed ledger store.
//
// The store uses a single table for all streams. Streams are created
// implicitly on first append — no DDL is required per stream.
//
// The caller is responsible for opening and closing the *sql.DB.
// Import a SQLite driver (e.g., modernc.org/sqlite) in your application.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/ledger"
)

var (
	_ ledger.Store[int64]  = (*Store)(nil)
	_ ledger.HealthChecker = (*Store)(nil)
)

// Store is a SQLite ledger store.
type Store struct {
	db     *sql.DB
	table  string
	logger *slog.Logger
	closed atomic.Bool
}

// Option configures the SQLite store.
type Option func(*options)

type options struct {
	table  string
	logger *slog.Logger
}

// WithTable sets the table name. Defaults to "ledger_entries".
// The name must be a valid SQL identifier (alphanumeric and underscore).
func WithTable(name string) Option {
	return func(o *options) { o.table = name }
}

// WithLogger sets the structured logger. Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// New creates a new SQLite ledger store. The table and indexes are created
// automatically. The caller is responsible for opening and closing the *sql.DB.
func New(ctx context.Context, db *sql.DB, opts ...Option) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("ledger/sqlite: db must not be nil")
	}
	o := options{table: "ledger_entries", logger: slog.Default()}
	for _, fn := range opts {
		fn(&o)
	}
	if err := ledger.ValidateName(o.table); err != nil {
		return nil, fmt.Errorf("ledger/sqlite: %w", err)
	}

	s := &Store{db: db, table: o.table, logger: o.logger}
	if err := s.createTable(ctx); err != nil {
		return nil, fmt.Errorf("ledger/sqlite: create table: %w", err)
	}
	return s, nil
}

func (s *Store) createTable(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id             INTEGER PRIMARY KEY AUTOINCREMENT,
			stream         TEXT    NOT NULL,
			payload        BLOB    NOT NULL,
			order_key      TEXT    NOT NULL DEFAULT '',
			dedup_key      TEXT    NOT NULL DEFAULT '',
			schema_version INTEGER NOT NULL DEFAULT 1,
			metadata       TEXT,
			created_at     TEXT    NOT NULL DEFAULT (strftime('%%Y-%%m-%%dT%%H:%%M:%%f','now'))
		)`, s.table)
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return err
	}

	idx := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_stream_id ON %s(stream, id)`, s.table, s.table)
	if _, err := s.db.ExecContext(ctx, idx); err != nil {
		return err
	}

	idx = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_stream_order ON %s(stream, order_key, id)`, s.table, s.table)
	if _, err := s.db.ExecContext(ctx, idx); err != nil {
		return err
	}

	idx = fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS idx_%s_dedup ON %s(stream, dedup_key) WHERE dedup_key != ''`, s.table, s.table)
	_, err := s.db.ExecContext(ctx, idx)
	return err
}

// Append adds entries to the named stream. Returns IDs of newly appended entries.
func (s *Store) Append(ctx context.Context, stream string, entries ...ledger.RawEntry) ([]int64, error) {
	if s.closed.Load() {
		return nil, ledger.ErrStoreClosed
	}
	if len(entries) == 0 {
		return nil, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("ledger/sqlite: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	query := fmt.Sprintf(
		`INSERT OR IGNORE INTO %s (stream, payload, order_key, dedup_key, schema_version, metadata) VALUES (?, ?, ?, ?, ?, ?)`,
		s.table,
	)
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ledger/sqlite: prepare: %w", err)
	}
	defer stmt.Close()

	var ids []int64
	for _, e := range entries {
		meta, err := encodeMetadata(e.Metadata)
		if err != nil {
			return nil, fmt.Errorf("ledger/sqlite: encode metadata: %w", err)
		}

		res, err := stmt.ExecContext(ctx, stream, e.Payload, e.OrderKey, e.DedupKey, e.SchemaVersion, meta)
		if err != nil {
			return nil, fmt.Errorf("ledger/sqlite: insert: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("ledger/sqlite: rows affected: %w", err)
		}
		if n > 0 {
			id, err := res.LastInsertId()
			if err != nil {
				return nil, fmt.Errorf("ledger/sqlite: last insert id: %w", err)
			}
			ids = append(ids, id)
		} else if e.DedupKey != "" {
			s.logger.DebugContext(ctx, "dedup skip", "stream", stream, "dedup_key", e.DedupKey)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("ledger/sqlite: commit: %w", err)
	}
	return ids, nil
}

// Read returns entries from the named stream.
func (s *Store) Read(ctx context.Context, stream string, opts ...ledger.ReadOption) ([]ledger.StoredEntry[int64], error) {
	if s.closed.Load() {
		return nil, ledger.ErrStoreClosed
	}

	o := ledger.ApplyReadOptions(opts...)

	var (
		clauses []string
		args    []any
	)
	clauses = append(clauses, "stream = ?")
	args = append(args, stream)

	if o.HasAfter() {
		after, ok := ledger.AfterValue[int64](o)
		if !ok {
			return nil, fmt.Errorf("%w: expected int64", ledger.ErrInvalidCursor)
		}
		if o.Order() == ledger.Descending {
			clauses = append(clauses, "id < ?")
		} else {
			clauses = append(clauses, "id > ?")
		}
		args = append(args, after)
	}

	if key := o.OrderKeyFilter(); key != "" {
		clauses = append(clauses, "order_key = ?")
		args = append(args, key)
	}

	dir := "ASC"
	if o.Order() == ledger.Descending {
		dir = "DESC"
	}

	query := fmt.Sprintf(
		`SELECT id, stream, payload, order_key, dedup_key, schema_version, metadata, created_at FROM %s WHERE %s ORDER BY id %s LIMIT ?`,
		s.table, strings.Join(clauses, " AND "), dir,
	)
	args = append(args, o.Limit())

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("ledger/sqlite: query: %w", err)
	}
	defer rows.Close()

	var entries []ledger.StoredEntry[int64]
	for rows.Next() {
		var (
			e         ledger.StoredEntry[int64]
			meta      sql.NullString
			createdAt string
		)
		if err := rows.Scan(&e.ID, &e.Stream, &e.Payload, &e.OrderKey, &e.DedupKey, &e.SchemaVersion, &meta, &createdAt); err != nil {
			return nil, fmt.Errorf("ledger/sqlite: scan: %w", err)
		}
		if meta.Valid {
			m, err := decodeMetadata(meta.String)
			if err != nil {
				return nil, fmt.Errorf("ledger/sqlite: decode metadata: %w", err)
			}
			e.Metadata = m
		}
		t, err := time.Parse("2006-01-02T15:04:05.000", createdAt)
		if err != nil {
			return nil, fmt.Errorf("ledger/sqlite: parse created_at: %w", err)
		}
		e.CreatedAt = t
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ledger/sqlite: rows iteration: %w", err)
	}
	return entries, nil
}

// Count returns the number of entries in the named stream.
func (s *Store) Count(ctx context.Context, stream string) (int64, error) {
	if s.closed.Load() {
		return 0, ledger.ErrStoreClosed
	}
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE stream = ?`, s.table)
	var count int64
	if err := s.db.QueryRowContext(ctx, query, stream).Scan(&count); err != nil {
		return 0, fmt.Errorf("ledger/sqlite: count: %w", err)
	}
	return count, nil
}

// Trim deletes entries from the named stream with IDs <= beforeID.
func (s *Store) Trim(ctx context.Context, stream string, beforeID int64) (int64, error) {
	if s.closed.Load() {
		return 0, ledger.ErrStoreClosed
	}
	query := fmt.Sprintf(`DELETE FROM %s WHERE stream = ? AND id <= ?`, s.table)
	res, err := s.db.ExecContext(ctx, query, stream, beforeID)
	if err != nil {
		return 0, fmt.Errorf("ledger/sqlite: trim: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("ledger/sqlite: trim rows affected: %w", err)
	}
	return n, nil
}

// Health checks database connectivity by pinging the underlying connection.
func (s *Store) Health(ctx context.Context) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
	}
	return s.db.PingContext(ctx)
}

// Close marks the store as closed. The caller is responsible for closing
// the underlying *sql.DB.
func (s *Store) Close(_ context.Context) error {
	s.closed.Store(true)
	return nil
}

func encodeMetadata(m map[string]string) (sql.NullString, error) {
	if len(m) == 0 {
		return sql.NullString{}, nil
	}
	data, err := json.Marshal(m)
	if err != nil {
		return sql.NullString{}, err
	}
	return sql.NullString{String: string(data), Valid: true}, nil
}

func decodeMetadata(s string) (map[string]string, error) {
	var m map[string]string
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, err
	}
	return m, nil
}
