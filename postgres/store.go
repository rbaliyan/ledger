// Package postgres provides a PostgreSQL-backed ledger store.
//
// The store uses a single table for all streams. Streams are created
// implicitly on first append — no DDL is required per stream.
//
// The caller is responsible for opening and closing the *sql.DB.
// Import a PostgreSQL driver (e.g., github.com/lib/pq) in your application.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"

	"github.com/rbaliyan/ledger"
)

var (
	_ ledger.Store[int64]  = (*Store)(nil)
	_ ledger.HealthChecker = (*Store)(nil)
)

// Store is a PostgreSQL ledger store.
type Store struct {
	db     *sql.DB
	table  string
	logger *slog.Logger
	closed atomic.Bool
}

// Option configures the PostgreSQL store.
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

// New creates a new PostgreSQL ledger store. The table and indexes are created
// automatically. The caller is responsible for opening and closing the *sql.DB.
func New(ctx context.Context, db *sql.DB, opts ...Option) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("ledger/postgres: db must not be nil")
	}
	o := options{table: "ledger_entries", logger: slog.Default()}
	for _, fn := range opts {
		fn(&o)
	}
	if err := ledger.ValidateName(o.table); err != nil {
		return nil, fmt.Errorf("ledger/postgres: %w", err)
	}

	s := &Store{db: db, table: o.table, logger: o.logger}
	if err := s.createTable(ctx); err != nil {
		return nil, fmt.Errorf("ledger/postgres: create table: %w", err)
	}
	return s, nil
}

func (s *Store) createTable(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id             BIGSERIAL   PRIMARY KEY,
			stream         TEXT        NOT NULL,
			payload        BYTEA       NOT NULL,
			order_key      TEXT        NOT NULL DEFAULT '',
			dedup_key      TEXT        NOT NULL DEFAULT '',
			schema_version INTEGER     NOT NULL DEFAULT 1,
			metadata       JSONB,
			created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
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
		return nil, fmt.Errorf("ledger/postgres: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	query := fmt.Sprintf(
		`INSERT INTO %s (stream, payload, order_key, dedup_key, schema_version, metadata)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (stream, dedup_key) WHERE dedup_key != '' DO NOTHING
		 RETURNING id`,
		s.table,
	)
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ledger/postgres: prepare: %w", err)
	}
	defer stmt.Close()

	var ids []int64
	for _, e := range entries {
		meta, err := encodeMetadata(e.Metadata)
		if err != nil {
			return nil, fmt.Errorf("ledger/postgres: encode metadata: %w", err)
		}

		var id int64
		err = stmt.QueryRowContext(ctx, stream, e.Payload, e.OrderKey, e.DedupKey, e.SchemaVersion, meta).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			s.logger.DebugContext(ctx, "dedup skip", "stream", stream, "dedup_key", e.DedupKey)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("ledger/postgres: insert: %w", err)
		}
		ids = append(ids, id)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("ledger/postgres: commit: %w", err)
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
		argN    int
	)

	argN++
	clauses = append(clauses, fmt.Sprintf("stream = $%d", argN))
	args = append(args, stream)

	if o.HasAfter() {
		after, ok := ledger.AfterValue[int64](o)
		if !ok {
			return nil, fmt.Errorf("%w: expected int64", ledger.ErrInvalidCursor)
		}
		argN++
		if o.Order() == ledger.Descending {
			clauses = append(clauses, fmt.Sprintf("id < $%d", argN))
		} else {
			clauses = append(clauses, fmt.Sprintf("id > $%d", argN))
		}
		args = append(args, after)
	}

	if key := o.OrderKeyFilter(); key != "" {
		argN++
		clauses = append(clauses, fmt.Sprintf("order_key = $%d", argN))
		args = append(args, key)
	}

	dir := "ASC"
	if o.Order() == ledger.Descending {
		dir = "DESC"
	}

	argN++
	query := fmt.Sprintf(
		`SELECT id, stream, payload, order_key, dedup_key, schema_version, metadata, created_at FROM %s WHERE %s ORDER BY id %s LIMIT $%d`,
		s.table, strings.Join(clauses, " AND "), dir, argN,
	)
	args = append(args, o.Limit())

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("ledger/postgres: query: %w", err)
	}
	defer rows.Close()

	var entries []ledger.StoredEntry[int64]
	for rows.Next() {
		var (
			e    ledger.StoredEntry[int64]
			meta []byte
		)
		if err := rows.Scan(&e.ID, &e.Stream, &e.Payload, &e.OrderKey, &e.DedupKey, &e.SchemaVersion, &meta, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("ledger/postgres: scan: %w", err)
		}
		if len(meta) > 0 {
			m, err := decodeMetadata(meta)
			if err != nil {
				return nil, fmt.Errorf("ledger/postgres: decode metadata: %w", err)
			}
			e.Metadata = m
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ledger/postgres: rows iteration: %w", err)
	}
	return entries, nil
}

// Count returns the number of entries in the named stream.
func (s *Store) Count(ctx context.Context, stream string) (int64, error) {
	if s.closed.Load() {
		return 0, ledger.ErrStoreClosed
	}
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE stream = $1`, s.table)
	var count int64
	if err := s.db.QueryRowContext(ctx, query, stream).Scan(&count); err != nil {
		return 0, fmt.Errorf("ledger/postgres: count: %w", err)
	}
	return count, nil
}

// Trim deletes entries from the named stream with IDs <= beforeID.
func (s *Store) Trim(ctx context.Context, stream string, beforeID int64) (int64, error) {
	if s.closed.Load() {
		return 0, ledger.ErrStoreClosed
	}
	query := fmt.Sprintf(`DELETE FROM %s WHERE stream = $1 AND id <= $2`, s.table)
	res, err := s.db.ExecContext(ctx, query, stream, beforeID)
	if err != nil {
		return 0, fmt.Errorf("ledger/postgres: trim: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("ledger/postgres: trim rows affected: %w", err)
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

func encodeMetadata(m map[string]string) ([]byte, error) {
	if len(m) == 0 {
		return nil, nil
	}
	return json.Marshal(m)
}

func decodeMetadata(data []byte) (map[string]string, error) {
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}
