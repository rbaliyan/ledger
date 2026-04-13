// Package postgres provides a PostgreSQL-backed ledger store.
//
// The store uses a single table for all streams. Streams are created
// implicitly on first append — no DDL is required per stream.
//
// The caller is responsible for opening and closing the *sql.DB.
// Import a PostgreSQL driver (e.g., github.com/lib/pq) in your application.
//
// Transaction support: pass a *sql.Tx via [ledger.WithTx] to have the store
// participate in an external transaction instead of creating its own.
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

type sqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

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
func WithTable(name string) Option {
	return func(o *options) { o.table = name }
}

// WithLogger sets the structured logger. Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// New creates a new PostgreSQL ledger store. The table and indexes are created
// automatically. Tag GIN indexes are created asynchronously in the background.
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
	go s.createAsyncIndexes(o.logger)
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
			tags           JSONB       NOT NULL DEFAULT '[]'::jsonb,
			annotations    JSONB,
			created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at     TIMESTAMPTZ
		)`, s.table)
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return err
	}

	for _, idx := range []string{
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_stream_id ON %s(stream, id)`, s.table, s.table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_stream_order ON %s(stream, order_key, id)`, s.table, s.table),
		fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS idx_%s_dedup ON %s(stream, dedup_key) WHERE dedup_key != ''`, s.table, s.table),
		// Single-column stream index supports efficient DISTINCT scans for ListStreamIDs;
		// cheap to maintain due to low cardinality of stream values.
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_stream ON %s(stream)`, s.table, s.table),
	} {
		if _, err := s.db.ExecContext(ctx, idx); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) createAsyncIndexes(logger *slog.Logger) {
	if s.closed.Load() {
		return
	}
	// GIN index for tag-based filtering. Created async to avoid blocking startup.
	idx := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_tags ON %s USING gin(tags)`, s.table, s.table)
	if _, err := s.db.Exec(idx); err != nil && !s.closed.Load() {
		logger.Warn("failed to create tags GIN index", "error", err)
	}
}

func (s *Store) executor(ctx context.Context) sqlExecutor {
	if tx, ok := ledger.TxFromContext(ctx).(*sql.Tx); ok {
		return tx
	}
	return s.db
}

// Append adds entries to the named stream. Returns IDs of newly appended entries.
func (s *Store) Append(ctx context.Context, stream string, entries ...ledger.RawEntry) ([]int64, error) {
	if s.closed.Load() {
		return nil, ledger.ErrStoreClosed
	}
	if len(entries) == 0 {
		return nil, nil
	}

	exec := s.executor(ctx)

	var ownTx *sql.Tx
	if _, isExternalTx := exec.(*sql.Tx); !isExternalTx {
		var err error
		ownTx, err = s.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("ledger/postgres: begin tx: %w", err)
		}
		defer ownTx.Rollback() //nolint:errcheck
		exec = ownTx
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (stream, payload, order_key, dedup_key, schema_version, metadata, tags)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (stream, dedup_key) WHERE dedup_key != '' DO NOTHING
		 RETURNING id`,
		s.table,
	)
	stmt, err := exec.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ledger/postgres: prepare: %w", err)
	}
	defer stmt.Close()

	var ids []int64
	for _, e := range entries {
		meta, err := encodeJSONB(e.Metadata)
		if err != nil {
			return nil, fmt.Errorf("ledger/postgres: encode metadata: %w", err)
		}
		tags := e.Tags
		if tags == nil {
			tags = []string{}
		}
		tagsJSON, _ := json.Marshal(tags)

		var id int64
		err = stmt.QueryRowContext(ctx, stream, e.Payload, e.OrderKey, e.DedupKey, e.SchemaVersion, meta, tagsJSON).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			s.logger.DebugContext(ctx, "dedup skip", "stream", stream, "dedup_key", e.DedupKey)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("ledger/postgres: insert: %w", err)
		}
		ids = append(ids, id)
	}

	if ownTx != nil {
		if err := ownTx.Commit(); err != nil {
			return nil, fmt.Errorf("ledger/postgres: commit: %w", err)
		}
	}
	return ids, nil
}

// Read returns entries from the named stream.
func (s *Store) Read(ctx context.Context, stream string, opts ...ledger.ReadOption) ([]ledger.StoredEntry[int64], error) {
	if s.closed.Load() {
		return nil, ledger.ErrStoreClosed
	}

	exec := s.executor(ctx)
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

	if tag := o.Tag(); tag != "" {
		argN++
		clauses = append(clauses, fmt.Sprintf("tags @> $%d::jsonb", argN))
		tagJSON, _ := json.Marshal([]string{tag})
		args = append(args, string(tagJSON))
	}
	if allTags := o.AllTags(); len(allTags) > 0 {
		argN++
		clauses = append(clauses, fmt.Sprintf("tags @> $%d::jsonb", argN))
		tagsJSON, _ := json.Marshal(allTags)
		args = append(args, string(tagsJSON))
	}

	dir := "ASC"
	if o.Order() == ledger.Descending {
		dir = "DESC"
	}

	argN++
	query := fmt.Sprintf(
		`SELECT id, stream, payload, order_key, dedup_key, schema_version, metadata, tags, annotations, created_at, updated_at FROM %s WHERE %s ORDER BY id %s LIMIT $%d`,
		s.table, strings.Join(clauses, " AND "), dir, argN,
	)
	args = append(args, o.Limit())

	rows, err := exec.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("ledger/postgres: query: %w", err)
	}
	defer rows.Close()

	var entries []ledger.StoredEntry[int64]
	for rows.Next() {
		var (
			e           ledger.StoredEntry[int64]
			meta        []byte
			tagsJSON    []byte
			annotations []byte
			updatedAt   sql.NullTime
		)
		if err := rows.Scan(&e.ID, &e.Stream, &e.Payload, &e.OrderKey, &e.DedupKey, &e.SchemaVersion, &meta, &tagsJSON, &annotations, &e.CreatedAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("ledger/postgres: scan: %w", err)
		}
		if len(meta) > 0 {
			e.Metadata, _ = decodeJSONB(meta)
		}
		if len(tagsJSON) > 0 {
			json.Unmarshal(tagsJSON, &e.Tags) //nolint:errcheck
		}
		if len(annotations) > 0 {
			e.Annotations, _ = decodeJSONB(annotations)
		}
		if updatedAt.Valid {
			e.UpdatedAt = &updatedAt.Time
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
	exec := s.executor(ctx)
	var count int64
	if err := exec.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE stream = $1`, s.table), stream).Scan(&count); err != nil {
		return 0, fmt.Errorf("ledger/postgres: count: %w", err)
	}
	return count, nil
}

// SetTags replaces all tags on an entry.
func (s *Store) SetTags(ctx context.Context, stream string, id int64, tags []string) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
	}
	if tags == nil {
		tags = []string{}
	}
	tagsJSON, _ := json.Marshal(tags)
	exec := s.executor(ctx)
	res, err := exec.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET tags = $1::jsonb, updated_at = now() WHERE stream = $2 AND id = $3`, s.table), string(tagsJSON), stream, id)
	if err != nil {
		return fmt.Errorf("ledger/postgres: set tags: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ledger.ErrEntryNotFound
	}
	return nil
}

// SetAnnotations merges annotations into an entry. Keys with nil values are deleted.
func (s *Store) SetAnnotations(ctx context.Context, stream string, id int64, annotations map[string]*string) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
	}
	exec := s.executor(ctx)

	// Read current
	var raw []byte
	err := exec.QueryRowContext(ctx, fmt.Sprintf(`SELECT COALESCE(annotations, '{}'::jsonb) FROM %s WHERE stream = $1 AND id = $2`, s.table), stream, id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return ledger.ErrEntryNotFound
	}
	if err != nil {
		return fmt.Errorf("ledger/postgres: read annotations: %w", err)
	}

	current := make(map[string]string)
	json.Unmarshal(raw, &current) //nolint:errcheck

	for k, v := range annotations {
		if v == nil {
			delete(current, k)
		} else {
			current[k] = *v
		}
	}

	data, _ := json.Marshal(current)
	_, err = exec.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET annotations = $1::jsonb, updated_at = now() WHERE stream = $2 AND id = $3`, s.table), string(data), stream, id)
	if err != nil {
		return fmt.Errorf("ledger/postgres: set annotations: %w", err)
	}
	return nil
}

// ListStreamIDs returns distinct stream IDs with at least one entry in this store.
// Results are ascending by stream ID and cursor-paginated via ListAfter/ListLimit.
func (s *Store) ListStreamIDs(ctx context.Context, opts ...ledger.ListOption) ([]string, error) {
	if s.closed.Load() {
		return nil, ledger.ErrStoreClosed
	}
	o := ledger.ApplyListOptions(opts...)
	exec := s.executor(ctx)

	query := fmt.Sprintf(
		`SELECT DISTINCT stream FROM %s WHERE stream > $1 ORDER BY stream ASC LIMIT $2`,
		s.table,
	)
	rows, err := exec.QueryContext(ctx, query, o.After(), o.Limit())
	if err != nil {
		return nil, fmt.Errorf("ledger/postgres: list stream ids: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("ledger/postgres: scan stream id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ledger/postgres: list stream ids iteration: %w", err)
	}
	return ids, nil
}

// Type returns the table name this store is bound to, which represents the
// entity type for all streams in this store. Intended for logging/tracing.
func (s *Store) Type() string { return s.table }

// Trim deletes entries from the named stream with IDs <= beforeID.
func (s *Store) Trim(ctx context.Context, stream string, beforeID int64) (int64, error) {
	if s.closed.Load() {
		return 0, ledger.ErrStoreClosed
	}
	exec := s.executor(ctx)
	res, err := exec.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE stream = $1 AND id <= $2`, s.table), stream, beforeID)
	if err != nil {
		return 0, fmt.Errorf("ledger/postgres: trim: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// Health checks database connectivity.
func (s *Store) Health(ctx context.Context) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
	}
	return s.db.PingContext(ctx)
}

// Close marks the store as closed.
func (s *Store) Close(_ context.Context) error {
	s.closed.Store(true)
	return nil
}

func encodeJSONB(m map[string]string) ([]byte, error) {
	if len(m) == 0 {
		return nil, nil
	}
	return json.Marshal(m)
}

func decodeJSONB(data []byte) (map[string]string, error) {
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}
