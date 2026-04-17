// Package sqlite provides a SQLite-backed ledger store.
//
// The store uses a single table for all streams. Streams are created
// implicitly on first append — no DDL is required per stream.
//
// The caller is responsible for opening and closing the *sql.DB.
// Import a SQLite driver (e.g., modernc.org/sqlite) in your application.
//
// Transaction support: pass a *sql.Tx via [ledger.WithTx] to have the store
// participate in an external transaction instead of creating its own.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/ledger"
	internalReplication "github.com/rbaliyan/ledger/internal/replication"
)

// sqlExecutor is the common interface between *sql.DB and *sql.Tx.
type sqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

var (
	_ ledger.Store[int64, json.RawMessage] = (*Store)(nil)
	_ ledger.HealthChecker                 = (*Store)(nil)
	_ ledger.CursorStore                   = (*Store)(nil)
	_ ledger.SourceIDLookup[int64]         = (*Store)(nil)
)

// Store is a SQLite ledger store.
type Store struct {
	db          *sql.DB
	table       string
	logger      *slog.Logger
	closed      atomic.Bool
	mutationLog ledger.Store[int64, json.RawMessage]
	appendOnly  bool
}

// Option configures the SQLite store.
type Option func(*options)

type options struct {
	table       string
	logger      *slog.Logger
	mutationLog ledger.Store[int64, json.RawMessage]
	appendOnly  bool
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

// WithMutationLog enables atomic mutation tracking. mutLog must be backed by the
// same *sql.DB so mutations and main writes share a transaction.
func WithMutationLog(mutLog ledger.Store[int64, json.RawMessage]) Option {
	return func(o *options) { o.mutationLog = mutLog }
}

// WithAppendOnly disables SetTags and SetAnnotations, returning [ledger.ErrNotSupported].
// Use this when the replication sink (e.g. ClickHouse) does not support entry mutations,
// so the source cannot produce mutation events that the sink cannot apply.
func WithAppendOnly() Option {
	return func(o *options) { o.appendOnly = true }
}

// New creates a new SQLite ledger store. The table and indexes are created
// automatically. Tag indexes are created asynchronously in the background.
// The caller is responsible for opening and closing the *sql.DB.
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

	s := &Store{db: db, table: o.table, logger: o.logger, mutationLog: o.mutationLog, appendOnly: o.appendOnly}
	if err := s.createTable(ctx); err != nil {
		return nil, fmt.Errorf("ledger/sqlite: create table: %w", err)
	}
	if err := s.migrateSchema(ctx); err != nil {
		return nil, err
	}
	go s.createAsyncIndexes(o.logger)
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
			tags           TEXT    NOT NULL DEFAULT '[]',
			annotations    TEXT,
			source_id      TEXT    NOT NULL DEFAULT '',
			created_at     TEXT    NOT NULL DEFAULT (strftime('%%Y-%%m-%%dT%%H:%%M:%%f','now')),
			updated_at     TEXT
		)`, s.table)
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return err
	}

	idx := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_stream_id ON %s(stream, id)`, s.table, s.table)
	if _, err := s.db.ExecContext(ctx, idx); err != nil {
		return err
	}

	// Single-column index on stream supports efficient DISTINCT stream scans
	// for ListStreamIDs; cheap to maintain due to low cardinality of stream values.
	idx = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_stream ON %s(stream)`, s.table, s.table)
	if _, err := s.db.ExecContext(ctx, idx); err != nil {
		return err
	}

	idx = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_stream_order ON %s(stream, order_key, id)`, s.table, s.table)
	if _, err := s.db.ExecContext(ctx, idx); err != nil {
		return err
	}

	idx = fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS idx_%s_dedup ON %s(stream, dedup_key) WHERE dedup_key != ''`, s.table, s.table)
	if _, err := s.db.ExecContext(ctx, idx); err != nil {
		return err
	}

	idx = fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS idx_%s_source ON %s(source_id) WHERE source_id != ''`, s.table, s.table)
	if _, err := s.db.ExecContext(ctx, idx); err != nil {
		return err
	}

	cursorTable := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_cursors (name TEXT NOT NULL PRIMARY KEY, cursor TEXT NOT NULL)`, s.table)
	_, err := s.db.ExecContext(ctx, cursorTable)
	return err
}

func (s *Store) createAsyncIndexes(logger *slog.Logger) {
	if s.closed.Load() {
		return
	}
	// Tag-based filtering index. Created async since it's not needed for core operations.
	idx := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_tags ON %s(stream, tags)`, s.table, s.table)
	if _, err := s.db.Exec(idx); err != nil && !s.closed.Load() {
		logger.Warn("failed to create tags index", "error", err)
	}
}

func (s *Store) migrateSchema(ctx context.Context) error {
	// Add source_id to existing tables; ignore "duplicate column name" error.
	alter := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN source_id TEXT NOT NULL DEFAULT ''`, s.table)
	if _, err := s.db.ExecContext(ctx, alter); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("ledger/sqlite: migrate schema: %w", err)
		}
	}
	// Create cursor table (idempotent).
	cursor := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_cursors (name TEXT NOT NULL PRIMARY KEY, cursor TEXT NOT NULL)`, s.table)
	if _, err := s.db.ExecContext(ctx, cursor); err != nil {
		return fmt.Errorf("ledger/sqlite: create cursors table: %w", err)
	}
	// Create source_id index (idempotent).
	idx := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS idx_%s_source ON %s(source_id) WHERE source_id != ''`, s.table, s.table) // #nosec G201 -- table name validated by ValidateName
	if _, err := s.db.ExecContext(ctx, idx); err != nil {
		return fmt.Errorf("ledger/sqlite: create source index: %w", err)
	}
	return nil
}

// executor returns the *sql.Tx from context if present, otherwise s.db.
func (s *Store) executor(ctx context.Context) sqlExecutor {
	if tx, ok := ledger.TxFromContext(ctx).(*sql.Tx); ok {
		return tx
	}
	return s.db
}

// withMutTx runs fn inside a transaction shared with the mutation log.
// If there's already an external tx in ctx, uses it. Otherwise starts own tx only when mutationLog is set.
// When mutationLog is nil, fn is called with the existing executor directly.
func (s *Store) withMutTx(ctx context.Context, fn func(ctx context.Context, exec sqlExecutor) error) error {
	exec := s.executor(ctx)
	if s.mutationLog == nil {
		return fn(ctx, exec)
	}
	if _, isExt := exec.(*sql.Tx); isExt {
		return fn(ctx, exec)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("ledger/sqlite: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck
	ctx = ledger.WithTx(ctx, tx)
	if err := fn(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

// Append adds entries to the named stream. Returns IDs of newly appended entries.
func (s *Store) Append(ctx context.Context, stream string, entries ...ledger.RawEntry[json.RawMessage]) ([]int64, error) {
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
			return nil, fmt.Errorf("ledger/sqlite: begin tx: %w", err)
		}
		defer ownTx.Rollback() //nolint:errcheck
		exec = ownTx
	}

	query := fmt.Sprintf(
		`INSERT OR IGNORE INTO %s (stream, payload, order_key, dedup_key, schema_version, metadata, tags, source_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		s.table,
	)
	stmt, err := exec.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ledger/sqlite: prepare: %w", err)
	}
	defer stmt.Close()

	type appendedEntry struct {
		id int64
		e  ledger.RawEntry[json.RawMessage]
	}
	var appended []appendedEntry

	var ids []int64
	for _, e := range entries {
		meta, err := encodeNullableJSON(e.Metadata)
		if err != nil {
			return nil, fmt.Errorf("ledger/sqlite: encode metadata: %w", err)
		}
		tagsJSON, err := json.Marshal(e.Tags)
		if err != nil {
			return nil, fmt.Errorf("ledger/sqlite: encode tags: %w", err)
		}
		if e.Tags == nil {
			tagsJSON = []byte("[]")
		}

		res, err := stmt.ExecContext(ctx, stream, []byte(e.Payload), e.OrderKey, e.DedupKey, e.SchemaVersion, meta, string(tagsJSON), e.SourceID)
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
			appended = append(appended, appendedEntry{id: id, e: e})
		} else if e.DedupKey != "" {
			s.logger.DebugContext(ctx, "dedup skip", "stream", stream, "dedup_key", e.DedupKey)
		}
	}

	if s.mutationLog != nil && len(appended) > 0 {
		evtEntries := make([]internalReplication.EventEntry, len(appended))
		for i, ae := range appended {
			evtEntries[i] = internalReplication.EventEntry{
				ID:            strconv.FormatInt(ae.id, 10),
				Payload:       json.RawMessage(ae.e.Payload),
				OrderKey:      ae.e.OrderKey,
				DedupKey:      ae.e.DedupKey,
				SchemaVersion: ae.e.SchemaVersion,
				Metadata:      ae.e.Metadata,
				Tags:          ae.e.Tags,
			}
		}
		evt := internalReplication.Event{
			Type:    internalReplication.TypeAppend,
			Stream:  stream,
			Entries: evtEntries,
		}
		evtData, err := json.Marshal(evt)
		if err != nil {
			return nil, fmt.Errorf("ledger/sqlite: encode mutation event: %w", err)
		}
		var mutCtx context.Context
		if ownTx != nil {
			mutCtx = ledger.WithTx(ctx, ownTx)
		} else {
			mutCtx = ctx
		}
		if _, err := s.mutationLog.Append(mutCtx, internalReplication.MutationStream, ledger.RawEntry[json.RawMessage]{Payload: evtData, SchemaVersion: 1}); err != nil {
			return nil, fmt.Errorf("ledger/sqlite: append mutation: %w", err)
		}
	}

	if ownTx != nil {
		if err := ownTx.Commit(); err != nil {
			return nil, fmt.Errorf("ledger/sqlite: commit: %w", err)
		}
	}
	return ids, nil
}

// Read returns entries from the named stream.
func (s *Store) Read(ctx context.Context, stream string, opts ...ledger.ReadOption) ([]ledger.StoredEntry[int64, json.RawMessage], error) {
	if s.closed.Load() {
		return nil, ledger.ErrStoreClosed
	}

	exec := s.executor(ctx)
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

	if tag := o.Tag(); tag != "" {
		clauses = append(clauses, "EXISTS (SELECT 1 FROM json_each(tags) WHERE json_each.value = ?)")
		args = append(args, tag)
	}
	for _, tag := range o.AllTags() {
		clauses = append(clauses, "EXISTS (SELECT 1 FROM json_each(tags) WHERE json_each.value = ?)")
		args = append(args, tag)
	}

	dir := "ASC"
	if o.Order() == ledger.Descending {
		dir = "DESC"
	}

	query := fmt.Sprintf(
		`SELECT id, stream, payload, order_key, dedup_key, schema_version, metadata, tags, annotations, created_at, updated_at FROM %s WHERE %s ORDER BY id %s LIMIT ?`,
		s.table, strings.Join(clauses, " AND "), dir,
	)
	args = append(args, o.Limit())

	rows, err := exec.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("ledger/sqlite: query: %w", err)
	}
	defer rows.Close()

	var entries []ledger.StoredEntry[int64, json.RawMessage]
	for rows.Next() {
		var (
			e            ledger.StoredEntry[int64, json.RawMessage]
			payloadBytes []byte
			meta         sql.NullString
			tagsJSON     string
			annotations  sql.NullString
			createdAt    string
			updatedAt    sql.NullString
		)
		if err := rows.Scan(&e.ID, &e.Stream, &payloadBytes, &e.OrderKey, &e.DedupKey, &e.SchemaVersion, &meta, &tagsJSON, &annotations, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("ledger/sqlite: scan: %w", err)
		}
		e.Payload = json.RawMessage(payloadBytes)
		if meta.Valid {
			m, err := decodeStringMap(meta.String)
			if err != nil {
				return nil, fmt.Errorf("ledger/sqlite: decode metadata: %w", err)
			}
			e.Metadata = m
		}
		if err := json.Unmarshal([]byte(tagsJSON), &e.Tags); err != nil {
			return nil, fmt.Errorf("ledger/sqlite: decode tags: %w", err)
		}
		if annotations.Valid {
			m, err := decodeStringMap(annotations.String)
			if err != nil {
				return nil, fmt.Errorf("ledger/sqlite: decode annotations: %w", err)
			}
			e.Annotations = m
		}
		t, err := time.Parse("2006-01-02T15:04:05.000", createdAt)
		if err != nil {
			return nil, fmt.Errorf("ledger/sqlite: parse created_at: %w", err)
		}
		e.CreatedAt = t
		if updatedAt.Valid {
			ut, err := time.Parse("2006-01-02T15:04:05.000", updatedAt.String)
			if err != nil {
				return nil, fmt.Errorf("ledger/sqlite: parse updated_at: %w", err)
			}
			e.UpdatedAt = &ut
		}
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
	exec := s.executor(ctx)
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE stream = ?`, s.table)
	var count int64
	if err := exec.QueryRowContext(ctx, query, stream).Scan(&count); err != nil {
		return 0, fmt.Errorf("ledger/sqlite: count: %w", err)
	}
	return count, nil
}

// SetTags replaces all tags on an entry.
func (s *Store) SetTags(ctx context.Context, stream string, id int64, tags []string) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
	}
	if s.appendOnly {
		return ledger.ErrNotSupported
	}
	if tags == nil {
		tags = []string{}
	}
	tagsJSON, err := json.Marshal(tags)
	if err != nil {
		return fmt.Errorf("ledger/sqlite: encode tags: %w", err)
	}
	return s.withMutTx(ctx, func(ctx context.Context, exec sqlExecutor) error {
		query := fmt.Sprintf(`UPDATE %s SET tags = ?, updated_at = strftime('%%Y-%%m-%%dT%%H:%%M:%%f','now') WHERE stream = ? AND id = ?`, s.table)
		res, err := exec.ExecContext(ctx, query, string(tagsJSON), stream, id)
		if err != nil {
			return fmt.Errorf("ledger/sqlite: set tags: %w", err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return ledger.ErrEntryNotFound
		}
		if s.mutationLog != nil {
			evt := internalReplication.Event{
				Type:    internalReplication.TypeSetTags,
				Stream:  stream,
				EntryID: strconv.FormatInt(id, 10),
				Tags:    tags,
			}
			data, _ := json.Marshal(evt)
			if _, err := s.mutationLog.Append(ctx, internalReplication.MutationStream, ledger.RawEntry[json.RawMessage]{Payload: data, SchemaVersion: 1}); err != nil {
				return fmt.Errorf("ledger/sqlite: append set_tags mutation: %w", err)
			}
		}
		return nil
	})
}

// SetAnnotations merges annotations into an entry. Keys with nil values are deleted.
func (s *Store) SetAnnotations(ctx context.Context, stream string, id int64, annotations map[string]*string) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
	}
	if s.appendOnly {
		return ledger.ErrNotSupported
	}
	return s.withMutTx(ctx, func(ctx context.Context, exec sqlExecutor) error {
		// Read current annotations.
		query := fmt.Sprintf(`SELECT annotations FROM %s WHERE stream = ? AND id = ?`, s.table)
		var raw sql.NullString
		if err := exec.QueryRowContext(ctx, query, stream, id).Scan(&raw); err != nil {
			if err == sql.ErrNoRows {
				return ledger.ErrEntryNotFound
			}
			return fmt.Errorf("ledger/sqlite: read annotations: %w", err)
		}

		current := make(map[string]string)
		if raw.Valid {
			json.Unmarshal([]byte(raw.String), &current) //nolint:errcheck
		}

		// Merge: set or delete.
		for k, v := range annotations {
			if v == nil {
				delete(current, k)
			} else {
				current[k] = *v
			}
		}

		data, err := json.Marshal(current)
		if err != nil {
			return fmt.Errorf("ledger/sqlite: encode annotations: %w", err)
		}

		update := fmt.Sprintf(`UPDATE %s SET annotations = ?, updated_at = strftime('%%Y-%%m-%%dT%%H:%%M:%%f','now') WHERE stream = ? AND id = ?`, s.table)
		_, err = exec.ExecContext(ctx, update, string(data), stream, id)
		if err != nil {
			return fmt.Errorf("ledger/sqlite: set annotations: %w", err)
		}

		if s.mutationLog != nil {
			evt := internalReplication.Event{
				Type:        internalReplication.TypeSetAnnotations,
				Stream:      stream,
				EntryID:     strconv.FormatInt(id, 10),
				Annotations: annotations,
			}
			evtData, _ := json.Marshal(evt)
			if _, err := s.mutationLog.Append(ctx, internalReplication.MutationStream, ledger.RawEntry[json.RawMessage]{Payload: evtData, SchemaVersion: 1}); err != nil {
				return fmt.Errorf("ledger/sqlite: append set_annotations mutation: %w", err)
			}
		}
		return nil
	})
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
		`SELECT DISTINCT stream FROM %s WHERE stream > ? ORDER BY stream ASC LIMIT ?`,
		s.table,
	)
	rows, err := exec.QueryContext(ctx, query, o.After(), o.Limit())
	if err != nil {
		return nil, fmt.Errorf("ledger/sqlite: list stream ids: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("ledger/sqlite: scan stream id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ledger/sqlite: list stream ids iteration: %w", err)
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
	var n int64
	err := s.withMutTx(ctx, func(ctx context.Context, exec sqlExecutor) error {
		query := fmt.Sprintf(`DELETE FROM %s WHERE stream = ? AND id <= ?`, s.table)
		res, err := exec.ExecContext(ctx, query, stream, beforeID)
		if err != nil {
			return fmt.Errorf("ledger/sqlite: trim: %w", err)
		}
		n, err = res.RowsAffected()
		if err != nil {
			return fmt.Errorf("ledger/sqlite: trim rows affected: %w", err)
		}
		if s.mutationLog != nil {
			evt := internalReplication.Event{
				Type:     internalReplication.TypeTrim,
				Stream:   stream,
				BeforeID: strconv.FormatInt(beforeID, 10),
			}
			data, _ := json.Marshal(evt)
			if _, err := s.mutationLog.Append(ctx, internalReplication.MutationStream, ledger.RawEntry[json.RawMessage]{Payload: data, SchemaVersion: 1}); err != nil {
				return fmt.Errorf("ledger/sqlite: append trim mutation: %w", err)
			}
		}
		return nil
	})
	return n, err
}

// FindBySourceID resolves a sink entry ID from its replication source ID.
func (s *Store) FindBySourceID(ctx context.Context, stream, sourceID string) (int64, bool, error) {
	if s.closed.Load() {
		return 0, false, ledger.ErrStoreClosed
	}
	exec := s.executor(ctx)
	query := fmt.Sprintf(`SELECT id FROM %s WHERE stream = ? AND source_id = ? LIMIT 1`, s.table)
	var id int64
	err := exec.QueryRowContext(ctx, query, stream, sourceID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("ledger/sqlite: find by source id: %w", err)
	}
	return id, true, nil
}

// GetCursor returns the persisted replication cursor for the given name.
func (s *Store) GetCursor(ctx context.Context, name string) (string, bool, error) {
	if s.closed.Load() {
		return "", false, ledger.ErrStoreClosed
	}
	var cursor string
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT cursor FROM %s_cursors WHERE name = ?`, s.table), name).Scan(&cursor)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("ledger/sqlite: get cursor: %w", err)
	}
	return cursor, true, nil
}

// SetCursor persists the replication cursor for the given name.
func (s *Store) SetCursor(ctx context.Context, name, cursor string) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
	}
	query := fmt.Sprintf(`INSERT INTO %s_cursors (name, cursor) VALUES (?, ?) ON CONFLICT (name) DO UPDATE SET cursor = excluded.cursor`, s.table) // #nosec G201 -- table name validated by ValidateName
	if _, err := s.db.ExecContext(ctx, query, name, cursor); err != nil {
		return fmt.Errorf("ledger/sqlite: set cursor: %w", err)
	}
	return nil
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

func encodeNullableJSON(m map[string]string) (sql.NullString, error) {
	if len(m) == 0 {
		return sql.NullString{}, nil
	}
	data, err := json.Marshal(m)
	if err != nil {
		return sql.NullString{}, err
	}
	return sql.NullString{String: string(data), Valid: true}, nil
}

func decodeStringMap(s string) (map[string]string, error) {
	var m map[string]string
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, err
	}
	return m, nil
}
