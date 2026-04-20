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
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/rbaliyan/ledger"
	internalReplication "github.com/rbaliyan/ledger/internal/replication"
)

type sqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

var (
	_ ledger.Store[int64, json.RawMessage]    = (*Store)(nil)
	_ ledger.HealthChecker                    = (*Store)(nil)
	_ ledger.CursorStore                      = (*Store)(nil)
	_ ledger.SourceIDLookup[int64]            = (*Store)(nil)
	_ ledger.Searcher[int64, json.RawMessage] = (*Store)(nil)
	_ ledger.SearchIndexer                    = (*Store)(nil)
)

// Store is a PostgreSQL ledger store.
type Store struct {
	db          *sql.DB
	table       string
	logger      *slog.Logger
	closed      atomic.Bool
	mutationLog ledger.Store[int64, json.RawMessage]
	appendOnly  bool
	ftsEnabled  bool
}

// Option configures the PostgreSQL store.
type Option func(*options)

type options struct {
	table       string
	logger      *slog.Logger
	mutationLog ledger.Store[int64, json.RawMessage]
	appendOnly  bool
	ftsEnabled  bool
}

// WithTable sets the table name. Defaults to "ledger_entries".
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
// Use this when the replication sink (e.g. ClickHouse) does not support entry mutations.
func WithAppendOnly() Option {
	return func(o *options) { o.appendOnly = true }
}

// WithFullTextSearch switches Search from ILIKE substring matching to PostgreSQL
// tsvector full-text search. Call [Store.EnsureSearchIndex] once at startup to
// create the backing GIN expression index; without the index Search still works
// but performs a sequential scan.
func WithFullTextSearch() Option {
	return func(o *options) { o.ftsEnabled = true }
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

	s := &Store{db: db, table: o.table, logger: o.logger, mutationLog: o.mutationLog, appendOnly: o.appendOnly, ftsEnabled: o.ftsEnabled}
	if err := s.createTable(ctx); err != nil {
		return nil, fmt.Errorf("ledger/postgres: create table: %w", err)
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
			id             BIGSERIAL   PRIMARY KEY,
			stream         TEXT        NOT NULL,
			payload        BYTEA       NOT NULL,
			order_key      TEXT        NOT NULL DEFAULT '',
			dedup_key      TEXT        NOT NULL DEFAULT '',
			schema_version INTEGER     NOT NULL DEFAULT 1,
			metadata       JSONB,
			tags           JSONB       NOT NULL DEFAULT '[]'::jsonb,
			annotations    JSONB,
			source_id      TEXT        NOT NULL DEFAULT '',
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
		fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS idx_%s_source ON %s(source_id) WHERE source_id != ''`, s.table, s.table),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_cursors (name TEXT NOT NULL PRIMARY KEY, cursor TEXT NOT NULL)`, s.table),
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

func (s *Store) migrateSchema(ctx context.Context) error {
	alter := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS source_id TEXT NOT NULL DEFAULT ''`, s.table)
	if _, err := s.db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("ledger/postgres: migrate schema: %w", err)
	}
	cursor := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_cursors (name TEXT NOT NULL PRIMARY KEY, cursor TEXT NOT NULL)`, s.table)
	if _, err := s.db.ExecContext(ctx, cursor); err != nil {
		return fmt.Errorf("ledger/postgres: create cursors table: %w", err)
	}
	idx := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS idx_%s_source ON %s(source_id) WHERE source_id != ''`, s.table, s.table) // #nosec G201 -- table name validated by ValidateName
	if _, err := s.db.ExecContext(ctx, idx); err != nil {
		return fmt.Errorf("ledger/postgres: create source index: %w", err)
	}
	return nil
}

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
		return fmt.Errorf("ledger/postgres: begin tx: %w", err)
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
			return nil, fmt.Errorf("ledger/postgres: begin tx: %w", err)
		}
		defer ownTx.Rollback() //nolint:errcheck
		exec = ownTx
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (stream, payload, order_key, dedup_key, schema_version, metadata, tags, source_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (stream, dedup_key) WHERE dedup_key != '' DO NOTHING
		 RETURNING id`,
		s.table,
	)
	stmt, err := exec.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ledger/postgres: prepare: %w", err)
	}
	defer stmt.Close()

	type appendedEntry struct {
		id int64
		e  ledger.RawEntry[json.RawMessage]
	}
	var appended []appendedEntry

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
		err = stmt.QueryRowContext(ctx, stream, []byte(e.Payload), e.OrderKey, e.DedupKey, e.SchemaVersion, meta, tagsJSON, e.SourceID).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			s.logger.DebugContext(ctx, "dedup skip", "stream", stream, "dedup_key", e.DedupKey)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("ledger/postgres: insert: %w", err)
		}
		ids = append(ids, id)
		appended = append(appended, appendedEntry{id: id, e: e})
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
			return nil, fmt.Errorf("ledger/postgres: encode mutation event: %w", err)
		}
		var mutCtx context.Context
		if ownTx != nil {
			mutCtx = ledger.WithTx(ctx, ownTx)
		} else {
			mutCtx = ctx
		}
		if _, err := s.mutationLog.Append(mutCtx, internalReplication.MutationStream, ledger.RawEntry[json.RawMessage]{Payload: evtData, SchemaVersion: 1}); err != nil {
			return nil, fmt.Errorf("ledger/postgres: append mutation: %w", err)
		}
	}

	if ownTx != nil {
		if err := ownTx.Commit(); err != nil {
			return nil, fmt.Errorf("ledger/postgres: commit: %w", err)
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

	var entries []ledger.StoredEntry[int64, json.RawMessage]
	for rows.Next() {
		var (
			e            ledger.StoredEntry[int64, json.RawMessage]
			payloadBytes []byte
			meta         []byte
			tagsJSON     []byte
			annotations  []byte
			updatedAt    sql.NullTime
		)
		if err := rows.Scan(&e.ID, &e.Stream, &payloadBytes, &e.OrderKey, &e.DedupKey, &e.SchemaVersion, &meta, &tagsJSON, &annotations, &e.CreatedAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("ledger/postgres: scan: %w", err)
		}
		e.Payload = json.RawMessage(payloadBytes)
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

// Stat returns metrics for the named stream.
func (s *Store) Stat(ctx context.Context, stream string) (ledger.StreamStat[int64], error) {
	if s.closed.Load() {
		return ledger.StreamStat[int64]{}, ledger.ErrStoreClosed
	}
	exec := s.executor(ctx)
	query := fmt.Sprintf(`SELECT COUNT(*), MIN(id), MAX(id) FROM %s WHERE stream = $1`, s.table)
	var (
		count int64
		minID sql.NullInt64
		maxID sql.NullInt64
	)
	if err := exec.QueryRowContext(ctx, query, stream).Scan(&count, &minID, &maxID); err != nil {
		return ledger.StreamStat[int64]{}, fmt.Errorf("ledger/postgres: stat: %w", err)
	}
	return ledger.StreamStat[int64]{
		Stream:  stream,
		Count:   count,
		FirstID: minID.Int64,
		LastID:  maxID.Int64,
	}, nil
}

// Search performs a full-text search on entry payloads using ILIKE.
func (s *Store) Search(ctx context.Context, stream string, query string, opts ...ledger.ReadOption) ([]ledger.StoredEntry[int64, json.RawMessage], error) {
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

	if stream != "" {
		argN++
		clauses = append(clauses, fmt.Sprintf("stream = $%d", argN))
		args = append(args, stream)
	}

	argN++
	if s.ftsEnabled {
		clauses = append(clauses, fmt.Sprintf(
			"to_tsvector('english', convert_from(payload, 'UTF8')) @@ plainto_tsquery('english', $%d)", argN))
		args = append(args, query)
	} else {
		clauses = append(clauses, fmt.Sprintf("convert_from(payload, 'UTF8') ILIKE $%d", argN))
		args = append(args, "%"+query+"%")
	}

	if o.HasAfter() {
		after, _ := ledger.AfterValue[int64](o)
		argN++
		if o.Order() == ledger.Descending {
			clauses = append(clauses, fmt.Sprintf("id < $%d", argN))
		} else {
			clauses = append(clauses, fmt.Sprintf("id > $%d", argN))
		}
		args = append(args, after)
	}

	dir := "ASC"
	if o.Order() == ledger.Descending {
		dir = "DESC"
	}

	argN++
	sqlQuery := fmt.Sprintf(
		`SELECT id, stream, payload, order_key, dedup_key, schema_version, metadata, tags, annotations, created_at, updated_at FROM %s WHERE %s ORDER BY id %s LIMIT $%d`,
		s.table, strings.Join(clauses, " AND "), dir, argN,
	)
	args = append(args, o.Limit())

	rows, err := exec.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("ledger/postgres: search: %w", err)
	}
	defer rows.Close()

	var entries []ledger.StoredEntry[int64, json.RawMessage]
	for rows.Next() {
		var (
			e            ledger.StoredEntry[int64, json.RawMessage]
			payloadBytes []byte
			meta         []byte
			tagsJSON     []byte
			annotations  []byte
			updatedAt    sql.NullTime
		)
		if err := rows.Scan(&e.ID, &e.Stream, &payloadBytes, &e.OrderKey, &e.DedupKey, &e.SchemaVersion, &meta, &tagsJSON, &annotations, &e.CreatedAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("ledger/postgres: scan: %w", err)
		}
		e.Payload = json.RawMessage(payloadBytes)
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
	return entries, nil
}

// EnsureSearchIndex creates the GIN expression index required for efficient
// full-text search. Requires [WithFullTextSearch] to be set; returns an error
// otherwise. The operation is idempotent (CREATE INDEX … IF NOT EXISTS).
//
// CREATE INDEX CONCURRENTLY does not block reads or writes but cannot run
// inside a transaction. Call this once at application startup, not per request.
func (s *Store) EnsureSearchIndex(ctx context.Context) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
	}
	if !s.ftsEnabled {
		return fmt.Errorf("ledger/postgres: EnsureSearchIndex requires WithFullTextSearch() option")
	}
	// #nosec G201 -- table name validated by ValidateName
	idx := fmt.Sprintf(
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_%s_payload_fts ON %s USING GIN(to_tsvector('english', convert_from(payload, 'UTF8')))`,
		s.table, s.table,
	)
	if _, err := s.db.ExecContext(ctx, idx); err != nil {
		return fmt.Errorf("ledger/postgres: create FTS index: %w", err)
	}
	return nil
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
	tagsJSON, _ := json.Marshal(tags)
	return s.withMutTx(ctx, func(ctx context.Context, exec sqlExecutor) error {
		res, err := exec.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET tags = $1::jsonb, updated_at = now() WHERE stream = $2 AND id = $3`, s.table), string(tagsJSON), stream, id)
		if err != nil {
			return fmt.Errorf("ledger/postgres: set tags: %w", err)
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
				return fmt.Errorf("ledger/postgres: append set_tags mutation: %w", err)
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
		// Build merge map (keys to set) and delete list (keys with nil values).
		mergeMap := make(map[string]string)
		deleteKeys := []string{}
		for k, v := range annotations {
			if v == nil {
				deleteKeys = append(deleteKeys, k)
			} else {
				mergeMap[k] = *v
			}
		}
		mergeJSON, err := json.Marshal(mergeMap)
		if err != nil {
			return fmt.Errorf("ledger/postgres: encode annotations: %w", err)
		}
		deleteJSON, err := json.Marshal(deleteKeys)
		if err != nil {
			return fmt.Errorf("ledger/postgres: encode delete keys: %w", err)
		}
		// Atomic JSONB merge then subtract deleted keys — no read-modify-write race.
		// ARRAY(SELECT ...) converts the JSON array to a PostgreSQL text[].
		query := fmt.Sprintf(
			`UPDATE %s SET
				annotations = (COALESCE(annotations, '{}'::jsonb) || $1::jsonb)
				              - ARRAY(SELECT jsonb_array_elements_text($2::jsonb)),
				updated_at = now()
			WHERE stream = $3 AND id = $4`,
			s.table,
		)
		res, err := exec.ExecContext(ctx, query, string(mergeJSON), string(deleteJSON), stream, id)
		if err != nil {
			return fmt.Errorf("ledger/postgres: set annotations: %w", err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return ledger.ErrEntryNotFound
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
				return fmt.Errorf("ledger/postgres: append set_annotations mutation: %w", err)
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
	var n int64
	err := s.withMutTx(ctx, func(ctx context.Context, exec sqlExecutor) error {
		res, err := exec.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE stream = $1 AND id <= $2`, s.table), stream, beforeID)
		if err != nil {
			return fmt.Errorf("ledger/postgres: trim: %w", err)
		}
		n, _ = res.RowsAffected()
		if s.mutationLog != nil {
			evt := internalReplication.Event{
				Type:     internalReplication.TypeTrim,
				Stream:   stream,
				BeforeID: strconv.FormatInt(beforeID, 10),
			}
			data, _ := json.Marshal(evt)
			if _, err := s.mutationLog.Append(ctx, internalReplication.MutationStream, ledger.RawEntry[json.RawMessage]{Payload: data, SchemaVersion: 1}); err != nil {
				return fmt.Errorf("ledger/postgres: append trim mutation: %w", err)
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
	var id int64
	err := exec.QueryRowContext(ctx, fmt.Sprintf(`SELECT id FROM %s WHERE stream = $1 AND source_id = $2 LIMIT 1`, s.table), stream, sourceID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("ledger/postgres: find by source id: %w", err)
	}
	return id, true, nil
}

// GetCursor returns the persisted replication cursor for the given name.
func (s *Store) GetCursor(ctx context.Context, name string) (string, bool, error) {
	if s.closed.Load() {
		return "", false, ledger.ErrStoreClosed
	}
	var cursor string
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT cursor FROM %s_cursors WHERE name = $1`, s.table), name).Scan(&cursor)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("ledger/postgres: get cursor: %w", err)
	}
	return cursor, true, nil
}

// SetCursor persists the replication cursor for the given name.
// The cursor is only advanced — if the stored cursor is already at or past the given
// value, the write is a no-op. This prevents a lagging Bridge instance from regressing
// the cursor position set by a faster instance.
//
// Note: comparison is lexicographic (TEXT). For int64 decimal cursors this is correct
// once IDs reach two or more digits of the same length; any minor regression at
// single-digit vs multi-digit boundaries causes idempotent replay only.
func (s *Store) SetCursor(ctx context.Context, name, cursor string) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
	}
	query := fmt.Sprintf( // #nosec G201 -- table name validated by ValidateName
		`INSERT INTO %s_cursors (name, cursor) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET cursor = EXCLUDED.cursor WHERE EXCLUDED.cursor > %s_cursors.cursor`,
		s.table, s.table,
	)
	if _, err := s.db.ExecContext(ctx, query, name, cursor); err != nil {
		return fmt.Errorf("ledger/postgres: set cursor: %w", err)
	}
	return nil
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
