// Package clickhouse provides a ClickHouse-backed ledger store.
//
// ClickHouse is an append-only analytics backend. SetTags and SetAnnotations
// are not supported and always return [ledger.ErrNotSupported]. Trim uses
// ClickHouse lightweight deletes (requires ClickHouse 22.8+) and is
// asynchronous — deleted rows are physically removed during the next merge.
//
// # Bridge sink
//
// The store implements [ledger.CursorStore] and [ledger.SourceIDLookup] so it
// can be used as a sink with [bridge.Bridge]. Always pair it with
// [bridge.WithSkipMutationTypes](MutationSetTags, MutationSetAnnotations)
// on the Bridge, and create the source store with [WithAppendOnly] (or the
// equivalent option on the source backend) to prevent the source from producing
// mutation events that the sink cannot apply.
//
// # Ordering and pagination
//
// Entries are stored with a client-generated time-ordered ID. When used as a
// replication sink, the source_id (string-encoded source entry ID) is used as
// the ClickHouse row ID. For SQL sources (int64 source IDs encoded as decimal
// strings), lexicographic ordering of IDs may not match insertion order;
// prefer querying ClickHouse directly via SQL for analytics workloads.
package clickhouse

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // register "clickhouse" driver
	"github.com/rbaliyan/ledger"
)

var (
	_ ledger.Store[string, json.RawMessage] = (*Store)(nil)
	_ ledger.HealthChecker                  = (*Store)(nil)
	_ ledger.CursorStore                    = (*Store)(nil)
	_ ledger.SourceIDLookup[string]         = (*Store)(nil)
)

// Store is a ClickHouse ledger store. SetTags and SetAnnotations always return
// [ledger.ErrNotSupported].
type Store struct {
	db     *sql.DB
	table  string
	logger *slog.Logger
	closed atomic.Bool
}

// Option configures the ClickHouse store.
type Option func(*options)

type options struct {
	table  string
	logger *slog.Logger
}

// WithTable sets the table name. Defaults to "ledger_entries".
// The name must be a valid identifier (alphanumeric and underscore only).
func WithTable(name string) Option {
	return func(o *options) { o.table = name }
}

// WithLogger sets the structured logger. Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// New creates a ClickHouse ledger store. The table and cursors table are
// created automatically. db must be opened with the "clickhouse" driver
// (github.com/ClickHouse/clickhouse-go/v2).
func New(ctx context.Context, db *sql.DB, opts ...Option) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("ledger/clickhouse: db must not be nil")
	}
	o := options{table: "ledger_entries", logger: slog.Default()}
	for _, fn := range opts {
		fn(&o)
	}
	if err := ledger.ValidateName(o.table); err != nil {
		return nil, fmt.Errorf("ledger/clickhouse: %w", err)
	}
	s := &Store{db: db, table: o.table, logger: o.logger}
	if err := s.createTable(ctx); err != nil {
		return nil, fmt.Errorf("ledger/clickhouse: create table: %w", err)
	}
	return s, nil
}

// Type returns the table name. Intended for logging/tracing.
func (s *Store) Type() string { return s.table }

func (s *Store) createTable(ctx context.Context) error {
	// Main entries table — MergeTree, ordered by (stream, id).
	// id is a client-generated time-ordered hex string.
	entries := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    id             String,
    stream         LowCardinality(String),
    payload        String,
    order_key      String        DEFAULT '',
    dedup_key      String        DEFAULT '',
    schema_version Int32         DEFAULT 1,
    metadata       String        DEFAULT '{}',
    tags           String        DEFAULT '[]',
    source_id      String        DEFAULT '',
    created_at     DateTime64(3, 'UTC')
) ENGINE = MergeTree()
ORDER BY (stream, id)
SETTINGS index_granularity = 8192`, s.table) // #nosec G201 -- table name validated by ValidateName
	if _, err := s.db.ExecContext(ctx, entries); err != nil {
		return fmt.Errorf("create entries table: %w", err)
	}

	// Cursors table — ReplacingMergeTree gives upsert semantics.
	cursors := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s_cursors (
    name        String,
    cursor      String,
    updated_at  DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY name`, s.table) // #nosec G201 -- table name validated by ValidateName
	if _, err := s.db.ExecContext(ctx, cursors); err != nil {
		return fmt.Errorf("create cursors table: %w", err)
	}
	return nil
}

// generateID returns a time-ordered hex string suitable for use as a row ID.
// Format: 16-char nanosecond timestamp + 16-char random suffix (32 chars total, 64 bits of entropy).
func generateID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	ts := uint64(time.Now().UnixNano())
	return fmt.Sprintf("%016x%016x", ts, binary.BigEndian.Uint64(b[:]))
}

// Append inserts entries into the named stream as a single batch transaction.
// Tags are stored as a JSON array string. The returned IDs are the ClickHouse
// row IDs (source_id when replicating, otherwise a generated time-ordered ID).
func (s *Store) Append(ctx context.Context, stream string, entries ...ledger.RawEntry[json.RawMessage]) ([]string, error) {
	if s.closed.Load() {
		return nil, ledger.ErrStoreClosed
	}
	if len(entries) == 0 {
		return nil, nil
	}

	query := fmt.Sprintf( // #nosec G201 -- table name validated by ValidateName
		`INSERT INTO %s (id, stream, payload, order_key, dedup_key, schema_version, metadata, tags, source_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		s.table,
	)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("ledger/clickhouse: begin tx: %w", err)
	}
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		tx.Rollback() //nolint:errcheck
		return nil, fmt.Errorf("ledger/clickhouse: prepare: %w", err)
	}
	defer stmt.Close()

	now := time.Now().UTC()
	ids := make([]string, len(entries))
	for i, e := range entries {
		id := e.SourceID
		if id == "" {
			id = generateID()
		}
		ids[i] = id

		metadata := "{}"
		if len(e.Metadata) > 0 {
			if b, err := json.Marshal(e.Metadata); err == nil {
				metadata = string(b)
			}
		}
		tags := "[]"
		if len(e.Tags) > 0 {
			if b, err := json.Marshal(e.Tags); err == nil {
				tags = string(b)
			}
		}

		if _, err := stmt.ExecContext(ctx, id, stream, string(e.Payload),
			e.OrderKey, e.DedupKey, e.SchemaVersion, metadata, tags, e.SourceID, now); err != nil {
			tx.Rollback() //nolint:errcheck
			return nil, fmt.Errorf("ledger/clickhouse: append entry %d: %w", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("ledger/clickhouse: commit: %w", err)
	}
	return ids, nil
}

// Read returns entries from the named stream.
func (s *Store) Read(ctx context.Context, stream string, opts ...ledger.ReadOption) ([]ledger.StoredEntry[string, json.RawMessage], error) {
	if s.closed.Load() {
		return nil, ledger.ErrStoreClosed
	}
	o := ledger.ApplyReadOptions(opts...)

	var sb strings.Builder
	args := make([]any, 0, 4)

	fmt.Fprintf(&sb, `SELECT id, stream, payload, order_key, dedup_key, schema_version, metadata, tags, source_id, created_at FROM %s WHERE stream = ?`, s.table) // #nosec G201
	args = append(args, stream)

	if o.HasAfter() {
		after, ok := ledger.AfterValue[string](o)
		if !ok {
			return nil, fmt.Errorf("%w: expected string", ledger.ErrInvalidCursor)
		}
		if o.Order() == ledger.Descending {
			sb.WriteString(" AND id < ?")
		} else {
			sb.WriteString(" AND id > ?")
		}
		args = append(args, after)
	}
	if key := o.OrderKeyFilter(); key != "" {
		sb.WriteString(" AND order_key = ?")
		args = append(args, key)
	}
	if tag := o.Tag(); tag != "" {
		sb.WriteString(" AND has(JSONExtractArrayRaw(tags), ?)")
		args = append(args, fmt.Sprintf(`"%s"`, tag))
	}

	if o.Order() == ledger.Descending {
		sb.WriteString(" ORDER BY id DESC")
	} else {
		sb.WriteString(" ORDER BY id ASC")
	}
	fmt.Fprintf(&sb, " LIMIT %d", o.Limit())

	rows, err := s.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("ledger/clickhouse: read: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	var result []ledger.StoredEntry[string, json.RawMessage]
	for rows.Next() {
		var (
			id, stream_, payload, orderKey, dedupKey string
			schemaVersion                            int
			metadata, tags, sourceID                 string
			createdAt                                time.Time
		)
		if err := rows.Scan(&id, &stream_, &payload, &orderKey, &dedupKey,
			&schemaVersion, &metadata, &tags, &sourceID, &createdAt); err != nil {
			return nil, fmt.Errorf("ledger/clickhouse: scan: %w", err)
		}
		var meta map[string]string
		_ = json.Unmarshal([]byte(metadata), &meta)
		var tagSlice []string
		_ = json.Unmarshal([]byte(tags), &tagSlice)

		result = append(result, ledger.StoredEntry[string, json.RawMessage]{
			ID:            id,
			Stream:        stream_,
			Payload:       json.RawMessage(payload),
			OrderKey:      orderKey,
			DedupKey:      dedupKey,
			SchemaVersion: schemaVersion,
			Metadata:      meta,
			Tags:          tagSlice,
			CreatedAt:     createdAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ledger/clickhouse: rows: %w", err)
	}
	return result, nil
}

// Count returns the number of entries in the named stream.
func (s *Store) Count(ctx context.Context, stream string) (int64, error) {
	if s.closed.Load() {
		return 0, ledger.ErrStoreClosed
	}
	var n int64
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT count() FROM %s WHERE stream = ?`, s.table), // #nosec G201
		stream,
	).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("ledger/clickhouse: count: %w", err)
	}
	return n, nil
}

// SetTags is not supported by the ClickHouse backend.
// Always returns [ledger.ErrNotSupported].
func (s *Store) SetTags(_ context.Context, _, _ string, _ []string) error {
	return ledger.ErrNotSupported
}

// SetAnnotations is not supported by the ClickHouse backend.
// Always returns [ledger.ErrNotSupported].
func (s *Store) SetAnnotations(_ context.Context, _, _ string, _ map[string]*string) error {
	return ledger.ErrNotSupported
}

// Trim deletes entries with IDs lexicographically less than or equal to beforeID.
// ClickHouse lightweight deletes are asynchronous; rows are physically removed
// during the next background merge. Requires ClickHouse 22.8+.
func (s *Store) Trim(ctx context.Context, stream, beforeID string) (int64, error) {
	if s.closed.Load() {
		return 0, ledger.ErrStoreClosed
	}
	var count int64
	if err := s.db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT count() FROM %s WHERE stream = ? AND id <= ?`, s.table), // #nosec G201
		stream, beforeID,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("ledger/clickhouse: trim count: %w", err)
	}
	if count == 0 {
		return 0, nil
	}
	if _, err := s.db.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE stream = ? AND id <= ?`, s.table), // #nosec G201
		stream, beforeID,
	); err != nil {
		return 0, fmt.Errorf("ledger/clickhouse: trim: %w", err)
	}
	return count, nil
}

// ListStreamIDs returns distinct stream IDs in this store.
func (s *Store) ListStreamIDs(ctx context.Context, opts ...ledger.ListOption) ([]string, error) {
	if s.closed.Load() {
		return nil, ledger.ErrStoreClosed
	}
	o := ledger.ApplyListOptions(opts...)
	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf( // #nosec G201
			`SELECT DISTINCT stream FROM %s WHERE stream > ? ORDER BY stream ASC LIMIT ?`,
			s.table,
		),
		o.After(), int64(o.Limit()),
	)
	if err != nil {
		return nil, fmt.Errorf("ledger/clickhouse: list stream ids: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("ledger/clickhouse: scan stream id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// FindBySourceID resolves a sink entry ID from its replication source ID.
func (s *Store) FindBySourceID(ctx context.Context, stream, sourceID string) (string, bool, error) {
	if s.closed.Load() {
		return "", false, ledger.ErrStoreClosed
	}
	var id string
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT id FROM %s WHERE stream = ? AND source_id = ? LIMIT 1`, s.table), // #nosec G201
		stream, sourceID,
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("ledger/clickhouse: find by source id: %w", err)
	}
	return id, true, nil
}

// GetCursor returns the persisted replication cursor for the given name.
func (s *Store) GetCursor(ctx context.Context, name string) (string, bool, error) {
	if s.closed.Load() {
		return "", false, ledger.ErrStoreClosed
	}
	var cursor string
	// FINAL ensures we read the latest version from ReplacingMergeTree.
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT cursor FROM %s_cursors FINAL WHERE name = ?`, s.table), // #nosec G201
		name,
	).Scan(&cursor)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("ledger/clickhouse: get cursor: %w", err)
	}
	return cursor, true, nil
}

// SetCursor persists the replication cursor for the given name.
// The cursor is only advanced — if the stored cursor is already at or past the
// given value, the write is skipped. This prevents a lagging Bridge instance from
// regressing the cursor set by a faster instance.
//
// ClickHouse ReplacingMergeTree has no conditional upsert, so advancement is
// checked with a preceding read. The read-then-write window is a best-effort
// guard; the Bridge's application-level check in advanceCursor provides an
// additional layer of protection.
func (s *Store) SetCursor(ctx context.Context, name, cursor string) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
	}
	current, ok, err := s.GetCursor(ctx, name)
	if err != nil {
		return fmt.Errorf("ledger/clickhouse: check cursor: %w", err)
	}
	if ok && current >= cursor {
		return nil
	}
	_, err = s.db.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO %s_cursors (name, cursor, updated_at) VALUES (?, ?, ?)`, s.table), // #nosec G201
		name, cursor, time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("ledger/clickhouse: set cursor: %w", err)
	}
	return nil
}

// Health checks ClickHouse connectivity.
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
