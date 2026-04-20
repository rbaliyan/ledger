// Package mongodb provides a MongoDB-backed ledger store.
//
// The store uses a single collection for all streams. Streams are created
// implicitly on first append — no collection setup is required per stream.
//
// The caller is responsible for managing the *mongo.Client lifecycle.
//
// Append semantics: Uses InsertMany with ordered:false. If a non-dedup error
// occurs, partial inserts may be committed. SQL backends use transactions for
// atomic batch inserts.
//
// Transaction support: pass a *mongo.Session via [ledger.WithTx] to have
// operations participate in an external MongoDB transaction.
package mongodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/ledger"
	internalReplication "github.com/rbaliyan/ledger/internal/replication"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	_ ledger.Store[string, bson.Raw] = (*Store)(nil)
	_ ledger.HealthChecker           = (*Store)(nil)
	_ ledger.CursorStore             = (*Store)(nil)
	_ ledger.SourceIDLookup[string]  = (*Store)(nil)
)

type entry struct {
	ID            bson.ObjectID     `bson:"_id,omitempty"`
	Stream        string            `bson:"stream"`
	Payload       bson.Raw          `bson:"payload"`
	OrderKey      string            `bson:"order_key"`
	DedupKey      string            `bson:"dedup_key"`
	SchemaVersion int               `bson:"schema_version"`
	Metadata      map[string]string `bson:"metadata,omitempty"`
	Tags          []string          `bson:"tags"`
	Annotations   map[string]string `bson:"annotations,omitempty"`
	SourceID      string            `bson:"source_id,omitempty"`
	CreatedAt     time.Time         `bson:"created_at"`
	UpdatedAt     *time.Time        `bson:"updated_at,omitempty"`
}

// Store is a MongoDB ledger store.
type Store struct {
	db          *mongo.Database
	coll        *mongo.Collection
	cursors     *mongo.Collection
	mutationLog ledger.Store[string, json.RawMessage]
	logger      *slog.Logger
	closed      atomic.Bool
	appendOnly  bool
}

// Option configures the MongoDB store.
type Option func(*options)

type options struct {
	collection  string
	logger      *slog.Logger
	mutationLog ledger.Store[string, json.RawMessage]
	appendOnly  bool
}

// WithCollection sets the collection name. Defaults to "ledger_entries".
func WithCollection(name string) Option {
	return func(o *options) { o.collection = name }
}

// WithLogger sets the structured logger. Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithAppendOnly disables SetTags and SetAnnotations, returning [ledger.ErrNotSupported].
// Use this when the replication sink (e.g. ClickHouse) does not support entry mutations.
func WithAppendOnly() Option {
	return func(o *options) { o.appendOnly = true }
}

// WithMutationLog enables mutation logging. Every successful write
// (Append, SetTags, SetAnnotations, Trim) is recorded as a JSON event in the
// mutation log so a [bridge.Bridge] can apply it to a sink store.
//
// Use [NewJSONStore] in the same MongoDB database to keep everything in one
// cluster. Mutation log writes share the session from [ledger.WithTx], so on
// replica-set clusters you can make them atomic by wrapping calls in a
// multi-document transaction. Without a transaction the writes are best-effort:
// a crash between the main write and the mutation log write will lose that
// event (the main data is still durable).
func WithMutationLog(mutLog ledger.Store[string, json.RawMessage]) Option {
	return func(o *options) { o.mutationLog = mutLog }
}

// New creates a new MongoDB ledger store. Indexes are created automatically.
// Tag multikey indexes are created asynchronously in the background.
func New(ctx context.Context, db *mongo.Database, opts ...Option) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("ledger/mongodb: db must not be nil")
	}
	o := options{collection: "ledger_entries", logger: slog.Default()}
	for _, fn := range opts {
		fn(&o)
	}
	if err := ledger.ValidateName(o.collection); err != nil {
		return nil, fmt.Errorf("ledger/mongodb: %w", err)
	}

	coll := db.Collection(o.collection)
	cursors := db.Collection(o.collection + "_cursors")
	s := &Store{db: db, coll: coll, cursors: cursors, mutationLog: o.mutationLog, logger: o.logger, appendOnly: o.appendOnly}
	if err := s.ensureIndexes(ctx); err != nil {
		return nil, fmt.Errorf("ledger/mongodb: ensure indexes: %w", err)
	}
	go s.createAsyncIndexes(o.logger)
	return s, nil
}

func (s *Store) ensureIndexes(ctx context.Context) error {
	models := []mongo.IndexModel{
		{Keys: bson.D{{Key: "stream", Value: 1}, {Key: "_id", Value: 1}}},
		{Keys: bson.D{{Key: "stream", Value: 1}, {Key: "order_key", Value: 1}, {Key: "_id", Value: 1}}},
		{
			Keys: bson.D{{Key: "stream", Value: 1}, {Key: "dedup_key", Value: 1}},
			Options: mongoopts.Index().
				SetUnique(true).
				SetPartialFilterExpression(bson.D{{Key: "dedup_key", Value: bson.D{{Key: "$gt", Value: ""}}}}),
		},
		// Single-field stream index supports DISTINCT_SCAN plan for ListStreamIDs;
		// cheap to maintain due to low cardinality of stream values.
		{Keys: bson.D{{Key: "stream", Value: 1}}},
		{
			Keys:    bson.D{{Key: "source_id", Value: 1}},
			Options: mongoopts.Index().SetSparse(true).SetUnique(true),
		},
	}
	_, err := s.coll.Indexes().CreateMany(ctx, models)
	return err
}

func (s *Store) createAsyncIndexes(logger *slog.Logger) {
	if s.closed.Load() {
		return
	}
	// Multikey index for tag-based filtering. Created async to avoid blocking startup.
	model := mongo.IndexModel{
		Keys: bson.D{{Key: "stream", Value: 1}, {Key: "tags", Value: 1}, {Key: "_id", Value: 1}},
	}
	if _, err := s.coll.Indexes().CreateOne(context.Background(), model); err != nil && !s.closed.Load() {
		logger.Warn("failed to create tags index", "error", err)
	}
}

// bsonToJSON converts a BSON document to relaxed Extended JSON so it can be
// stored in the mutation log and consumed by JSON-native sinks (e.g. ClickHouse).
// Relaxed mode produces natural JSON numbers instead of $numberInt/$numberLong wrappers.
func bsonToJSON(raw bson.Raw) (json.RawMessage, error) {
	b, err := bson.MarshalExtJSON(raw, false, false)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}

// writeMutationEvent appends a mutation event to the mutation log.
// Failures are logged as warnings; the main operation has already committed.
func (s *Store) writeMutationEvent(ctx context.Context, evt internalReplication.Event) {
	data, _ := json.Marshal(evt)
	if _, err := s.mutationLog.Append(ctx, internalReplication.MutationStream,
		ledger.RawEntry[json.RawMessage]{Payload: data, SchemaVersion: 1}); err != nil {
		s.logger.WarnContext(ctx, "mutation log write failed", "error", err, "type", evt.Type, "stream", evt.Stream)
	}
}

func (s *Store) sessionCtx(ctx context.Context) context.Context {
	if sess, ok := ledger.TxFromContext(ctx).(*mongo.Session); ok {
		return mongo.NewSessionContext(ctx, sess)
	}
	return ctx
}

// Append adds entries to the named stream.
func (s *Store) Append(ctx context.Context, stream string, entries ...ledger.RawEntry[bson.Raw]) ([]string, error) {
	if s.closed.Load() {
		return nil, ledger.ErrStoreClosed
	}
	if len(entries) == 0 {
		return nil, nil
	}

	ctx = s.sessionCtx(ctx)
	now := time.Now().UTC()
	docs := make([]entry, len(entries))
	for i, e := range entries {
		tags := e.Tags
		if tags == nil {
			tags = []string{}
		}
		docs[i] = entry{
			ID:            bson.NewObjectID(),
			Stream:        stream,
			Payload:       e.Payload,
			OrderKey:      e.OrderKey,
			DedupKey:      e.DedupKey,
			SchemaVersion: e.SchemaVersion,
			Metadata:      e.Metadata,
			Tags:          tags,
			SourceID:      e.SourceID,
			CreatedAt:     now,
		}
	}

	insertOpts := mongoopts.InsertMany().SetOrdered(false)
	_, err := s.coll.InsertMany(ctx, docs, insertOpts)

	failed := make(map[int]struct{})
	var appendErr error
	if err != nil {
		var bwe mongo.BulkWriteException
		if !errors.As(err, &bwe) {
			return nil, fmt.Errorf("ledger/mongodb: insert many: %w", err)
		}
		for _, we := range bwe.WriteErrors {
			failed[we.Index] = struct{}{}
			if we.HasErrorCode(11000) {
				s.logger.DebugContext(ctx, "dedup skip", "stream", stream, "index", we.Index)
			} else {
				appendErr = errors.Join(appendErr, fmt.Errorf("ledger/mongodb: insert at index %d code=%d: %s", we.Index, we.Code, we.Message))
			}
		}
	}

	ids := make([]string, 0, len(docs)-len(failed))
	for i, doc := range docs {
		if _, ok := failed[i]; !ok {
			ids = append(ids, doc.ID.Hex())
		}
	}

	if s.mutationLog != nil && len(ids) > 0 {
		evtEntries := make([]internalReplication.EventEntry, 0, len(ids))
		for i, doc := range docs {
			if _, skip := failed[i]; skip {
				continue
			}
			payload, err := bsonToJSON(doc.Payload)
			if err != nil {
				s.logger.WarnContext(ctx, "mutation log: bson→json transcode failed", "error", err, "stream", stream)
				continue
			}
			evtEntries = append(evtEntries, internalReplication.EventEntry{
				ID:            doc.ID.Hex(),
				Payload:       payload,
				OrderKey:      doc.OrderKey,
				DedupKey:      doc.DedupKey,
				SchemaVersion: doc.SchemaVersion,
				Metadata:      doc.Metadata,
				Tags:          doc.Tags,
			})
		}
		if len(evtEntries) > 0 {
			s.writeMutationEvent(ctx, internalReplication.Event{
				Type:    internalReplication.TypeAppend,
				Stream:  stream,
				Entries: evtEntries,
			})
		}
	}

	return ids, appendErr
}

// Read returns entries from the named stream.
func (s *Store) Read(ctx context.Context, stream string, opts ...ledger.ReadOption) ([]ledger.StoredEntry[string, bson.Raw], error) {
	if s.closed.Load() {
		return nil, ledger.ErrStoreClosed
	}

	ctx = s.sessionCtx(ctx)
	o := ledger.ApplyReadOptions(opts...)

	filter := bson.D{{Key: "stream", Value: stream}}

	if o.HasAfter() {
		after, ok := ledger.AfterValue[string](o)
		if !ok {
			return nil, fmt.Errorf("%w: expected string", ledger.ErrInvalidCursor)
		}
		oid, err := bson.ObjectIDFromHex(after)
		if err != nil {
			return nil, fmt.Errorf("ledger/mongodb: invalid cursor: %w", err)
		}
		if o.Order() == ledger.Descending {
			filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$lt", Value: oid}}})
		} else {
			filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$gt", Value: oid}}})
		}
	}

	if key := o.OrderKeyFilter(); key != "" {
		filter = append(filter, bson.E{Key: "order_key", Value: key})
	}

	if tag := o.Tag(); tag != "" {
		filter = append(filter, bson.E{Key: "tags", Value: tag})
	}
	if allTags := o.AllTags(); len(allTags) > 0 {
		filter = append(filter, bson.E{Key: "tags", Value: bson.D{{Key: "$all", Value: allTags}}})
	}

	sortDir := 1
	if o.Order() == ledger.Descending {
		sortDir = -1
	}

	findOpts := mongoopts.Find().
		SetSort(bson.D{{Key: "_id", Value: sortDir}}).
		SetLimit(int64(o.Limit()))

	cursor, err := s.coll.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, fmt.Errorf("ledger/mongodb: find: %w", err)
	}
	defer cursor.Close(ctx)

	var entries []ledger.StoredEntry[string, bson.Raw]
	for cursor.Next(ctx) {
		var doc entry
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("ledger/mongodb: decode: %w", err)
		}
		entries = append(entries, ledger.StoredEntry[string, bson.Raw]{
			ID:            doc.ID.Hex(),
			Stream:        doc.Stream,
			Payload:       doc.Payload,
			OrderKey:      doc.OrderKey,
			DedupKey:      doc.DedupKey,
			SchemaVersion: doc.SchemaVersion,
			Metadata:      doc.Metadata,
			Tags:          doc.Tags,
			Annotations:   doc.Annotations,
			CreatedAt:     doc.CreatedAt,
			UpdatedAt:     doc.UpdatedAt,
		})
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("ledger/mongodb: cursor: %w", err)
	}
	return entries, nil
}

// Count returns the number of entries in the named stream.
func (s *Store) Count(ctx context.Context, stream string) (int64, error) {
	if s.closed.Load() {
		return 0, ledger.ErrStoreClosed
	}
	ctx = s.sessionCtx(ctx)
	n, err := s.coll.CountDocuments(ctx, bson.D{{Key: "stream", Value: stream}})
	if err != nil {
		return 0, fmt.Errorf("ledger/mongodb: count: %w", err)
	}
	return n, nil
}

// Stat returns metrics for the named stream.
func (s *Store) Stat(ctx context.Context, stream string) (ledger.StreamStat[string], error) {
	if s.closed.Load() {
		return ledger.StreamStat[string]{}, ledger.ErrStoreClosed
	}
	ctx = s.sessionCtx(ctx)
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "stream", Value: stream}}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
			{Key: "min", Value: bson.D{{Key: "$min", Value: "$_id"}}},
			{Key: "max", Value: bson.D{{Key: "$max", Value: "$_id"}}},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return ledger.StreamStat[string]{}, fmt.Errorf("ledger/mongodb: stat: %w", err)
	}
	defer cursor.Close(ctx)

	if !cursor.Next(ctx) {
		return ledger.StreamStat[string]{Stream: stream}, nil
	}

	var res struct {
		Count int64         `bson:"count"`
		Min   bson.ObjectID `bson:"min"`
		Max   bson.ObjectID `bson:"max"`
	}
	if err := cursor.Decode(&res); err != nil {
		return ledger.StreamStat[string]{}, fmt.Errorf("ledger/mongodb: decode stat: %w", err)
	}
	return ledger.StreamStat[string]{
		Stream:  stream,
		Count:   res.Count,
		FirstID: res.Min.Hex(),
		LastID:  res.Max.Hex(),
	}, nil
}

// Search performs a full-text search on entry payloads using $text.
// Requires a text index on the payload field.
func (s *Store) Search(ctx context.Context, stream string, query string, opts ...ledger.ReadOption) ([]ledger.StoredEntry[string, bson.Raw], error) {
	if s.closed.Load() {
		return nil, ledger.ErrStoreClosed
	}

	ctx = s.sessionCtx(ctx)
	o := ledger.ApplyReadOptions(opts...)

	filter := bson.D{{Key: "$text", Value: bson.D{{Key: "$search", Value: query}}}}
	if stream != "" {
		filter = append(filter, bson.E{Key: "stream", Value: stream})
	}

	if o.HasAfter() {
		after, _ := ledger.AfterValue[string](o)
		oid, _ := bson.ObjectIDFromHex(after)
		if o.Order() == ledger.Descending {
			filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$lt", Value: oid}}})
		} else {
			filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$gt", Value: oid}}})
		}
	}

	sortDir := 1
	if o.Order() == ledger.Descending {
		sortDir = -1
	}

	findOpts := mongoopts.Find().
		SetSort(bson.D{{Key: "_id", Value: sortDir}}).
		SetLimit(int64(o.Limit()))

	cursor, err := s.coll.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, fmt.Errorf("ledger/mongodb: search: %w", err)
	}
	defer cursor.Close(ctx)

	var entries []ledger.StoredEntry[string, bson.Raw]
	for cursor.Next(ctx) {
		var doc entry
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("ledger/mongodb: decode: %w", err)
		}
		entries = append(entries, ledger.StoredEntry[string, bson.Raw]{
			ID:            doc.ID.Hex(),
			Stream:        doc.Stream,
			Payload:       doc.Payload,
			OrderKey:      doc.OrderKey,
			DedupKey:      doc.DedupKey,
			SchemaVersion: doc.SchemaVersion,
			Metadata:      doc.Metadata,
			Tags:          doc.Tags,
			Annotations:   doc.Annotations,
			CreatedAt:     doc.CreatedAt,
			UpdatedAt:     doc.UpdatedAt,
		})
	}
	return entries, nil
}

// SetTags replaces all tags on an entry.
func (s *Store) SetTags(ctx context.Context, stream string, id string, tags []string) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
	}
	if s.appendOnly {
		return ledger.ErrNotSupported
	}
	if tags == nil {
		tags = []string{}
	}
	ctx = s.sessionCtx(ctx)
	oid, err := bson.ObjectIDFromHex(id)
	if err != nil {
		return ledger.ErrEntryNotFound
	}
	now := time.Now().UTC()
	res, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "stream", Value: stream}, {Key: "_id", Value: oid}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "tags", Value: tags}, {Key: "updated_at", Value: now}}}},
	)
	if err != nil {
		return fmt.Errorf("ledger/mongodb: set tags: %w", err)
	}
	if res.MatchedCount == 0 {
		return ledger.ErrEntryNotFound
	}
	if s.mutationLog != nil {
		s.writeMutationEvent(ctx, internalReplication.Event{
			Type:    internalReplication.TypeSetTags,
			Stream:  stream,
			EntryID: id,
			Tags:    tags,
		})
	}
	return nil
}

// SetAnnotations merges annotations into an entry. Keys with nil values are deleted.
func (s *Store) SetAnnotations(ctx context.Context, stream string, id string, annotations map[string]*string) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
	}
	if s.appendOnly {
		return ledger.ErrNotSupported
	}
	ctx = s.sessionCtx(ctx)
	oid, err := bson.ObjectIDFromHex(id)
	if err != nil {
		return ledger.ErrEntryNotFound
	}

	setFields := bson.D{}
	unsetFields := bson.D{}
	for k, v := range annotations {
		if v == nil {
			unsetFields = append(unsetFields, bson.E{Key: "annotations." + k, Value: ""})
		} else {
			setFields = append(setFields, bson.E{Key: "annotations." + k, Value: *v})
		}
	}

	now := time.Now().UTC()
	setFields = append(setFields, bson.E{Key: "updated_at", Value: now})

	update := bson.D{{Key: "$set", Value: setFields}}
	if len(unsetFields) > 0 {
		update = append(update, bson.E{Key: "$unset", Value: unsetFields})
	}

	filter := bson.D{{Key: "stream", Value: stream}, {Key: "_id", Value: oid}}
	res, err := s.coll.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("ledger/mongodb: set annotations: %w", err)
	}
	if res.MatchedCount == 0 {
		return ledger.ErrEntryNotFound
	}
	if s.mutationLog != nil {
		s.writeMutationEvent(ctx, internalReplication.Event{
			Type:        internalReplication.TypeSetAnnotations,
			Stream:      stream,
			EntryID:     id,
			Annotations: annotations,
		})
	}
	return nil
}

// ListStreamIDs returns distinct stream IDs with at least one entry in this store.
// Results are ascending by stream ID and cursor-paginated via ListAfter/ListLimit.
//
// Implementation uses an aggregation pipeline with a cursor-bounded $match so
// the work per page is O(limit * entries_per_stream) rather than a full collection
// scan. The {stream:1} index supports the $match/$group stages.
func (s *Store) ListStreamIDs(ctx context.Context, opts ...ledger.ListOption) ([]string, error) {
	if s.closed.Load() {
		return nil, ledger.ErrStoreClosed
	}
	o := ledger.ApplyListOptions(opts...)
	ctx = s.sessionCtx(ctx)

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "stream", Value: bson.D{{Key: "$gt", Value: o.After()}}}}}},
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$stream"}}}},
		{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
		{{Key: "$limit", Value: int64(o.Limit())}},
	}

	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("ledger/mongodb: list stream ids: %w", err)
	}
	defer cursor.Close(ctx)

	var ids []string
	for cursor.Next(ctx) {
		var row struct {
			ID string `bson:"_id"`
		}
		if err := cursor.Decode(&row); err != nil {
			return nil, fmt.Errorf("ledger/mongodb: decode stream id: %w", err)
		}
		ids = append(ids, row.ID)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("ledger/mongodb: list stream ids cursor: %w", err)
	}
	return ids, nil
}

// Type returns the collection name this store is bound to, which represents the
// entity type for all streams in this store. Intended for logging/tracing.
func (s *Store) Type() string { return s.coll.Name() }

// Trim deletes entries from the named stream with IDs <= beforeID.
func (s *Store) Trim(ctx context.Context, stream string, beforeID string) (int64, error) {
	if s.closed.Load() {
		return 0, ledger.ErrStoreClosed
	}
	ctx = s.sessionCtx(ctx)
	oid, err := bson.ObjectIDFromHex(beforeID)
	if err != nil {
		return 0, fmt.Errorf("ledger/mongodb: invalid trim cursor: %w", err)
	}
	res, err := s.coll.DeleteMany(ctx, bson.D{
		{Key: "stream", Value: stream},
		{Key: "_id", Value: bson.D{{Key: "$lte", Value: oid}}},
	})
	if err != nil {
		return 0, fmt.Errorf("ledger/mongodb: trim: %w", err)
	}
	if s.mutationLog != nil && res.DeletedCount > 0 {
		s.writeMutationEvent(ctx, internalReplication.Event{
			Type:     internalReplication.TypeTrim,
			Stream:   stream,
			BeforeID: beforeID,
		})
	}
	return res.DeletedCount, nil
}

// FindBySourceID resolves a sink entry ID from its replication source ID.
func (s *Store) FindBySourceID(ctx context.Context, stream, sourceID string) (string, bool, error) {
	if s.closed.Load() {
		return "", false, ledger.ErrStoreClosed
	}
	ctx = s.sessionCtx(ctx)
	var doc entry
	err := s.coll.FindOne(ctx, bson.D{{Key: "stream", Value: stream}, {Key: "source_id", Value: sourceID}}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("ledger/mongodb: find by source id: %w", err)
	}
	return doc.ID.Hex(), true, nil
}

// GetCursor returns the persisted replication cursor for the given name.
func (s *Store) GetCursor(ctx context.Context, name string) (string, bool, error) {
	if s.closed.Load() {
		return "", false, ledger.ErrStoreClosed
	}
	var doc struct {
		Cursor string `bson:"cursor"`
	}
	err := s.cursors.FindOne(ctx, bson.D{{Key: "_id", Value: name}}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("ledger/mongodb: get cursor: %w", err)
	}
	return doc.Cursor, true, nil
}

// SetCursor persists the replication cursor for the given name.
// Uses MongoDB $max so the cursor only advances — if the stored cursor is already
// at or past the given value, the update is a no-op. This prevents a lagging Bridge
// instance from regressing the cursor position set by a faster instance.
// $max uses lexicographic comparison for strings, which is correct for MongoDB
// ObjectID hex cursors (time-ordered, fixed-length).
func (s *Store) SetCursor(ctx context.Context, name, cursor string) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
	}
	_, err := s.cursors.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: name}},
		bson.D{{Key: "$max", Value: bson.D{{Key: "cursor", Value: cursor}}}},
		mongoopts.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("ledger/mongodb: set cursor: %w", err)
	}
	return nil
}

// Health checks database connectivity.
func (s *Store) Health(ctx context.Context) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
	}
	return s.db.Client().Ping(ctx, nil)
}

// Close marks the store as closed.
func (s *Store) Close(_ context.Context) error {
	s.closed.Store(true)
	return nil
}

// BSONCodec implements [ledger.PayloadCodec][T, bson.Raw] using the MongoDB BSON driver.
// Use this as the codec when constructing a [ledger.Stream] backed by this MongoDB store.
type BSONCodec[T any] struct{}

// Marshal encodes v to a BSON document.
func (BSONCodec[T]) Marshal(v T) (bson.Raw, error) {
	b, err := bson.Marshal(v)
	if err != nil {
		return nil, err
	}
	return bson.Raw(b), nil
}

// Unmarshal decodes a BSON document into v.
func (BSONCodec[T]) Unmarshal(p bson.Raw, v *T) error {
	return bson.Unmarshal(p, v)
}
