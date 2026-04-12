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
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/ledger"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	_ ledger.Store[string] = (*Store)(nil)
	_ ledger.HealthChecker = (*Store)(nil)
)

type entry struct {
	ID            bson.ObjectID     `bson:"_id,omitempty"`
	Stream        string            `bson:"stream"`
	Payload       []byte            `bson:"payload"`
	OrderKey      string            `bson:"order_key"`
	DedupKey      string            `bson:"dedup_key"`
	SchemaVersion int               `bson:"schema_version"`
	Metadata      map[string]string `bson:"metadata,omitempty"`
	Tags          []string          `bson:"tags"`
	Annotations   map[string]string `bson:"annotations,omitempty"`
	CreatedAt     time.Time         `bson:"created_at"`
	UpdatedAt     *time.Time        `bson:"updated_at,omitempty"`
}

// Store is a MongoDB ledger store.
type Store struct {
	db     *mongo.Database
	coll   *mongo.Collection
	logger *slog.Logger
	closed atomic.Bool
}

// Option configures the MongoDB store.
type Option func(*storeOptions)

type storeOptions struct {
	collection string
	logger     *slog.Logger
}

// WithCollection sets the collection name. Defaults to "ledger_entries".
func WithCollection(name string) Option {
	return func(o *storeOptions) { o.collection = name }
}

// WithLogger sets the structured logger. Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *storeOptions) { o.logger = l }
}

// New creates a new MongoDB ledger store. Indexes are created automatically.
// Tag multikey indexes are created asynchronously in the background.
func New(ctx context.Context, db *mongo.Database, opts ...Option) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("ledger/mongodb: db must not be nil")
	}
	o := storeOptions{collection: "ledger_entries", logger: slog.Default()}
	for _, fn := range opts {
		fn(&o)
	}
	if err := ledger.ValidateName(o.collection); err != nil {
		return nil, fmt.Errorf("ledger/mongodb: %w", err)
	}

	coll := db.Collection(o.collection)
	s := &Store{db: db, coll: coll, logger: o.logger}
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
			Options: options.Index().
				SetUnique(true).
				SetPartialFilterExpression(bson.D{{Key: "dedup_key", Value: bson.D{{Key: "$gt", Value: ""}}}}),
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

func (s *Store) sessionCtx(ctx context.Context) context.Context {
	if sess, ok := ledger.TxFromContext(ctx).(*mongo.Session); ok {
		return mongo.NewSessionContext(ctx, sess)
	}
	return ctx
}

// Append adds entries to the named stream.
func (s *Store) Append(ctx context.Context, stream string, entries ...ledger.RawEntry) ([]string, error) {
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
			CreatedAt:     now,
		}
	}

	insertOpts := options.InsertMany().SetOrdered(false)
	_, err := s.coll.InsertMany(ctx, docs, insertOpts)

	failed := make(map[int]struct{})
	if err != nil {
		bwe, ok := err.(mongo.BulkWriteException)
		if !ok {
			return nil, fmt.Errorf("ledger/mongodb: insert many: %w", err)
		}
		for _, we := range bwe.WriteErrors {
			if we.HasErrorCode(11000) {
				failed[we.Index] = struct{}{}
				s.logger.DebugContext(ctx, "dedup skip", "stream", stream, "index", we.Index)
			} else {
				return nil, fmt.Errorf("ledger/mongodb: insert at index %d: %w", we.Index, err)
			}
		}
	}

	ids := make([]string, 0, len(docs)-len(failed))
	for i, doc := range docs {
		if _, ok := failed[i]; !ok {
			ids = append(ids, doc.ID.Hex())
		}
	}
	return ids, nil
}

// Read returns entries from the named stream.
func (s *Store) Read(ctx context.Context, stream string, opts ...ledger.ReadOption) ([]ledger.StoredEntry[string], error) {
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

	findOpts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: sortDir}}).
		SetLimit(int64(o.Limit()))

	cursor, err := s.coll.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, fmt.Errorf("ledger/mongodb: find: %w", err)
	}
	defer cursor.Close(ctx)

	var entries []ledger.StoredEntry[string]
	for cursor.Next(ctx) {
		var doc entry
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("ledger/mongodb: decode: %w", err)
		}
		entries = append(entries, ledger.StoredEntry[string]{
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

// SetTags replaces all tags on an entry.
func (s *Store) SetTags(ctx context.Context, stream string, id string, tags []string) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
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
	return nil
}

// SetAnnotations merges annotations into an entry. Keys with nil values are deleted.
func (s *Store) SetAnnotations(ctx context.Context, stream string, id string, annotations map[string]*string) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
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
	return nil
}

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
	return res.DeletedCount, nil
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
