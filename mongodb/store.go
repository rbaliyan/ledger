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
	_ ledger.Store[string]  = (*Store)(nil)
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
	CreatedAt     time.Time         `bson:"created_at"`
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
// The name must be a valid identifier (alphanumeric and underscore).
func WithCollection(name string) Option {
	return func(o *storeOptions) { o.collection = name }
}

// WithLogger sets the structured logger. Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *storeOptions) { o.logger = l }
}

// New creates a new MongoDB ledger store. Indexes are created automatically.
// The caller is responsible for managing the *mongo.Client lifecycle.
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
	return s, nil
}

func (s *Store) ensureIndexes(ctx context.Context) error {
	models := []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "stream", Value: 1}, {Key: "_id", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "stream", Value: 1}, {Key: "order_key", Value: 1}, {Key: "_id", Value: 1}},
		},
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

// Append adds entries to the named stream. Returns IDs (hex-encoded ObjectIDs) of newly appended entries.
// Entries with duplicate dedup keys are silently skipped.
func (s *Store) Append(ctx context.Context, stream string, entries ...ledger.RawEntry) ([]string, error) {
	if s.closed.Load() {
		return nil, ledger.ErrStoreClosed
	}
	if len(entries) == 0 {
		return nil, nil
	}

	now := time.Now().UTC()
	docs := make([]entry, len(entries))
	for i, e := range entries {
		docs[i] = entry{
			ID:            bson.NewObjectID(),
			Stream:        stream,
			Payload:       e.Payload,
			OrderKey:      e.OrderKey,
			DedupKey:      e.DedupKey,
			SchemaVersion: e.SchemaVersion,
			Metadata:      e.Metadata,
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
			CreatedAt:     doc.CreatedAt,
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
	n, err := s.coll.CountDocuments(ctx, bson.D{{Key: "stream", Value: stream}})
	if err != nil {
		return 0, fmt.Errorf("ledger/mongodb: count: %w", err)
	}
	return n, nil
}

// Trim deletes entries from the named stream with IDs (ObjectIDs) less than or
// equal to beforeID (hex-encoded ObjectID).
func (s *Store) Trim(ctx context.Context, stream string, beforeID string) (int64, error) {
	if s.closed.Load() {
		return 0, ledger.ErrStoreClosed
	}
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

// Health checks database connectivity by pinging the underlying connection.
func (s *Store) Health(ctx context.Context) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
	}
	return s.db.Client().Ping(ctx, nil)
}

// Close marks the store as closed. The caller is responsible for closing
// the underlying *mongo.Client.
func (s *Store) Close(_ context.Context) error {
	s.closed.Store(true)
	return nil
}
