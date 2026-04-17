package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/ledger"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	_ ledger.Store[string, json.RawMessage] = (*JSONStore)(nil)
	_ ledger.HealthChecker                  = (*JSONStore)(nil)
)

type jsonEntry struct {
	ID            bson.ObjectID     `bson:"_id,omitempty"`
	Stream        string            `bson:"stream"`
	Payload       string            `bson:"payload"` // JSON text, not BSON
	OrderKey      string            `bson:"order_key"`
	DedupKey      string            `bson:"dedup_key"`
	SchemaVersion int               `bson:"schema_version"`
	Metadata      map[string]string `bson:"metadata,omitempty"`
	Tags          []string          `bson:"tags"`
	Annotations   map[string]string `bson:"annotations,omitempty"`
	CreatedAt     time.Time         `bson:"created_at"`
	UpdatedAt     *time.Time        `bson:"updated_at,omitempty"`
}

// JSONStore is a MongoDB-backed ledger store that serialises payloads as JSON text.
// It implements Store[string, json.RawMessage] and is designed for use as the
// mutation log when MongoDB is the replication source backend.
//
// Mutation log writes are best-effort when no external session is active. To
// guarantee atomicity on replica-set clusters, pass a *mongo.Session with an
// active transaction via [ledger.WithTx]; both the main write and the mutation log
// write will participate in that transaction.
type JSONStore struct {
	db     *mongo.Database
	coll   *mongo.Collection
	closed atomic.Bool
}

// NewJSONStore creates a JSONStore backed by the given database and collection name.
// Intended for use as the mutation log in [Store.WithMutationLog].
func NewJSONStore(ctx context.Context, db *mongo.Database, collection string) (*JSONStore, error) {
	if db == nil {
		return nil, fmt.Errorf("ledger/mongodb: db must not be nil")
	}
	if err := ledger.ValidateName(collection); err != nil {
		return nil, fmt.Errorf("ledger/mongodb: %w", err)
	}
	coll := db.Collection(collection)
	s := &JSONStore{db: db, coll: coll}
	models := []mongo.IndexModel{
		{Keys: bson.D{{Key: "stream", Value: 1}, {Key: "_id", Value: 1}}},
	}
	if _, err := coll.Indexes().CreateMany(ctx, models); err != nil {
		return nil, fmt.Errorf("ledger/mongodb: ensure indexes: %w", err)
	}
	return s, nil
}

// Type returns the collection name. Intended for logging/tracing.
func (s *JSONStore) Type() string { return s.coll.Name() }

func (s *JSONStore) sessionCtx(ctx context.Context) context.Context {
	if sess, ok := ledger.TxFromContext(ctx).(*mongo.Session); ok {
		return mongo.NewSessionContext(ctx, sess)
	}
	return ctx
}

// Append adds entries to the named stream.
func (s *JSONStore) Append(ctx context.Context, stream string, entries ...ledger.RawEntry[json.RawMessage]) ([]string, error) {
	if s.closed.Load() {
		return nil, ledger.ErrStoreClosed
	}
	if len(entries) == 0 {
		return nil, nil
	}
	ctx = s.sessionCtx(ctx)
	now := time.Now().UTC()
	docs := make([]jsonEntry, len(entries))
	for i, e := range entries {
		tags := e.Tags
		if tags == nil {
			tags = []string{}
		}
		docs[i] = jsonEntry{
			ID:            bson.NewObjectID(),
			Stream:        stream,
			Payload:       string(e.Payload),
			OrderKey:      e.OrderKey,
			DedupKey:      e.DedupKey,
			SchemaVersion: e.SchemaVersion,
			Metadata:      e.Metadata,
			Tags:          tags,
			CreatedAt:     now,
		}
	}
	if _, err := s.coll.InsertMany(ctx, docs, mongoopts.InsertMany().SetOrdered(true)); err != nil {
		return nil, fmt.Errorf("ledger/mongodb: insert many: %w", err)
	}
	ids := make([]string, len(docs))
	for i, doc := range docs {
		ids[i] = doc.ID.Hex()
	}
	return ids, nil
}

// Read returns entries from the named stream.
func (s *JSONStore) Read(ctx context.Context, stream string, opts ...ledger.ReadOption) ([]ledger.StoredEntry[string, json.RawMessage], error) {
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
	defer cursor.Close(ctx) //nolint:errcheck

	var entries []ledger.StoredEntry[string, json.RawMessage]
	for cursor.Next(ctx) {
		var doc jsonEntry
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("ledger/mongodb: decode: %w", err)
		}
		entries = append(entries, ledger.StoredEntry[string, json.RawMessage]{
			ID:            doc.ID.Hex(),
			Stream:        doc.Stream,
			Payload:       json.RawMessage(doc.Payload),
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
func (s *JSONStore) Count(ctx context.Context, stream string) (int64, error) {
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
func (s *JSONStore) SetTags(ctx context.Context, stream, id string, tags []string) error {
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
func (s *JSONStore) SetAnnotations(ctx context.Context, stream, id string, annotations map[string]*string) error {
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
	res, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "stream", Value: stream}, {Key: "_id", Value: oid}},
		update,
	)
	if err != nil {
		return fmt.Errorf("ledger/mongodb: set annotations: %w", err)
	}
	if res.MatchedCount == 0 {
		return ledger.ErrEntryNotFound
	}
	return nil
}

// Trim deletes entries with IDs less than or equal to beforeID from the stream.
func (s *JSONStore) Trim(ctx context.Context, stream, beforeID string) (int64, error) {
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

// ListStreamIDs returns distinct stream IDs in this store.
func (s *JSONStore) ListStreamIDs(ctx context.Context, opts ...ledger.ListOption) ([]string, error) {
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
	defer cursor.Close(ctx) //nolint:errcheck
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

// Health checks database connectivity.
func (s *JSONStore) Health(ctx context.Context) error {
	if s.closed.Load() {
		return ledger.ErrStoreClosed
	}
	return s.db.Client().Ping(ctx, nil)
}

// Close marks the store as closed.
func (s *JSONStore) Close(_ context.Context) error {
	s.closed.Store(true)
	return nil
}
