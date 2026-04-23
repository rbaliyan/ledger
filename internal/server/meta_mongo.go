package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rbaliyan/ledger"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"
)

var _ metaStore = (*mongoMetaStore)(nil)

// mongoMetaStore implements metaStore for the MongoDB backend.
// Stream metadata is stored in a ledger_stream_metadata collection in the
// same database as the entry data — no sidecar database required.
type mongoMetaStore struct {
	coll *mongo.Collection
}

type streamMetaDoc struct {
	StoreName string    `bson:"store_name"`
	Name      string    `bson:"name"`
	StreamID  string    `bson:"stream_id"`
	CreatedAt time.Time `bson:"created_at"`
}

func newMongoMetaStore(ctx context.Context, db *mongo.Database) (*mongoMetaStore, error) {
	coll := db.Collection("ledger_stream_metadata")
	m := &mongoMetaStore{coll: coll}
	return m, m.ensureIndexes(ctx)
}

func (m *mongoMetaStore) ensureIndexes(ctx context.Context) error {
	models := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "store_name", Value: 1}, {Key: "name", Value: 1}},
			Options: mongoopts.Index().SetUnique(true).SetName("idx_store_name"),
		},
		{
			Keys:    bson.D{{Key: "store_name", Value: 1}, {Key: "stream_id", Value: 1}},
			Options: mongoopts.Index().SetUnique(true).SetName("idx_store_stream"),
		},
	}
	_, err := m.coll.Indexes().CreateMany(ctx, models)
	return err
}

func (m *mongoMetaStore) resolve(ctx context.Context, storeName, name string) (string, bool, error) {
	var doc streamMetaDoc
	err := m.coll.FindOne(ctx, bson.D{
		{Key: "store_name", Value: storeName},
		{Key: "name", Value: name},
	}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("meta: resolve stream %q: %w", name, err)
	}
	return doc.StreamID, true, nil
}

// resolveOrCreate uses FindOneAndUpdate with upsert and $setOnInsert for an
// atomic check-and-create. If the document already exists, its stream_id is
// returned unchanged; if it is created, the newly generated UUID is returned.
func (m *mongoMetaStore) resolveOrCreate(ctx context.Context, storeName, name string) (string, error) {
	newID := uuid.New().String()
	filter := bson.D{
		{Key: "store_name", Value: storeName},
		{Key: "name", Value: name},
	}
	update := bson.D{
		{Key: "$setOnInsert", Value: bson.D{
			{Key: "store_name", Value: storeName},
			{Key: "name", Value: name},
			{Key: "stream_id", Value: newID},
			{Key: "created_at", Value: time.Now().UTC()},
		}},
	}
	opts := mongoopts.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(mongoopts.After)

	var doc streamMetaDoc
	if err := m.coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc); err != nil {
		return "", fmt.Errorf("meta: resolveOrCreate stream %q: %w", name, err)
	}
	return doc.StreamID, nil
}

func (m *mongoMetaStore) rename(ctx context.Context, storeName, oldName, newName string) error {
	if oldName == newName {
		return nil
	}
	res, err := m.coll.UpdateOne(ctx,
		bson.D{{Key: "store_name", Value: storeName}, {Key: "name", Value: oldName}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: newName}}}},
	)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("%w: %q in store %q", ledger.ErrStreamExists, newName, storeName)
		}
		return fmt.Errorf("meta: rename stream %q: %w", oldName, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("%w: %q in store %q", ledger.ErrStreamNotFound, oldName, storeName)
	}
	return nil
}

func (m *mongoMetaStore) listNames(ctx context.Context, storeName, after string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	filter := bson.D{
		{Key: "store_name", Value: storeName},
		{Key: "name", Value: bson.D{{Key: "$gt", Value: after}}},
	}
	opts := mongoopts.Find().
		SetSort(bson.D{{Key: "name", Value: 1}}).
		SetLimit(int64(limit))

	cursor, err := m.coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("meta: list streams: %w", err)
	}
	defer cursor.Close(ctx) //nolint:errcheck

	var names []string
	for cursor.Next(ctx) {
		var doc streamMetaDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("meta: list streams decode: %w", err)
		}
		names = append(names, doc.Name)
	}
	return names, cursor.Err()
}
