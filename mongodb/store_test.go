package mongodb_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/rbaliyan/ledger"
	"github.com/rbaliyan/ledger/mongodb"
	"github.com/rbaliyan/ledger/storetest"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// mustMarshalBSON marshals v to a bson.Raw document, panicking on error.
func mustMarshalBSON(v any) bson.Raw {
	b, err := bson.Marshal(v)
	if err != nil {
		panic(err)
	}
	return bson.Raw(b)
}

func newTestStore(t *testing.T) *mongodb.Store {
	t.Helper()
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		t.Skip("MONGO_URI not set, skipping integration test")
	}
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { client.Disconnect(context.Background()) })

	db := client.Database("ledger_test")
	ctx := context.Background()

	// Drop for clean test state, then re-create with indexes
	db.Collection("ledger_test_entries").Drop(ctx) //nolint:errcheck

	store, err := mongodb.New(ctx, db, mongodb.WithCollection("ledger_test_entries"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() {
		db.Collection("ledger_test_entries").Drop(context.Background()) //nolint:errcheck
		store.Close(context.Background())
	})

	return store
}

func TestConformance(t *testing.T) {
	store := newTestStore(t)
	storetest.RunStoreTests(t, store, ledger.After[string], storetest.TestConfig[bson.Raw]{
		SamplePayload: mustMarshalBSON(bson.D{{Key: "x", Value: 1}}),
	})
}

func TestClosedStoreErrors(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	store.Close(ctx)

	_, err := store.Append(ctx, "stream", ledger.RawEntry[bson.Raw]{Payload: mustMarshalBSON(bson.D{})})
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("Append on closed: %v, want ErrStoreClosed", err)
	}

	_, err = store.Read(ctx, "stream")
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("Read on closed: %v, want ErrStoreClosed", err)
	}

	_, err = store.ListStreamIDs(ctx)
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("ListStreamIDs on closed: %v, want ErrStoreClosed", err)
	}
}

func TestNew_InvalidCollectionName(t *testing.T) {
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		t.Skip("MONGO_URI not set")
	}
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Disconnect(context.Background())

	_, err = mongodb.New(context.Background(), client.Database("ledger_test"), mongodb.WithCollection("bad name!"))
	if err == nil {
		t.Fatal("expected error for invalid collection name")
	}
}

func TestNew_NilDB(t *testing.T) {
	_, err := mongodb.New(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil db")
	}
}

// mongoClient connects to MONGO_URI, skipping if unset.
func mongoClient(t *testing.T) *mongo.Client {
	t.Helper()
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		t.Skip("MONGO_URI not set, skipping integration test")
	}
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { client.Disconnect(context.Background()) })
	return client
}

// newSearchStore creates a fresh store using collection name coll.
// The collection is dropped before and after the test.
func newSearchStore(t *testing.T, coll string, opts ...mongodb.Option) *mongodb.Store {
	t.Helper()
	client := mongoClient(t)
	db := client.Database("ledger_test")
	ctx := context.Background()

	db.Collection(coll).Drop(ctx) //nolint:errcheck

	allOpts := append([]mongodb.Option{mongodb.WithCollection(coll)}, opts...)
	store, err := mongodb.New(ctx, db, allOpts...)
	if err != nil {
		t.Fatalf("new store %q: %v", coll, err)
	}
	t.Cleanup(func() {
		db.Collection(coll).Drop(context.Background()) //nolint:errcheck
		store.Close(context.Background())
	})
	return store
}

// seedMongoSearch appends entries with searchable text payloads.
func seedMongoSearch(ctx context.Context, t *testing.T, store *mongodb.Store) {
	t.Helper()
	docs := []struct {
		stream string
		doc    bson.D
	}{
		{"stream-a", bson.D{{Key: "event", Value: "user_login"}, {Key: "user", Value: "alice"}}},
		{"stream-a", bson.D{{Key: "event", Value: "user_logout"}, {Key: "user", Value: "alice"}}},
		{"stream-b", bson.D{{Key: "event", Value: "purchase"}, {Key: "user", Value: "bob"}, {Key: "item", Value: "widget"}}},
	}
	for _, d := range docs {
		if _, err := store.Append(ctx, d.stream, ledger.RawEntry[bson.Raw]{
			Payload:       mustMarshalBSON(d.doc),
			SchemaVersion: 1,
		}); err != nil {
			t.Fatalf("seed append: %v", err)
		}
	}
}

func TestSearch_TextIndex(t *testing.T) {
	store := newSearchStore(t, "ledger_search_text_test")
	ctx := context.Background()

	// EnsureSearchIndex creates the text index.
	if err := store.EnsureSearchIndex(ctx); err != nil {
		t.Fatalf("EnsureSearchIndex: %v", err)
	}

	seedMongoSearch(ctx, t, store)

	// Search across all streams.
	results, err := store.Search(ctx, "", "login")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}

	// Stream-scoped search.
	results, err = store.Search(ctx, "stream-a", "user")
	if err != nil {
		t.Fatalf("Search stream-a: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results for stream-a, want 2", len(results))
	}

	// No match.
	results, err = store.Search(ctx, "", "nonexistent_xyz")
	if err != nil {
		t.Fatalf("Search no-match: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("got %d results, want 0", len(results))
	}
}

func TestEnsureSearchIndex_Mongo(t *testing.T) {
	ctx := context.Background()

	// Default mode — must succeed and be idempotent.
	store := newSearchStore(t, "ledger_ensure_idx_test")
	if err := store.EnsureSearchIndex(ctx); err != nil {
		t.Fatalf("EnsureSearchIndex (first): %v", err)
	}
	if err := store.EnsureSearchIndex(ctx); err != nil {
		t.Fatalf("EnsureSearchIndex (second, idempotent): %v", err)
	}

	// Atlas mode — must return an error; user manages the index externally.
	atlasStore := newSearchStore(t, "ledger_atlas_idx_test", mongodb.WithAtlasSearch("my-index"))
	if err := atlasStore.EnsureSearchIndex(ctx); err == nil {
		t.Fatal("expected error calling EnsureSearchIndex with WithAtlasSearch")
	}
}

func TestSearch_NoTextIndex(t *testing.T) {
	store := newSearchStore(t, "ledger_no_idx_test")
	ctx := context.Background()
	seedMongoSearch(ctx, t, store)

	// $text search without a text index — MongoDB returns an error.
	_, err := store.Search(ctx, "", "login")
	if err == nil {
		t.Fatal("expected error searching without text index")
	}
}

func TestSearch_ClosedStore_Mongo(t *testing.T) {
	store := newSearchStore(t, "ledger_closed_search_test")
	ctx := context.Background()
	store.Close(ctx)

	_, err := store.Search(ctx, "", "anything")
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("Search on closed: %v, want ErrStoreClosed", err)
	}
	if err := store.EnsureSearchIndex(ctx); !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("EnsureSearchIndex on closed: %v, want ErrStoreClosed", err)
	}
}

// TestSearch_AtlasSearch requires a real Atlas cluster.
// Set MONGO_ATLAS_URI and MONGO_ATLAS_SEARCH_INDEX to enable.
func TestSearch_AtlasSearch(t *testing.T) {
	atlasURI := os.Getenv("MONGO_ATLAS_URI")
	indexName := os.Getenv("MONGO_ATLAS_SEARCH_INDEX")
	if atlasURI == "" || indexName == "" {
		t.Skip("MONGO_ATLAS_URI or MONGO_ATLAS_SEARCH_INDEX not set, skipping Atlas Search test")
	}

	client, err := mongo.Connect(options.Client().ApplyURI(atlasURI))
	if err != nil {
		t.Fatalf("connect to Atlas: %v", err)
	}
	t.Cleanup(func() { client.Disconnect(context.Background()) })

	db := client.Database("ledger_test")
	ctx := context.Background()

	const coll = "ledger_atlas_search_test"
	db.Collection(coll).Drop(ctx) //nolint:errcheck

	store, err := mongodb.New(ctx, db, mongodb.WithCollection(coll), mongodb.WithAtlasSearch(indexName))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() {
		db.Collection(coll).Drop(context.Background()) //nolint:errcheck
		store.Close(context.Background())
	})

	seedMongoSearch(ctx, t, store)

	results, err := store.Search(ctx, "", "login")
	if err != nil {
		t.Fatalf("Atlas Search: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}

	// Stream-scoped Atlas search.
	results, err = store.Search(ctx, "stream-a", "user")
	if err != nil {
		t.Fatalf("Atlas Search stream-a: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
}

// TestCrossStoreIsolation verifies that two stores on the same database but
// different collections do not share any entries: each store represents one type.
func TestCrossStoreIsolation(t *testing.T) {
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		t.Skip("MONGO_URI not set, skipping integration test")
	}
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { client.Disconnect(context.Background()) })

	ctx := context.Background()
	db := client.Database("ledger_test")

	db.Collection("ledger_iso_orders").Drop(ctx) //nolint:errcheck
	db.Collection("ledger_iso_users").Drop(ctx)  //nolint:errcheck

	orders, err := mongodb.New(ctx, db, mongodb.WithCollection("ledger_iso_orders"))
	if err != nil {
		t.Fatalf("new orders: %v", err)
	}
	users, err := mongodb.New(ctx, db, mongodb.WithCollection("ledger_iso_users"))
	if err != nil {
		t.Fatalf("new users: %v", err)
	}
	t.Cleanup(func() {
		db.Collection("ledger_iso_orders").Drop(context.Background()) //nolint:errcheck
		db.Collection("ledger_iso_users").Drop(context.Background())  //nolint:errcheck
		orders.Close(ctx)
		users.Close(ctx)
	})

	orderPayload := mustMarshalBSON(bson.D{{Key: "type", Value: "order"}})
	userPayload := mustMarshalBSON(bson.D{{Key: "type", Value: "user"}})

	if _, err := orders.Append(ctx, "alice", ledger.RawEntry[bson.Raw]{Payload: orderPayload, SchemaVersion: 1}); err != nil {
		t.Fatalf("append orders: %v", err)
	}
	if _, err := users.Append(ctx, "alice", ledger.RawEntry[bson.Raw]{Payload: userPayload, SchemaVersion: 1}); err != nil {
		t.Fatalf("append users: %v", err)
	}

	// Each store sees only its own entries.
	o, _ := orders.Read(ctx, "alice")
	u, _ := users.Read(ctx, "alice")

	if len(o) != 1 {
		t.Errorf("orders/alice: want 1 entry, got %d", len(o))
	} else {
		var doc bson.D
		if err := bson.Unmarshal(o[0].Payload, &doc); err != nil {
			t.Errorf("orders/alice: unmarshal payload: %v", err)
		}
	}

	if len(u) != 1 {
		t.Errorf("users/alice: want 1 entry, got %d", len(u))
	} else {
		var doc bson.D
		if err := bson.Unmarshal(u[0].Payload, &doc); err != nil {
			t.Errorf("users/alice: unmarshal payload: %v", err)
		}
	}

	// Each store's Type() returns its own collection name.
	if orders.Type() != "ledger_iso_orders" {
		t.Errorf("orders.Type() = %q", orders.Type())
	}
	if users.Type() != "ledger_iso_users" {
		t.Errorf("users.Type() = %q", users.Type())
	}

	// ListStreamIDs is scoped per store.
	oIDs, _ := orders.ListStreamIDs(ctx)
	uIDs, _ := users.ListStreamIDs(ctx)
	if len(oIDs) != 1 || oIDs[0] != "alice" {
		t.Errorf("orders ListStreamIDs = %v", oIDs)
	}
	if len(uIDs) != 1 || uIDs[0] != "alice" {
		t.Errorf("users ListStreamIDs = %v", uIDs)
	}
}
