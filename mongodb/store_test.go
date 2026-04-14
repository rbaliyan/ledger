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
