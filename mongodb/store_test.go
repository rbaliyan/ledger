package mongodb_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/rbaliyan/ledger"
	"github.com/rbaliyan/ledger/mongodb"
	"github.com/rbaliyan/ledger/storetest"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

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
	storetest.RunStoreTests(t, store, ledger.After[string])
}

func TestClosedStoreErrors(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	store.Close(ctx)

	_, err := store.Append(ctx, "stream", ledger.RawEntry{Payload: []byte(`{}`)})
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("Append on closed: %v, want ErrStoreClosed", err)
	}

	_, err = store.Read(ctx, "stream")
	if !errors.Is(err, ledger.ErrStoreClosed) {
		t.Errorf("Read on closed: %v, want ErrStoreClosed", err)
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
