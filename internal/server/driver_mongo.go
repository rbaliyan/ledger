package server

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/rbaliyan/ledger/internal/config"
	"github.com/rbaliyan/ledger/ledgerpb"
	"github.com/rbaliyan/ledger/mongodb"
)

func init() { RegisterDriver("mongodb", mongoDriver) }

func mongoDriver(ctx context.Context, cfg *config.Config) (DriverResources, error) {
	client, err := mongo.Connect(mongoopts.Client().ApplyURI(cfg.DB.MongoDB.URI))
	if err != nil {
		return DriverResources{}, fmt.Errorf("server: mongodb connect: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return DriverResources{}, fmt.Errorf("server: mongodb ping: %w", err)
	}

	db := client.Database(cfg.DB.MongoDB.Database)

	meta, err := newMongoMetaStore(ctx, db)
	if err != nil {
		_ = client.Disconnect(context.Background())
		return DriverResources{}, fmt.Errorf("server: mongodb meta store: %w", err)
	}

	factory := func(ctx context.Context, name string) (ledgerpb.Provider, error) {
		store, err := mongodb.NewJSONStore(ctx, db, name)
		if err != nil {
			return nil, fmt.Errorf("open mongodb store %q: %w", name, err)
		}
		return ledgerpb.NewStringProvider(store), nil
	}

	closer := closerFunc(func() error {
		return client.Disconnect(context.Background())
	})

	return DriverResources{Meta: meta, Factory: factory, Closer: closer}, nil
}
