package cli

import (
	"context"
	"fmt"

	ledgerv1 "github.com/rbaliyan/ledger/api/ledger/v1"
	"github.com/rbaliyan/ledger/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// clientConfig resolves the effective configuration for CLI commands.
// Flag values take priority over config file values.
func clientConfig() (*config.Config, error) {
	var cfg *config.Config
	var err error
	if flagConfig != "" {
		cfg, err = config.LoadFrom(flagConfig)
	} else {
		cfg, err = config.Load()
	}
	if err != nil {
		return nil, err
	}
	if flagAddr != "" {
		cfg.Listen = flagAddr
	}
	if flagAPIKey != "" {
		cfg.APIKey = flagAPIKey
	}
	return cfg, nil
}

// dial opens a gRPC connection to the daemon.
func dial(cfg *config.Config) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(cfg.Listen, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", cfg.Listen, err)
	}
	return conn, nil
}

// storeCtx returns a context with the x-ledger-store and (if set) x-api-key metadata.
func storeCtx(ctx context.Context, cfg *config.Config, store string) context.Context {
	md := metadata.Pairs("x-ledger-store", store)
	if cfg.APIKey != "" {
		md.Append("x-api-key", cfg.APIKey)
	}
	return metadata.NewOutgoingContext(ctx, md)
}

// newClient creates a LedgerServiceClient connected to the daemon.
func newClient(cfg *config.Config) (ledgerv1.LedgerServiceClient, *grpc.ClientConn, error) {
	conn, err := dial(cfg)
	if err != nil {
		return nil, nil, err
	}
	return ledgerv1.NewLedgerServiceClient(conn), conn, nil
}
