package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	ledgerv1 "github.com/rbaliyan/ledger/api/ledger/v1"
	"github.com/rbaliyan/ledger/internal/config"
	"github.com/rbaliyan/ledger/ledgerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// clientConfig resolves the effective configuration for CLI commands.
// When --config is given the file at that path is loaded with no discovery.
// When --config is absent, built-in defaults are used.
// --addr and --api-key always override the loaded values.
func clientConfig() (*config.Config, error) {
	var cfg *config.Config
	if flagConfig != "" {
		var err error
		cfg, err = config.LoadFrom(flagConfig)
		if err != nil {
			return nil, err
		}
	} else {
		cfg = config.Defaults()
	}
	if flagAddr != "" {
		cfg.Listen = flagAddr
	}
	if flagAPIKey != "" {
		cfg.APIKey = flagAPIKey
	}
	return cfg, nil
}

// dial opens a gRPC connection to the daemon, using TLS when configured and
// insecure credentials otherwise.
func dial(cfg *config.Config) (*grpc.ClientConn, error) {
	creds, err := clientCreds(cfg.TLS)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(cfg.Listen, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", cfg.Listen, err)
	}
	return conn, nil
}

// clientCreds returns gRPC transport credentials. A populated TLS.CA pins the
// daemon's CA for verification; cert+key adds client authentication; otherwise
// the connection runs insecurely (intended for local development).
func clientCreds(tlsCfg config.TLSConfig) (credentials.TransportCredentials, error) {
	if tlsCfg.CA == "" && tlsCfg.Cert == "" && tlsCfg.Key == "" {
		return insecure.NewCredentials(), nil
	}
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if tlsCfg.CA != "" {
		ca, err := os.ReadFile(tlsCfg.CA)
		if err != nil {
			return nil, fmt.Errorf("client: read CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("client: invalid CA certificate")
		}
		tc.RootCAs = pool
	}
	if tlsCfg.Cert != "" && tlsCfg.Key != "" {
		cert, err := tls.LoadX509KeyPair(tlsCfg.Cert, tlsCfg.Key)
		if err != nil {
			return nil, fmt.Errorf("client: load cert/key: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return credentials.NewTLS(tc), nil
}

// storeCtx returns a context with the store metadata header and, if configured,
// the API key header.
func storeCtx(ctx context.Context, cfg *config.Config, store string) context.Context {
	md := metadata.Pairs(ledgerpb.StoreMetadataHeader, store)
	if cfg.APIKey != "" {
		md.Append(ledgerpb.APIKeyMetadataHeader, cfg.APIKey)
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
