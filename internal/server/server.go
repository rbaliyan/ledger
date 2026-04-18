package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"os"

	ledgerv1 "github.com/rbaliyan/ledger/api/ledger/v1"
	"github.com/rbaliyan/ledger/internal/config"
	"github.com/rbaliyan/ledger/ledgerpb"
	"github.com/rbaliyan/ledger/postgres"
	"github.com/rbaliyan/ledger/sqlite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	_ "github.com/lib/pq"
)

// Server wraps a gRPC server and its listener.
type Server struct {
	grpc     *grpc.Server
	listener net.Listener
}

// New creates and configures the gRPC server from cfg.
func New(ctx context.Context, cfg *config.Config) (*Server, error) {
	factory, err := backendFactory(ctx, cfg)
	if err != nil {
		return nil, err
	}
	mux := newMuxBackend(factory)
	guard := &apiKeyGuard{apiKey: cfg.APIKey}

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(ledgerpb.UnaryInterceptor(guard)),
		grpc.StreamInterceptor(ledgerpb.StreamInterceptor(guard)),
	}

	if cfg.TLS.Cert != "" && cfg.TLS.Key != "" {
		creds, err := buildTLS(cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("server: tls: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}

	grpcSrv := grpc.NewServer(opts...)
	ledgerv1.RegisterLedgerServiceServer(grpcSrv, ledgerpb.NewServer(mux))

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, fmt.Errorf("server: listen %s: %w", cfg.Listen, err)
	}
	slog.Info("ledger daemon listening", "addr", ln.Addr().String())

	return &Server{grpc: grpcSrv, listener: ln}, nil
}

// Addr returns the address the server is listening on.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Serve blocks until the server stops.
func (s *Server) Serve() error {
	return s.grpc.Serve(s.listener)
}

// Stop gracefully stops the server.
func (s *Server) Stop() {
	s.grpc.GracefulStop()
}

// backendFactory returns a BackendFactory for the configured DB type.
func backendFactory(ctx context.Context, cfg *config.Config) (BackendFactory, error) {
	switch cfg.DB.Type {
	case "sqlite":
		db, err := openSQLite(ctx, cfg.DB.SQLite.Path)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, name string) (ledgerpb.Backend, error) {
			store, err := sqlite.New(ctx, db, sqlite.WithTable(name))
			if err != nil {
				return nil, err
			}
			return ledgerpb.NewInt64Backend(store), nil
		}, nil

	case "postgres":
		db, err := sql.Open("postgres", cfg.DB.Postgres.DSN)
		if err != nil {
			return nil, fmt.Errorf("server: postgres open: %w", err)
		}
		return func(ctx context.Context, name string) (ledgerpb.Backend, error) {
			store, err := postgres.New(ctx, db, postgres.WithTable(name))
			if err != nil {
				return nil, err
			}
			return ledgerpb.NewInt64Backend(store), nil
		}, nil

	case "mongodb":
		return nil, fmt.Errorf("server: mongodb backend not supported in daemon (use direct library)")

	default:
		return nil, fmt.Errorf("server: unknown db type %q", cfg.DB.Type)
	}
}

// openSQLite opens (or creates) a SQLite database at path.
func openSQLite(_ context.Context, path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("server: sqlite open %s: %w", path, err)
	}
	return db, nil
}

// buildTLS constructs gRPC transport credentials from cert/key/CA paths.
func buildTLS(tlsCfg config.TLSConfig) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(tlsCfg.Cert, tlsCfg.Key)
	if err != nil {
		return nil, fmt.Errorf("load cert/key: %w", err)
	}
	tc := &tls.Config{Certificates: []tls.Certificate{cert}}
	if tlsCfg.CA != "" {
		ca, err := os.ReadFile(tlsCfg.CA)
		if err != nil {
			return nil, fmt.Errorf("read CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("invalid CA certificate")
		}
		tc.ClientCAs = pool
		tc.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(tc), nil
}
