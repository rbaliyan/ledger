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
	_ "modernc.org/sqlite"
)

// Server wraps a gRPC server and its listener.
type Server struct {
	grpc     *grpc.Server
	listener net.Listener
	mux      *muxBackend
	db       *sql.DB
}

// New creates and configures the gRPC server from cfg.
func New(ctx context.Context, cfg *config.Config) (*Server, error) {
	db, factory, err := backendFactory(ctx, cfg)
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

	return &Server{grpc: grpcSrv, listener: ln, mux: mux, db: db}, nil
}

// Addr returns the address the server is listening on.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Serve blocks until the server stops.
func (s *Server) Serve() error {
	return s.grpc.Serve(s.listener)
}

// Stop gracefully drains in-flight RPCs, then closes the DB connection.
func (s *Server) Stop(ctx context.Context) {
	s.grpc.GracefulStop()
	s.mux.Close(ctx)
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			slog.Warn("error closing database", "err", err)
		}
	}
}

// backendFactory returns a BackendFactory for the configured DB type plus
// the underlying *sql.DB so it can be closed with Stop.
func backendFactory(ctx context.Context, cfg *config.Config) (*sql.DB, BackendFactory, error) {
	switch cfg.DB.Type {
	case "sqlite":
		db, err := openDB(ctx, "sqlite", cfg.DB.SQLite.Path)
		if err != nil {
			return nil, nil, err
		}
		return db, func(ctx context.Context, name string) (ledgerpb.Backend, error) {
			store, err := sqlite.New(ctx, db, sqlite.WithTable(name))
			if err != nil {
				return nil, err
			}
			return ledgerpb.NewInt64Backend(store), nil
		}, nil

	case "postgres":
		db, err := openDB(ctx, "postgres", cfg.DB.Postgres.DSN)
		if err != nil {
			return nil, nil, err
		}
		return db, func(ctx context.Context, name string) (ledgerpb.Backend, error) {
			store, err := postgres.New(ctx, db, postgres.WithTable(name))
			if err != nil {
				return nil, err
			}
			return ledgerpb.NewInt64Backend(store), nil
		}, nil

	case "mongodb":
		return nil, nil, fmt.Errorf("server: mongodb backend not supported in daemon (use direct library)")

	default:
		return nil, nil, fmt.Errorf("server: unknown db type %q", cfg.DB.Type)
	}
}

// openDB opens a database connection, pings to verify it, and returns it.
// SQLite connections are limited to 1 to prevent write locking with the
// database/sql pool.
func openDB(ctx context.Context, driver, dsn string) (*sql.DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("server: open %s: %w", driver, err)
	}
	if driver == "sqlite" {
		db.SetMaxOpenConns(1)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("server: ping %s: %w", driver, err)
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
