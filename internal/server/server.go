package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"

	ledgerv1 "github.com/rbaliyan/ledger/api/ledger/v1"
	"github.com/rbaliyan/ledger/internal/config"
	"github.com/rbaliyan/ledger/ledgerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Server wraps a gRPC server and its listener.
type Server struct {
	grpc     *grpc.Server
	listener net.Listener
	mux      *muxProvider
	closer   io.Closer

	mu         sync.Mutex // protects hooks, hookCancel, and stopped
	hooks      []*hookRunner
	hookCancel context.CancelFunc
	stopped    bool // set by Stop; prevents ReloadHooks from starting new goroutines
}

// New creates and configures the gRPC server from cfg.
func New(ctx context.Context, cfg *config.Config) (*Server, error) {
	res, err := openDriver(ctx, cfg)
	if err != nil {
		return nil, err
	}
	mux := newMuxProvider(res.Factory, res.Meta)
	guard := newAPIKeyGuard(cfg.APIKey, cfg.AllowedStores)

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

	hookCtx, hookCancel := context.WithCancel(context.Background())
	srv := &Server{
		grpc:       grpcSrv,
		listener:   ln,
		mux:        mux,
		closer:     res.Closer,
		hookCancel: hookCancel,
	}

	for _, hcfg := range cfg.Hooks {
		h, err := newHookRunner(hcfg, mux)
		if err != nil {
			hookCancel()
			_ = ln.Close()
			mux.Close(ctx)
			return nil, fmt.Errorf("server: hook: %w", err)
		}
		h.Start(hookCtx)
		srv.hooks = append(srv.hooks, h)
	}

	return srv, nil
}

// Addr returns the address the server is listening on.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// ReloadHooks cancels the current hook set, waits for them to stop, then
// starts a new set from cfg.Hooks. The gRPC listener and backend connection
// are not affected. No-ops if Stop has already been called.
func (s *Server) ReloadHooks(cfg *config.Config) {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	cancel := s.hookCancel
	old := s.hooks
	s.mu.Unlock()

	if cancel != nil {
		cancel()
		for _, h := range old {
			h.Wait()
		}
	}

	hookCtx, hookCancel := context.WithCancel(context.Background())
	var hooks []*hookRunner
	for _, hcfg := range cfg.Hooks {
		h, err := newHookRunner(hcfg, s.mux)
		if err != nil {
			slog.Warn("hook reload failed", "hook", hcfg.Name, "err", err)
			continue
		}
		h.Start(hookCtx)
		hooks = append(hooks, h)
	}

	s.mu.Lock()
	s.hookCancel = hookCancel
	s.hooks = hooks
	s.mu.Unlock()
	slog.Info("hooks reloaded", "count", len(hooks))
}

// Serve blocks until the server stops.
func (s *Server) Serve() error {
	return s.grpc.Serve(s.listener)
}

// Stop gracefully drains in-flight RPCs, stops hook runners, then closes
// the backend resources.
//
// Shutdown order: cancel hooks first so they stop poking the mux, then drain
// gRPC RPCs, then close the mux and backend connection.
func (s *Server) Stop(ctx context.Context) {
	s.mu.Lock()
	s.stopped = true
	cancel := s.hookCancel
	hooks := s.hooks
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	for _, h := range hooks {
		h.Wait()
	}
	s.grpc.GracefulStop()
	s.mux.Close(ctx)
	if s.closer != nil {
		if err := s.closer.Close(); err != nil {
			slog.Warn("error closing backend", "err", err)
		}
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

// closerFunc adapts a plain func() error to io.Closer.
// Used by drivers that need to close multiple resources (e.g. MongoDB client + meta store).
type closerFunc func() error

func (f closerFunc) Close() error { return f() }

// buildTLS constructs gRPC transport credentials from cert/key/CA paths.
// A non-empty CA enables mutual TLS: clients must present a certificate signed
// by that CA.
func buildTLS(tlsCfg config.TLSConfig) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(tlsCfg.Cert, tlsCfg.Key)
	if err != nil {
		return nil, fmt.Errorf("load cert/key: %w", err)
	}
	tc := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
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
