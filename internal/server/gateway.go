package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	ledgerv1 "github.com/rbaliyan/ledger/api/ledger/v1"
	"github.com/rbaliyan/ledger/internal/config"
	"github.com/rbaliyan/ledger/ledgerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// newGatewayHandler dials the local gRPC server and returns an HTTP handler
// that translates REST requests to gRPC calls.
//
// The x-ledger-store and x-api-key HTTP headers are forwarded as gRPC metadata
// so the existing server-side auth interceptors handle authentication.
func newGatewayHandler(ctx context.Context, grpcAddr string, cfg *config.Config) (http.Handler, error) {
	dialOpts, err := gatewayDialOpts(cfg)
	if err != nil {
		return nil, fmt.Errorf("gateway: dial options: %w", err)
	}

	mux := runtime.NewServeMux(
		runtime.WithIncomingHeaderMatcher(gatewayHeaderMatcher),
	)
	if err := ledgerv1.RegisterLedgerServiceHandlerFromEndpoint(ctx, mux, grpcAddr, dialOpts); err != nil {
		return nil, fmt.Errorf("gateway: register: %w", err)
	}
	return mux, nil
}

// gatewayHeaderMatcher passes x-ledger-store and x-api-key through to gRPC
// metadata in addition to the default grpc-gateway headers.
func gatewayHeaderMatcher(key string) (string, bool) {
	switch strings.ToLower(key) {
	case ledgerpb.StoreMetadataHeader, ledgerpb.APIKeyMetadataHeader:
		return key, true
	}
	return runtime.DefaultHeaderMatcher(key)
}

// gatewayDialOpts builds the gRPC dial options for the gateway→gRPC loopback.
// When TLS is configured on the gRPC server the gateway uses matching credentials;
// otherwise it dials plaintext.
func gatewayDialOpts(cfg *config.Config) ([]grpc.DialOption, error) {
	if cfg.TLS.Cert == "" {
		return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
	}
	// Determine which certificate to trust: prefer an explicit CA, fall back to
	// the server cert itself (self-signed case).
	caPath := cfg.TLS.CA
	if caPath == "" {
		caPath = cfg.TLS.Cert
	}
	pem, err := os.ReadFile(caPath) // #nosec G304 -- path comes from config file, not user input
	if err != nil {
		return nil, fmt.Errorf("read CA cert: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("invalid CA certificate")
	}
	tc := credentials.NewTLS(&tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	})
	return []grpc.DialOption{grpc.WithTransportCredentials(tc)}, nil
}
