package server

import (
	"context"
	"crypto/subtle"
	"fmt"

	"github.com/rbaliyan/ledger/ledgerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// compile-time check
var _ ledgerpb.SecurityGuard = (*apiKeyGuard)(nil)

// apiKeyGuard authenticates via the x-api-key gRPC metadata header.
// When apiKey is empty, all requests are allowed unauthenticated.
// When allowedStores is non-empty, only those store names are accessible.
type apiKeyGuard struct {
	apiKey        string
	allowedStores map[string]struct{} // nil = all stores allowed
}

// newAPIKeyGuard creates a guard from the config fields.
func newAPIKeyGuard(apiKey string, allowedStores []string) *apiKeyGuard {
	g := &apiKeyGuard{apiKey: apiKey}
	if len(allowedStores) > 0 {
		g.allowedStores = make(map[string]struct{}, len(allowedStores))
		for _, s := range allowedStores {
			g.allowedStores[s] = struct{}{}
		}
	}
	return g
}

// staticIdentity is a minimal Identity implementation.
type staticIdentity struct{ name string }

func (s staticIdentity) UserID() string         { return s.name }
func (s staticIdentity) Claims() map[string]any { return nil }

// Authenticate checks the x-api-key metadata header.
func (g *apiKeyGuard) Authenticate(ctx context.Context) (ledgerpb.Identity, error) {
	if g.apiKey == "" {
		return staticIdentity{name: "anonymous"}, nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing gRPC metadata")
	}
	keys := md.Get(ledgerpb.APIKeyMetadataHeader)
	if len(keys) == 0 || subtle.ConstantTimeCompare([]byte(keys[0]), []byte(g.apiKey)) != 1 {
		return nil, status.Error(codes.Unauthenticated, "invalid api key")
	}
	return staticIdentity{name: "api-key"}, nil
}

// Authorize allows all authenticated requests, subject to the optional store
// allow-list. When allowedStores is non-empty, the x-ledger-store header must
// name one of the permitted stores; otherwise Decision.Allowed is false.
func (g *apiKeyGuard) Authorize(ctx context.Context, _ ledgerpb.Identity, _, _ string) (ledgerpb.Decision, error) {
	if g.allowedStores == nil {
		return ledgerpb.Decision{Allowed: true}, nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ledgerpb.Decision{Allowed: false, Reason: "missing gRPC metadata"}, nil
	}
	vals := md.Get(ledgerpb.StoreMetadataHeader)
	if len(vals) == 0 || vals[0] == "" {
		// Methods that don't require a store header (e.g. Health) are always allowed.
		return ledgerpb.Decision{Allowed: true}, nil
	}
	if _, ok := g.allowedStores[vals[0]]; !ok {
		return ledgerpb.Decision{Allowed: false, Reason: fmt.Sprintf("store %q is not accessible with this API key", vals[0])}, nil
	}
	return ledgerpb.Decision{Allowed: true}, nil
}
