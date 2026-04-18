package server

import (
	"context"
	"fmt"

	"github.com/rbaliyan/ledger/ledgerpb"
	"google.golang.org/grpc/metadata"
)

// apiKeyGuard authenticates via the x-api-key gRPC metadata header.
// When apiKey is empty, all requests are allowed unauthenticated.
type apiKeyGuard struct {
	apiKey string
}

// staticIdentity is a minimal Identity implementation.
type staticIdentity struct{ name string }

func (s staticIdentity) UserID() string        { return s.name }
func (s staticIdentity) Claims() map[string]any { return nil }

// Authenticate checks the x-api-key metadata header.
func (g *apiKeyGuard) Authenticate(ctx context.Context) (ledgerpb.Identity, error) {
	if g.apiKey == "" {
		return staticIdentity{name: "anonymous"}, nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("missing metadata")
	}
	keys := md.Get("x-api-key")
	if len(keys) == 0 || keys[0] != g.apiKey {
		return nil, fmt.Errorf("invalid api key")
	}
	return staticIdentity{name: "api-key"}, nil
}

// Authorize allows all authenticated requests.
func (g *apiKeyGuard) Authorize(_ context.Context, _ ledgerpb.Identity, _, _ string) (ledgerpb.Decision, error) {
	return ledgerpb.Decision{Allowed: true, Scope: "all"}, nil
}
