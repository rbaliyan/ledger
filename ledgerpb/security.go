// Package ledgerpb exposes the ledger.Store API as a gRPC service with
// pluggable authentication and authorisation via SecurityGuard.
package ledgerpb

import "context"

// Identity represents the authenticated subject extracted from an incoming request.
type Identity interface {
	// UserID returns the unique identifier of the authenticated user or service account.
	UserID() string
	// Claims returns arbitrary metadata attached to the identity
	// (e.g. roles, scopes, org_id, tenant_id).
	Claims() map[string]any
}

// Decision is the outcome of an authorisation check.
type Decision struct {
	// Allowed reports whether the action is permitted.
	Allowed bool
	// Scope narrows the breadth of access (e.g. "all", "owned", "tenant").
	// Handlers may retrieve the identity via IdentityFromContext and apply
	// row-level filtering based on the scope.
	Scope string
	// Reason is a human-readable explanation, populated on denial for debugging.
	Reason string
}

// SecurityGuard is the interface callers implement to plug authentication and
// authorisation into the LedgerService gRPC server and HTTP middleware.
//
// Example:
//
//	type myGuard struct{ /* jwt verifier, OPA client, etc. */ }
//
//	func (g *myGuard) Authenticate(ctx context.Context) (ledgerpb.Identity, error) {
//	    md, _ := metadata.FromIncomingContext(ctx)
//	    token := md.Get("authorization")[0]
//	    return verifyJWT(token)
//	}
//
//	func (g *myGuard) Authorize(ctx context.Context, id ledgerpb.Identity, resource, action string) (ledgerpb.Decision, error) {
//	    if id.Claims()["role"] == "admin" {
//	        return ledgerpb.Decision{Allowed: true, Scope: "all"}, nil
//	    }
//	    return ledgerpb.Decision{Allowed: false, Reason: "insufficient role"}, nil
//	}
type SecurityGuard interface {
	// Authenticate extracts and validates credentials from the incoming context
	// (gRPC metadata or HTTP headers) and returns the authenticated Identity.
	// Return a non-nil error to reject the request as unauthenticated.
	Authenticate(ctx context.Context) (Identity, error)

	// Authorize decides whether id may perform action on resource.
	//
	// For gRPC interceptors, action is the fully-qualified RPC method:
	//   "/ledger.v1.LedgerService/Append"
	//
	// For HTTP middleware, action is "METHOD:PATH":
	//   "POST:/ledger/v1/streams/orders/append"
	//
	// resource is always "ledger" in this library; it is provided for
	// coarse-grained policy matching (e.g. OPA, Casbin).
	Authorize(ctx context.Context, id Identity, resource, action string) (Decision, error)
}

type identityKey struct{}

// ContextWithIdentity returns a new context carrying the authenticated Identity.
// This is called by the interceptors; handlers retrieve it with IdentityFromContext.
func ContextWithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, id)
}

// IdentityFromContext returns the Identity stored in ctx by the security
// interceptor, or nil if the context carries no identity.
func IdentityFromContext(ctx context.Context) Identity {
	id, _ := ctx.Value(identityKey{}).(Identity)
	return id
}
