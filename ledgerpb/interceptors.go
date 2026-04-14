package ledgerpb

import (
	"context"
	"fmt"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryInterceptor returns a gRPC unary server interceptor that authenticates
// and authorises every incoming RPC using the provided SecurityGuard.
//
// On success the authenticated Identity is stored in the context and can be
// retrieved by downstream handlers via IdentityFromContext.
//
// Usage:
//
//	grpcSrv := grpc.NewServer(
//	    grpc.UnaryInterceptor(ledgerpb.UnaryInterceptor(myGuard)),
//	)
func UnaryInterceptor(guard SecurityGuard) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		identity, err := guard.Authenticate(ctx)
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "authentication failed: %v", err)
		}
		// action is the fully-qualified RPC name: "/ledger.v1.LedgerService/Append"
		decision, err := guard.Authorize(ctx, identity, "ledger", info.FullMethod)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "authorisation check failed: %v", err)
		}
		if !decision.Allowed {
			return nil, status.Errorf(codes.PermissionDenied, "access denied: %s", decision.Reason)
		}
		return handler(ContextWithIdentity(ctx, identity), req)
	}
}

// StreamInterceptor returns a gRPC streaming server interceptor that authenticates
// and authorises every incoming streaming RPC using the provided SecurityGuard.
func StreamInterceptor(guard SecurityGuard) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		identity, err := guard.Authenticate(ctx)
		if err != nil {
			return status.Errorf(codes.Unauthenticated, "authentication failed: %v", err)
		}
		decision, err := guard.Authorize(ctx, identity, "ledger", info.FullMethod)
		if err != nil {
			return status.Errorf(codes.Internal, "authorisation check failed: %v", err)
		}
		if !decision.Allowed {
			return status.Errorf(codes.PermissionDenied, "access denied: %s", decision.Reason)
		}
		return handler(srv, &wrappedStream{ServerStream: ss, ctx: ContextWithIdentity(ctx, identity)})
	}
}

// wrappedStream injects a new context into a grpc.ServerStream.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

// Middleware returns an HTTP handler middleware that authenticates and authorises
// every incoming HTTP request using the provided SecurityGuard.
//
// The action passed to Authorize is "METHOD:PATH", for example
// "POST:/ledger/v1/streams/orders/append". On success the authenticated Identity
// is stored in the request context.
//
// Usage:
//
//	http.Handle("/", ledgerpb.Middleware(myGuard)(myHandler))
func Middleware(guard SecurityGuard) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			identity, err := guard.Authenticate(r.Context())
			if err != nil {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			action := fmt.Sprintf("%s:%s", r.Method, r.URL.Path)
			decision, err := guard.Authorize(r.Context(), identity, "ledger", action)
			if err != nil {
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
			if !decision.Allowed {
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r.WithContext(ContextWithIdentity(r.Context(), identity)))
		})
	}
}
