package ledgerpb_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rbaliyan/ledger/ledgerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeIdentity is a minimal Identity for guard round-trips.
type fakeIdentity struct {
	id     string
	claims map[string]any
}

func (f fakeIdentity) UserID() string         { return f.id }
func (f fakeIdentity) Claims() map[string]any { return f.claims }

// fakeGuard is a configurable SecurityGuard used to drive interceptor and
// middleware branches deterministically.
type fakeGuard struct {
	authErr   error // returned by Authenticate when non-nil
	identity  ledgerpb.Identity
	decision  ledgerpb.Decision
	authzErr  error  // returned by Authorize when non-nil
	gotAction string // captured action passed to Authorize
}

func (g *fakeGuard) Authenticate(_ context.Context) (ledgerpb.Identity, error) {
	if g.authErr != nil {
		return nil, g.authErr
	}
	return g.identity, nil
}

func (g *fakeGuard) Authorize(_ context.Context, _ ledgerpb.Identity, _, action string) (ledgerpb.Decision, error) {
	g.gotAction = action
	if g.authzErr != nil {
		return ledgerpb.Decision{}, g.authzErr
	}
	return g.decision, nil
}

// fakeServerStream is a minimal grpc.ServerStream carrying a context, used to
// drive StreamInterceptor without a real connection.
type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *fakeServerStream) Context() context.Context { return s.ctx }

func TestUnaryInterceptor_Allow(t *testing.T) {
	t.Parallel()
	guard := &fakeGuard{
		identity: fakeIdentity{id: "alice"},
		decision: ledgerpb.Decision{Allowed: true},
	}
	interceptor := ledgerpb.UnaryInterceptor(guard)

	var gotIdentity ledgerpb.Identity
	handler := func(ctx context.Context, _ any) (any, error) {
		gotIdentity = ledgerpb.IdentityFromContext(ctx)
		return "ok", nil
	}

	resp, err := interceptor(context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: "/ledger.v1.LedgerService/Append"}, handler)
	if err != nil {
		t.Fatalf("interceptor returned error: %v", err)
	}
	if resp != "ok" {
		t.Errorf("resp = %v, want ok", resp)
	}
	if gotIdentity == nil || gotIdentity.UserID() != "alice" {
		t.Errorf("handler did not receive identity in context: %v", gotIdentity)
	}
	if guard.gotAction != "/ledger.v1.LedgerService/Append" {
		t.Errorf("Authorize action = %q, want the full RPC method", guard.gotAction)
	}
}

func TestUnaryInterceptor_AuthenticateFails(t *testing.T) {
	t.Parallel()
	guard := &fakeGuard{authErr: errors.New("bad token")}
	interceptor := ledgerpb.UnaryInterceptor(guard)

	called := false
	handler := func(context.Context, any) (any, error) { called = true; return nil, nil }

	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/m"}, handler)
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("code = %v, want Unauthenticated", status.Code(err))
	}
	if called {
		t.Error("handler must not be called when authentication fails")
	}
}

func TestUnaryInterceptor_Denied(t *testing.T) {
	t.Parallel()
	guard := &fakeGuard{
		identity: fakeIdentity{id: "bob"},
		decision: ledgerpb.Decision{Allowed: false, Reason: "no access"},
	}
	interceptor := ledgerpb.UnaryInterceptor(guard)

	called := false
	handler := func(context.Context, any) (any, error) { called = true; return nil, nil }

	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/m"}, handler)
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("code = %v, want PermissionDenied", status.Code(err))
	}
	if called {
		t.Error("handler must not be called when access is denied")
	}
}

func TestUnaryInterceptor_AuthorizeErrors(t *testing.T) {
	t.Parallel()
	guard := &fakeGuard{
		identity: fakeIdentity{id: "bob"},
		authzErr: errors.New("policy engine down"),
	}
	interceptor := ledgerpb.UnaryInterceptor(guard)

	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/m"},
		func(context.Context, any) (any, error) { return nil, nil })
	if status.Code(err) != codes.Internal {
		t.Errorf("code = %v, want Internal", status.Code(err))
	}
}

func TestStreamInterceptor_Allow(t *testing.T) {
	t.Parallel()
	guard := &fakeGuard{
		identity: fakeIdentity{id: "carol"},
		decision: ledgerpb.Decision{Allowed: true},
	}
	interceptor := ledgerpb.StreamInterceptor(guard)

	called := false
	var gotIdentity ledgerpb.Identity
	handler := func(_ any, ss grpc.ServerStream) error {
		called = true
		gotIdentity = ledgerpb.IdentityFromContext(ss.Context())
		return nil
	}

	ss := &fakeServerStream{ctx: context.Background()}
	err := interceptor(nil, ss, &grpc.StreamServerInfo{FullMethod: "/ledger.v1.LedgerService/Tail"}, handler)
	if err != nil {
		t.Fatalf("interceptor returned error: %v", err)
	}
	if !called {
		t.Fatal("handler was not called on allow")
	}
	if gotIdentity == nil || gotIdentity.UserID() != "carol" {
		t.Errorf("wrapped stream context missing identity: %v", gotIdentity)
	}
}

func TestStreamInterceptor_Denied(t *testing.T) {
	t.Parallel()
	guard := &fakeGuard{
		identity: fakeIdentity{id: "carol"},
		decision: ledgerpb.Decision{Allowed: false, Reason: "denied"},
	}
	interceptor := ledgerpb.StreamInterceptor(guard)

	called := false
	handler := func(any, grpc.ServerStream) error { called = true; return nil }

	err := interceptor(nil, &fakeServerStream{ctx: context.Background()},
		&grpc.StreamServerInfo{FullMethod: "/m"}, handler)
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("code = %v, want PermissionDenied", status.Code(err))
	}
	if called {
		t.Error("handler must not be called when stream access is denied")
	}
}

func TestStreamInterceptor_AuthenticateFails(t *testing.T) {
	t.Parallel()
	guard := &fakeGuard{authErr: errors.New("nope")}
	interceptor := ledgerpb.StreamInterceptor(guard)

	err := interceptor(nil, &fakeServerStream{ctx: context.Background()},
		&grpc.StreamServerInfo{FullMethod: "/m"},
		func(any, grpc.ServerStream) error { return nil })
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("code = %v, want Unauthenticated", status.Code(err))
	}
}

func TestMiddleware_Allow(t *testing.T) {
	t.Parallel()
	guard := &fakeGuard{
		identity: fakeIdentity{id: "dave"},
		decision: ledgerpb.Decision{Allowed: true},
	}

	var gotIdentity ledgerpb.Identity
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotIdentity = ledgerpb.IdentityFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/ledger/v1/streams/orders/append", nil)
	ledgerpb.Middleware(guard)(next).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if gotIdentity == nil || gotIdentity.UserID() != "dave" {
		t.Errorf("next handler missing identity: %v", gotIdentity)
	}
	if guard.gotAction != "POST:/ledger/v1/streams/orders/append" {
		t.Errorf("Authorize action = %q, want METHOD:PATH form", guard.gotAction)
	}
}

func TestMiddleware_AuthenticateFails(t *testing.T) {
	t.Parallel()
	guard := &fakeGuard{authErr: errors.New("missing key")}

	called := false
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	ledgerpb.Middleware(guard)(next).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
	if called {
		t.Error("next handler must not run when authentication fails")
	}
}

func TestMiddleware_Denied(t *testing.T) {
	t.Parallel()
	guard := &fakeGuard{
		identity: fakeIdentity{id: "x"},
		decision: ledgerpb.Decision{Allowed: false},
	}

	called := false
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	ledgerpb.Middleware(guard)(next).ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", rec.Code)
	}
	if called {
		t.Error("next handler must not run when access is denied")
	}
}

func TestMiddleware_AuthorizeErrors(t *testing.T) {
	t.Parallel()
	guard := &fakeGuard{
		identity: fakeIdentity{id: "x"},
		authzErr: errors.New("policy backend down"),
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	ledgerpb.Middleware(guard)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

func TestIdentityFromContext_RoundTrip(t *testing.T) {
	t.Parallel()
	id := fakeIdentity{id: "round-trip", claims: map[string]any{"role": "admin"}}
	ctx := ledgerpb.ContextWithIdentity(context.Background(), id)

	got := ledgerpb.IdentityFromContext(ctx)
	if got == nil {
		t.Fatal("IdentityFromContext returned nil after ContextWithIdentity")
	}
	if got.UserID() != "round-trip" {
		t.Errorf("UserID = %q, want round-trip", got.UserID())
	}
	if got.Claims()["role"] != "admin" {
		t.Errorf("Claims[role] = %v, want admin", got.Claims()["role"])
	}
}

func TestIdentityFromContext_Absent(t *testing.T) {
	t.Parallel()
	if got := ledgerpb.IdentityFromContext(context.Background()); got != nil {
		t.Errorf("IdentityFromContext on bare context = %v, want nil", got)
	}
}
