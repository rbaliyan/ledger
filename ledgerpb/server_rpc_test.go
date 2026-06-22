package ledgerpb_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"testing"

	"github.com/rbaliyan/ledger"
	ledgerv1 "github.com/rbaliyan/ledger/api/ledger/v1"
	"github.com/rbaliyan/ledger/ledgerpb"
	"github.com/rbaliyan/ledger/sqlite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	_ "modernc.org/sqlite"
)

// newRPCTestClient stands up an in-process gRPC server backed by a SQLite store
// wrapped via NewInt64Provider, with no SecurityGuard interceptor (the security
// path is covered separately in interceptors_test.go). It returns a connected
// client and registers cleanup.
func newRPCTestClient(t *testing.T) ledgerv1.LedgerServiceClient {
	t.Helper()
	provider := newSQLiteProvider(t)
	return serveProvider(t, provider)
}

// newSQLiteProvider opens an in-memory SQLite store and wraps it as an int64
// Provider. SQLite implements ledger.Searcher and StreamRenamer is supplied by
// the muxProvider in production; the bare provider does NOT implement
// StreamRenamer, which lets us exercise the Unimplemented path for RenameStream.
func newSQLiteProvider(t *testing.T) ledgerpb.Provider {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	store, err := sqlite.New(t.Context(), db)
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	t.Cleanup(func() { store.Close(context.Background()) })
	return ledgerpb.NewInt64Provider(store)
}

// serveProvider serves the given provider over a 127.0.0.1:0 gRPC listener and
// returns a connected client.
func serveProvider(t *testing.T, provider ledgerpb.Provider) ledgerv1.LedgerServiceClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	ledgerv1.RegisterLedgerServiceServer(grpcSrv, ledgerpb.NewServer(provider))
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return ledgerv1.NewLedgerServiceClient(conn)
}

// seedStream appends n entries (with a searchable token in the payload) to the
// named stream via the client and returns nothing; failures abort the test.
func seedStream(t *testing.T, client ledgerv1.LedgerServiceClient, ctx context.Context, stream string, payloads ...string) {
	t.Helper()
	entries := make([]*ledgerv1.EntryInput, len(payloads))
	for i, p := range payloads {
		entries[i] = &ledgerv1.EntryInput{Payload: json.RawMessage(p)}
	}
	if _, err := client.Append(ctx, &ledgerv1.AppendRequest{Stream: stream, Entries: entries}); err != nil {
		t.Fatalf("seed Append: %v", err)
	}
}

func TestServerStat_HappyPath(t *testing.T) {
	client := newRPCTestClient(t)
	ctx := context.Background()

	seedStream(t, client, ctx, "stat-stream", `{"event":"a"}`, `{"event":"b"}`, `{"event":"c"}`)

	resp, err := client.Stat(ctx, &ledgerv1.StatRequest{Stream: "stat-stream"})
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if resp.Stream != "stat-stream" {
		t.Errorf("Stat.Stream = %q, want stat-stream", resp.Stream)
	}
	if resp.Count != 3 {
		t.Errorf("Stat.Count = %d, want 3", resp.Count)
	}
	if resp.FirstId == "" || resp.LastId == "" {
		t.Errorf("Stat first/last = %q/%q, want non-empty", resp.FirstId, resp.LastId)
	}
	if resp.FirstId == resp.LastId {
		t.Errorf("Stat first == last (%q) for 3 entries", resp.FirstId)
	}
}

func TestServerStat_EmptyStreamArg(t *testing.T) {
	client := newRPCTestClient(t)
	_, err := client.Stat(context.Background(), &ledgerv1.StatRequest{Stream: ""})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Stat(empty stream) code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestServerSearch_HappyPath(t *testing.T) {
	client := newRPCTestClient(t)
	ctx := context.Background()

	seedStream(t, client, ctx, "search-stream",
		`{"event":"needle"}`,
		`{"event":"haystack"}`,
		`{"event":"haystack"}`,
	)

	resp, err := client.Search(ctx, &ledgerv1.SearchRequest{Stream: "search-stream", Query: "needle"})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(resp.Entries) != 1 {
		t.Errorf("Search(needle) returned %d entries, want 1", len(resp.Entries))
	}
}

func TestServerSearch_EmptyQuery(t *testing.T) {
	client := newRPCTestClient(t)
	_, err := client.Search(context.Background(), &ledgerv1.SearchRequest{Stream: "s", Query: ""})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Search(empty query) code = %v, want InvalidArgument", status.Code(err))
	}
}

// TestServerSearch_Unimplemented drives Search against a provider whose
// underlying store does not implement ledger.Searcher. The adapter returns
// ledger.ErrNotSupported, which the server maps to codes.Unimplemented.
func TestServerSearch_Unimplemented(t *testing.T) {
	provider := ledgerpb.NewStringProvider(newStringStore())
	client := serveProvider(t, provider)
	ctx := context.Background()

	// stringStore does not implement Searcher; the query is non-empty so the
	// handler reaches the searcher delegation path.
	_, err := client.Search(ctx, &ledgerv1.SearchRequest{Stream: "s", Query: "anything"})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("Search on non-searcher backend code = %v, want Unimplemented", status.Code(err))
	}
}

// TestServerRenameStream_Unimplemented drives RenameStream against a bare
// provider that does not implement StreamRenamer (only the muxProvider does).
func TestServerRenameStream_Unimplemented(t *testing.T) {
	client := newRPCTestClient(t)
	_, err := client.RenameStream(context.Background(), &ledgerv1.RenameStreamRequest{
		Name:    "old",
		NewName: "new",
	})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("RenameStream on non-renamer backend code = %v, want Unimplemented", status.Code(err))
	}
}

func TestServerRenameStream_EmptyArgs(t *testing.T) {
	client := newRPCTestClient(t)
	_, err := client.RenameStream(context.Background(), &ledgerv1.RenameStreamRequest{Name: "", NewName: ""})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("RenameStream(empty) code = %v, want InvalidArgument", status.Code(err))
	}
}

// TestServerRenameStream_HappyAndNotFound drives RenameStream through a provider
// that DOES implement StreamRenamer, exercising both the success path and the
// mapped error path for a missing stream.
func TestServerRenameStream_HappyAndNotFound(t *testing.T) {
	provider := &renamerProvider{Provider: newSQLiteProvider(t)}
	client := serveProvider(t, provider)
	ctx := context.Background()

	if _, err := client.RenameStream(ctx, &ledgerv1.RenameStreamRequest{Name: "a", NewName: "b"}); err != nil {
		t.Fatalf("RenameStream happy path: %v", err)
	}
	if !provider.called {
		t.Error("expected RenameStream to delegate to the renamer")
	}

	_, err := client.RenameStream(ctx, &ledgerv1.RenameStreamRequest{Name: "missing", NewName: "x"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("RenameStream(missing) code = %v, want NotFound", status.Code(err))
	}
}

// renamerProvider wraps a Provider and adds a StreamRenamer implementation so
// the server's renamer type-assertion succeeds. It reports ErrStreamNotFound for
// the sentinel name "missing".
type renamerProvider struct {
	ledgerpb.Provider
	called bool
}

func (p *renamerProvider) RenameStream(_ context.Context, oldName, _ string) error {
	p.called = true
	if oldName == "missing" {
		return ledger.ErrStreamNotFound
	}
	return nil
}

var _ ledgerpb.StreamRenamer = (*renamerProvider)(nil)
