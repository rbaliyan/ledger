// Package ledgerpb exposes the ledger.Store API as a gRPC service with
// pluggable authentication and authorisation via SecurityGuard.
//
// # Wire architecture
//
//	client
//	  │
//	  ▼  gRPC  (proto/ledger/v1/ledger.proto)
//	[Server]  ──[UnaryInterceptor(guard)]──► authenticate → authorise
//	  │
//	  ▼  Backend interface  (string IDs · json.RawMessage payloads)
//	[NewInt64Backend(store)]   wraps Store[int64, json.RawMessage]  (SQLite, PostgreSQL)
//	[NewStringBackend(store)]  wraps Store[string, json.RawMessage] (MongoDB)
//
// # Quick start
//
//	// 1. Open the backend
//	sqlStore, _ := sqlite.New(ctx, db)
//
//	// 2. Wrap as a Backend
//	backend := ledgerpb.NewInt64Backend(sqlStore)
//
//	// 3. Create the gRPC Server
//	srv := ledgerpb.NewServer(backend)
//
//	// 4. Register with a *grpc.Server (optionally add the security interceptor)
//	grpcSrv := grpc.NewServer(
//	    grpc.UnaryInterceptor(ledgerpb.UnaryInterceptor(myGuard)),
//	    grpc.StreamInterceptor(ledgerpb.StreamInterceptor(myGuard)),
//	)
//	ledgerv1.RegisterLedgerServiceServer(grpcSrv, srv)
package ledgerpb

import (
	"context"
	"encoding/json"

	ledgerv1 "github.com/rbaliyan/ledger/api/ledger/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// compile-time check
var _ ledgerv1.LedgerServiceServer = (*Server)(nil)

// Server implements ledgerv1.LedgerServiceServer backed by a Backend.
type Server struct {
	ledgerv1.UnimplementedLedgerServiceServer
	backend Backend
}

// NewServer creates a LedgerService gRPC server backed by the provided Backend.
// Use NewInt64Backend or NewStringBackend to create a Backend from an existing
// ledger store.
func NewServer(backend Backend) *Server {
	return &Server{backend: backend}
}

// Append adds entries to the named stream and returns their assigned IDs.
func (s *Server) Append(ctx context.Context, req *ledgerv1.AppendRequest) (*ledgerv1.AppendResponse, error) {
	if req.Stream == "" {
		return nil, status.Errorf(codes.InvalidArgument, "stream must not be empty")
	}
	entries := make([]InputEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = InputEntry{
			Payload:       json.RawMessage(e.Payload),
			OrderKey:      e.OrderKey,
			DedupKey:      e.DedupKey,
			SchemaVersion: int(e.SchemaVersion), //nolint:gosec // proto int64 fits in int
			Metadata:      e.Metadata,
			Tags:          e.Tags,
		}
	}
	ids, err := s.backend.Append(ctx, req.Stream, entries...)
	if err != nil {
		return nil, toGRPCStatus(err)
	}
	return &ledgerv1.AppendResponse{Ids: ids}, nil
}

// Read returns entries from the named stream according to the supplied options.
func (s *Server) Read(ctx context.Context, req *ledgerv1.ReadRequest) (*ledgerv1.ReadResponse, error) {
	if req.Stream == "" {
		return nil, status.Errorf(codes.InvalidArgument, "stream must not be empty")
	}
	opts := readOptionsFromProto(req.Options)
	stored, err := s.backend.Read(ctx, req.Stream, opts)
	if err != nil {
		return nil, toGRPCStatus(err)
	}
	entries := make([]*ledgerv1.Entry, len(stored))
	for i, e := range stored {
		entries[i] = storedEntryToProto(e)
	}
	return &ledgerv1.ReadResponse{Entries: entries}, nil
}

// Count returns the total number of entries in the named stream.
func (s *Server) Count(ctx context.Context, req *ledgerv1.CountRequest) (*ledgerv1.CountResponse, error) {
	n, err := s.backend.Count(ctx, req.Stream)
	if err != nil {
		return nil, toGRPCStatus(err)
	}
	return &ledgerv1.CountResponse{Count: n}, nil
}

// SetTags replaces all tags on an existing entry.
func (s *Server) SetTags(ctx context.Context, req *ledgerv1.SetTagsRequest) (*ledgerv1.SetTagsResponse, error) {
	if err := s.backend.SetTags(ctx, req.Stream, req.Id, req.Tags); err != nil {
		return nil, toGRPCStatus(err)
	}
	return &ledgerv1.SetTagsResponse{}, nil
}

// SetAnnotations merges annotations onto an existing entry.
// Keys in req.Set are upserted; keys in req.Delete are removed.
func (s *Server) SetAnnotations(ctx context.Context, req *ledgerv1.SetAnnotationsRequest) (*ledgerv1.SetAnnotationsResponse, error) {
	annotations := make(map[string]*string, len(req.Set)+len(req.Delete))
	for k, v := range req.Set {
		v := v
		annotations[k] = &v
	}
	for _, k := range req.Delete {
		annotations[k] = nil
	}
	if err := s.backend.SetAnnotations(ctx, req.Stream, req.Id, annotations); err != nil {
		return nil, toGRPCStatus(err)
	}
	return &ledgerv1.SetAnnotationsResponse{}, nil
}

// Trim deletes entries with ID <= before_id and returns the number deleted.
func (s *Server) Trim(ctx context.Context, req *ledgerv1.TrimRequest) (*ledgerv1.TrimResponse, error) {
	deleted, err := s.backend.Trim(ctx, req.Stream, req.BeforeId)
	if err != nil {
		return nil, toGRPCStatus(err)
	}
	return &ledgerv1.TrimResponse{Deleted: deleted}, nil
}

// ListStreamIDs returns distinct stream IDs that have at least one entry.
func (s *Server) ListStreamIDs(ctx context.Context, req *ledgerv1.ListStreamIDsRequest) (*ledgerv1.ListStreamIDsResponse, error) {
	ids, err := s.backend.ListStreamIDs(ctx, req.After, int(req.Limit))
	if err != nil {
		return nil, toGRPCStatus(err)
	}
	return &ledgerv1.ListStreamIDsResponse{StreamIds: ids}, nil
}

// RenameStream changes the human-readable name of a stream without touching entries.
// Returns Unimplemented if the backend does not support renaming.
func (s *Server) RenameStream(ctx context.Context, req *ledgerv1.RenameStreamRequest) (*ledgerv1.RenameStreamResponse, error) {
	if req.Name == "" || req.NewName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name and new_name must not be empty")
	}
	renamer, ok := s.backend.(StreamRenamer)
	if !ok {
		return nil, status.Errorf(codes.Unimplemented, "backend does not support stream rename")
	}
	if err := renamer.RenameStream(ctx, req.Name, req.NewName); err != nil {
		return nil, toGRPCStatus(err)
	}
	return &ledgerv1.RenameStreamResponse{}, nil
}

// Health reports backend connectivity. The gRPC call always succeeds; the
// status string carries the health information ("ok" or an error description).
func (s *Server) Health(ctx context.Context, req *ledgerv1.HealthRequest) (*ledgerv1.HealthResponse, error) {
	if err := s.backend.Health(ctx); err != nil {
		return &ledgerv1.HealthResponse{Status: err.Error()}, nil
	}
	return &ledgerv1.HealthResponse{Status: "ok"}, nil
}

// readOptionsFromProto converts a *ledgerv1.ReadOptions proto to ReadOptions.
// A nil proto returns zero-value ReadOptions (all defaults).
func readOptionsFromProto(p *ledgerv1.ReadOptions) ReadOptions {
	if p == nil {
		return ReadOptions{}
	}
	return ReadOptions{
		After:    p.After,
		Limit:    int(p.Limit), //nolint:gosec // proto int64 fits in int
		Desc:     p.Desc,
		OrderKey: p.OrderKey,
		Tag:      p.Tag,
		AllTags:  p.AllTags,
	}
}

// storedEntryToProto converts a StoredEntry to its proto representation.
func storedEntryToProto(e StoredEntry) *ledgerv1.Entry {
	pb := &ledgerv1.Entry{
		Id:            e.ID,
		Stream:        e.Stream,
		Payload:       e.Payload,
		OrderKey:      e.OrderKey,
		DedupKey:      e.DedupKey,
		SchemaVersion: int64(e.SchemaVersion),
		Metadata:      e.Metadata,
		Tags:          e.Tags,
		Annotations:   e.Annotations,
		CreatedAt:     timestamppb.New(e.CreatedAt),
	}
	if e.UpdatedAt != nil {
		pb.UpdatedAt = timestamppb.New(*e.UpdatedAt)
	}
	return pb
}
