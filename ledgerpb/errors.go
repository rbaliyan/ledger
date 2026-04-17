package ledgerpb

import (
	"context"
	"errors"

	"github.com/rbaliyan/ledger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// toGRPCStatus maps ledger sentinel errors to appropriate gRPC status codes.
// Unknown errors are mapped to codes.Internal.
func toGRPCStatus(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Errorf(codes.Canceled, "request cancelled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Errorf(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, ledger.ErrStoreClosed):
		return status.Errorf(codes.Unavailable, "store closed")
	case errors.Is(err, ledger.ErrEntryNotFound):
		return status.Errorf(codes.NotFound, "entry not found")
	case errors.Is(err, ledger.ErrInvalidCursor):
		return status.Errorf(codes.InvalidArgument, "%v", err)
	case errors.Is(err, ledger.ErrInvalidName):
		return status.Errorf(codes.InvalidArgument, "%v", err)
	case errors.Is(err, ledger.ErrEncode):
		return status.Errorf(codes.Internal, "payload encode error: %v", err)
	case errors.Is(err, ledger.ErrDecode):
		return status.Errorf(codes.Internal, "payload decode error: %v", err)
	case errors.Is(err, ledger.ErrNoUpcaster):
		return status.Errorf(codes.Internal, "schema upcaster missing: %v", err)
	case errors.Is(err, ledger.ErrNotSupported):
		return status.Errorf(codes.Unimplemented, "operation not supported by this backend")
	case errors.Is(err, ledger.ErrReadOnly):
		return status.Errorf(codes.FailedPrecondition, "stream is read-only")
	default:
		return status.Errorf(codes.Internal, "internal error: %v", err)
	}
}
