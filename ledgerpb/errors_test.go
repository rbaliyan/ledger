package ledgerpb

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/rbaliyan/ledger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToGRPCStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"nil", nil, codes.OK},
		{"context_canceled", context.Canceled, codes.Canceled},
		{"deadline_exceeded", context.DeadlineExceeded, codes.DeadlineExceeded},
		{"store_closed", ledger.ErrStoreClosed, codes.Unavailable},
		{"entry_not_found", ledger.ErrEntryNotFound, codes.NotFound},
		{"invalid_cursor", ledger.ErrInvalidCursor, codes.InvalidArgument},
		{"invalid_name", ledger.ErrInvalidName, codes.InvalidArgument},
		{"encode", ledger.ErrEncode, codes.Internal},
		{"decode", ledger.ErrDecode, codes.Internal},
		{"no_upcaster", ledger.ErrNoUpcaster, codes.Internal},
		{"not_supported", ledger.ErrNotSupported, codes.Unimplemented},
		{"read_only", ledger.ErrReadOnly, codes.FailedPrecondition},
		{"stream_not_found", ledger.ErrStreamNotFound, codes.NotFound},
		{"stream_exists", ledger.ErrStreamExists, codes.AlreadyExists},
		{"unknown", errors.New("some other failure"), codes.Internal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := toGRPCStatus(tt.err)
			if tt.err == nil {
				if got != nil {
					t.Fatalf("toGRPCStatus(nil) = %v, want nil", got)
				}
				return
			}
			if status.Code(got) != tt.want {
				t.Errorf("toGRPCStatus(%v) code = %v, want %v", tt.err, status.Code(got), tt.want)
			}
		})
	}
}

// TestToGRPCStatus_WrappedSentinel confirms wrapped sentinels are still mapped
// via errors.Is, not just bare sentinel values.
func TestToGRPCStatus_WrappedSentinel(t *testing.T) {
	t.Parallel()
	wrapped := fmt.Errorf("append failed: %w", ledger.ErrStreamExists)
	if got := status.Code(toGRPCStatus(wrapped)); got != codes.AlreadyExists {
		t.Errorf("wrapped ErrStreamExists code = %v, want AlreadyExists", got)
	}
}
