package ledger

import "errors"

var (
	// ErrStoreClosed is returned when operating on a closed store.
	ErrStoreClosed = errors.New("ledger: store closed")

	// ErrEncode is returned when payload encoding fails.
	ErrEncode = errors.New("ledger: encode failed")

	// ErrDecode is returned when payload decoding fails.
	ErrDecode = errors.New("ledger: decode failed")

	// ErrNoUpcaster is returned when no upcaster is available for a version transition.
	ErrNoUpcaster = errors.New("ledger: no upcaster available")

	// ErrInvalidCursor is returned when a cursor value has an unexpected type.
	ErrInvalidCursor = errors.New("ledger: invalid cursor type")

	// ErrInvalidName is returned when a table or collection name is invalid.
	ErrInvalidName = errors.New("ledger: invalid table/collection name")

	// ErrEntryNotFound is returned when SetTags or SetAnnotations targets a non-existent entry.
	ErrEntryNotFound = errors.New("ledger: entry not found")
)
