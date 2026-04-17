package replicate

import (
	"context"

	"github.com/rbaliyan/ledger"
)

// ReadOnlyStream wraps a [ledger.Stream] and returns [ledger.ErrReadOnly] for
// all write operations. Use this for sink streams to prevent accidental writes.
type ReadOnlyStream[I comparable, P any, T any] struct {
	inner ledger.Stream[I, P, T]
}

// NewReadOnlyStream wraps s so that Append, SetTags, and SetAnnotations return [ledger.ErrReadOnly].
func NewReadOnlyStream[I comparable, P any, T any](s ledger.Stream[I, P, T]) ReadOnlyStream[I, P, T] {
	return ReadOnlyStream[I, P, T]{inner: s}
}

// ID returns the stream instance ID.
func (s ReadOnlyStream[I, P, T]) ID() string { return s.inner.ID() }

// SchemaVersion returns the schema version used for new entries.
func (s ReadOnlyStream[I, P, T]) SchemaVersion() int { return s.inner.SchemaVersion() }

// Read returns decoded entries. Upcasting is applied as normal.
func (s ReadOnlyStream[I, P, T]) Read(ctx context.Context, opts ...ledger.ReadOption) ([]ledger.Entry[I, T], error) {
	return s.inner.Read(ctx, opts...)
}

// Append always returns [ledger.ErrReadOnly].
func (s ReadOnlyStream[I, P, T]) Append(_ context.Context, _ ...ledger.AppendInput[T]) ([]I, error) {
	return nil, ledger.ErrReadOnly
}

// SetTags always returns [ledger.ErrReadOnly].
func (s ReadOnlyStream[I, P, T]) SetTags(_ context.Context, _ I, _ []string) error {
	return ledger.ErrReadOnly
}

// SetAnnotations always returns [ledger.ErrReadOnly].
func (s ReadOnlyStream[I, P, T]) SetAnnotations(_ context.Context, _ I, _ map[string]*string) error {
	return ledger.ErrReadOnly
}
