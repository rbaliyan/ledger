// Package replication provides internal helpers for the ledger replication system.
// It is only accessible to packages within the ledger module.
package replication

import "context"

// MutationStream is the fixed stream name used in every mutation log store.
const MutationStream = "_"

// Mutation type constants used as the "type" field in mutation event JSON.
const (
	TypeAppend         = "append"
	TypeSetTags        = "set_tags"
	TypeSetAnnotations = "set_annotations"
	TypeTrim           = "trim"
)

type sourceIDsKey struct{}

// WithSourceIDs attaches per-entry source IDs to the context.
// Indices correspond 1:1 with the entries slice passed to Store.Append.
// Used by the replicator when writing to a sink store.
func WithSourceIDs(ctx context.Context, ids []string) context.Context {
	return context.WithValue(ctx, sourceIDsKey{}, ids)
}

// SourceIDsFromContext retrieves the source IDs from context.
// Returns nil if none were set.
func SourceIDsFromContext(ctx context.Context) []string {
	ids, _ := ctx.Value(sourceIDsKey{}).([]string)
	return ids
}
