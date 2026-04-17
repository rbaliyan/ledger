// Package replication provides internal helpers for the ledger replication system.
// It is only accessible to packages within the ledger module.
package replication

// MutationStream is the fixed stream name used in every mutation log store.
const MutationStream = "_"

// Mutation type constants used as the "type" field in mutation event JSON.
const (
	TypeAppend         = "append"
	TypeSetTags        = "set_tags"
	TypeSetAnnotations = "set_annotations"
	TypeTrim           = "trim"
)
