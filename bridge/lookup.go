package bridge

import "context"

// sinkLookup is the interface that Bridge sink stores may implement to support
// applying SetTags, SetAnnotations, and Trim mutations from the source.
// Backends satisfy this structurally — they need not import the bridge package.
type sinkLookup[DI comparable] interface {
	FindBySourceID(ctx context.Context, stream, sourceID string) (DI, bool, error)
}
