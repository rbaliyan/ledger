package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	ledgerv1 "github.com/rbaliyan/ledger/api/ledger/v1"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// usageErrorf prints the command's usage to stderr and returns a formatted error.
// Use this for flag/argument validation failures so the user sees both the
// error message and a reminder of the correct usage.
func usageErrorf(cmd *cobra.Command, format string, args ...any) error {
	err := fmt.Errorf(format, args...)
	_ = cmd.Usage()
	return err
}

func newStreamCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stream",
		Short: "Stream management commands",
	}
	cmd.AddCommand(
		newStreamListCmd(),
		newStreamAppendCmd(),
		newStreamReadCmd(),
		newStreamCountCmd(),
		newStreamTagCmd(),
		newStreamAnnotateCmd(),
		newStreamTrimCmd(),
		newStreamTailCmd(),
	)
	return cmd
}

// ── list ────────────────────────────────────────────────────────────────────

func newStreamListCmd() *cobra.Command {
	var store, after string
	var limit int64

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List stream IDs in a store",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := clientConfig()
			if err != nil {
				return err
			}
			client, conn, err := newClient(cfg)
			if err != nil {
				return err
			}
			defer conn.Close() //nolint:errcheck

			ctx := storeCtx(cmd.Context(), cfg, store)
			resp, err := client.ListStreamIDs(ctx, &ledgerv1.ListStreamIDsRequest{
				After: after,
				Limit: limit,
			})
			if err != nil {
				return err
			}
			for _, id := range resp.StreamIds {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), id)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&store, "store", "s", "ledger_entries", "store (table/collection) name")
	cmd.Flags().StringVar(&after, "after", "", "cursor: list stream IDs after this value")
	cmd.Flags().Int64Var(&limit, "limit", 0, "max results (0 = server default)")
	return cmd
}

// ── append ──────────────────────────────────────────────────────────────────

func newStreamAppendCmd() *cobra.Command {
	var store, stream, orderKey, dedupKey string
	var meta, tags []string

	cmd := &cobra.Command{
		Use:   "append <json-payload>",
		Short: "Append an entry to a stream",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if stream == "" {
				return usageErrorf(cmd, "--stream is required")
			}
			payload := []byte(args[0])
			if !json.Valid(payload) {
				return fmt.Errorf("payload is not valid JSON")
			}

			kv, err := parseKV(meta)
			if err != nil {
				return err
			}

			cfg, err := clientConfig()
			if err != nil {
				return err
			}
			client, conn, err := newClient(cfg)
			if err != nil {
				return err
			}
			defer conn.Close() //nolint:errcheck

			ctx := storeCtx(cmd.Context(), cfg, store)
			resp, err := client.Append(ctx, &ledgerv1.AppendRequest{
				Stream: stream,
				Entries: []*ledgerv1.EntryInput{{
					Payload:  payload,
					OrderKey: orderKey,
					DedupKey: dedupKey,
					Metadata: kv,
					Tags:     tags,
				}},
			})
			if err != nil {
				return err
			}
			for _, id := range resp.Ids {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), id)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&store, "store", "s", "ledger_entries", "store (table/collection) name")
	cmd.Flags().StringVarP(&stream, "stream", "S", "", "stream ID (required)")
	cmd.Flags().StringVar(&orderKey, "order-key", "", "ordering key")
	cmd.Flags().StringVar(&dedupKey, "dedup-key", "", "deduplication key")
	cmd.Flags().StringArrayVar(&meta, "meta", nil, "metadata as key=value pairs")
	cmd.Flags().StringArrayVar(&tags, "tag", nil, "tags to apply to the entry")
	return cmd
}

// ── read ─────────────────────────────────────────────────────────────────────

func newStreamReadCmd() *cobra.Command {
	var store, stream, after, orderKey, tag string
	var limit int64
	var desc bool

	cmd := &cobra.Command{
		Use:   "read",
		Short: "Read entries from a stream",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if stream == "" {
				return usageErrorf(cmd, "--stream is required")
			}
			cfg, err := clientConfig()
			if err != nil {
				return err
			}
			client, conn, err := newClient(cfg)
			if err != nil {
				return err
			}
			defer conn.Close() //nolint:errcheck

			ctx := storeCtx(cmd.Context(), cfg, store)
			resp, err := client.Read(ctx, &ledgerv1.ReadRequest{
				Stream: stream,
				Options: &ledgerv1.ReadOptions{
					After:    after,
					Limit:    limit,
					Desc:     desc,
					OrderKey: orderKey,
					Tag:      tag,
				},
			})
			if err != nil {
				return err
			}
			enc := json.NewEncoder(cmd.OutOrStdout())
			for _, e := range resp.Entries {
				if err := enc.Encode(e); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&store, "store", "s", "ledger_entries", "store (table/collection) name")
	cmd.Flags().StringVarP(&stream, "stream", "S", "", "stream ID (required)")
	cmd.Flags().StringVar(&after, "after", "", "cursor: only entries after this ID")
	cmd.Flags().Int64Var(&limit, "limit", 0, "max entries (0 = server default)")
	cmd.Flags().BoolVar(&desc, "desc", false, "newest first")
	cmd.Flags().StringVar(&orderKey, "order-key", "", "filter by order key")
	cmd.Flags().StringVar(&tag, "tag", "", "filter by tag")
	return cmd
}

// ── count ─────────────────────────────────────────────────────────────────────

func newStreamCountCmd() *cobra.Command {
	var store, stream string

	cmd := &cobra.Command{
		Use:   "count",
		Short: "Count entries in a stream",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if stream == "" {
				return usageErrorf(cmd, "--stream is required")
			}
			cfg, err := clientConfig()
			if err != nil {
				return err
			}
			client, conn, err := newClient(cfg)
			if err != nil {
				return err
			}
			defer conn.Close() //nolint:errcheck

			ctx := storeCtx(cmd.Context(), cfg, store)
			resp, err := client.Count(ctx, &ledgerv1.CountRequest{Stream: stream})
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), resp.Count)
			return nil
		},
	}
	cmd.Flags().StringVarP(&store, "store", "s", "ledger_entries", "store (table/collection) name")
	cmd.Flags().StringVarP(&stream, "stream", "S", "", "stream ID (required)")
	return cmd
}

// ── tag ──────────────────────────────────────────────────────────────────────

func newStreamTagCmd() *cobra.Command {
	var store, stream, entryID string
	var tags []string

	cmd := &cobra.Command{
		Use:   "tag",
		Short: "Replace all tags on an entry",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if stream == "" {
				return usageErrorf(cmd, "--stream is required")
			}
			if entryID == "" {
				return usageErrorf(cmd, "--id is required")
			}
			cfg, err := clientConfig()
			if err != nil {
				return err
			}
			client, conn, err := newClient(cfg)
			if err != nil {
				return err
			}
			defer conn.Close() //nolint:errcheck

			ctx := storeCtx(cmd.Context(), cfg, store)
			_, err = client.SetTags(ctx, &ledgerv1.SetTagsRequest{
				Stream: stream,
				Id:     entryID,
				Tags:   tags,
			})
			return err
		},
	}
	cmd.Flags().StringVarP(&store, "store", "s", "ledger_entries", "store (table/collection) name")
	cmd.Flags().StringVarP(&stream, "stream", "S", "", "stream ID (required)")
	cmd.Flags().StringVar(&entryID, "id", "", "entry ID (required)")
	cmd.Flags().StringArrayVarP(&tags, "tag", "t", nil, "tags (repeatable; replaces all existing tags)")
	return cmd
}

// ── annotate ─────────────────────────────────────────────────────────────────

func newStreamAnnotateCmd() *cobra.Command {
	var store, stream, entryID string
	var pairs []string

	cmd := &cobra.Command{
		Use:   "annotate",
		Short: "Merge annotations into an entry (key= deletes the annotation)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if stream == "" {
				return usageErrorf(cmd, "--stream is required")
			}
			if entryID == "" {
				return usageErrorf(cmd, "--id is required")
			}
			if len(pairs) == 0 {
				return fmt.Errorf("at least one --set key=value or --del key is required")
			}
			cfg, err := clientConfig()
			if err != nil {
				return err
			}
			client, conn, err := newClient(cfg)
			if err != nil {
				return err
			}
			defer conn.Close() //nolint:errcheck

			set := make(map[string]string)
			var del []string
			for _, kv := range pairs {
				k, v, hasEq := strings.Cut(kv, "=")
				if hasEq && v == "" {
					del = append(del, k)
				} else {
					set[k] = v
				}
			}

			ctx := storeCtx(cmd.Context(), cfg, store)
			_, err = client.SetAnnotations(ctx, &ledgerv1.SetAnnotationsRequest{
				Stream: stream,
				Id:     entryID,
				Set:    set,
				Delete: del,
			})
			return err
		},
	}
	cmd.Flags().StringVarP(&store, "store", "s", "ledger_entries", "store (table/collection) name")
	cmd.Flags().StringVarP(&stream, "stream", "S", "", "stream ID (required)")
	cmd.Flags().StringVar(&entryID, "id", "", "entry ID (required)")
	cmd.Flags().StringArrayVar(&pairs, "set", nil, "annotation key=value to set; key= to delete (repeatable)")
	return cmd
}

// ── trim ─────────────────────────────────────────────────────────────────────

func newStreamTrimCmd() *cobra.Command {
	var store, stream, beforeID string

	cmd := &cobra.Command{
		Use:   "trim",
		Short: "Trim entries with ID <= --before from a stream",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if stream == "" {
				return usageErrorf(cmd, "--stream is required")
			}
			if beforeID == "" {
				return usageErrorf(cmd, "--before is required")
			}
			cfg, err := clientConfig()
			if err != nil {
				return err
			}
			client, conn, err := newClient(cfg)
			if err != nil {
				return err
			}
			defer conn.Close() //nolint:errcheck

			ctx := storeCtx(cmd.Context(), cfg, store)
			resp, err := client.Trim(ctx, &ledgerv1.TrimRequest{
				Stream:   stream,
				BeforeId: beforeID,
			})
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "trimmed %d entries\n", resp.Deleted)
			return nil
		},
	}
	cmd.Flags().StringVarP(&store, "store", "s", "ledger_entries", "store (table/collection) name")
	cmd.Flags().StringVarP(&stream, "stream", "S", "", "stream ID (required)")
	cmd.Flags().StringVar(&beforeID, "before", "", "trim entries with ID <= this value (required)")
	return cmd
}

// ── tail ─────────────────────────────────────────────────────────────────────

func newStreamTailCmd() *cobra.Command {
	var store, stream string
	var interval time.Duration
	var limit int64

	cmd := &cobra.Command{
		Use:   "tail",
		Short: "Continuously poll a stream and print new entries",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if stream == "" {
				return usageErrorf(cmd, "--stream is required")
			}
			cfg, err := clientConfig()
			if err != nil {
				return err
			}
			client, conn, err := newClient(cfg)
			if err != nil {
				return err
			}
			defer conn.Close() //nolint:errcheck

			ctx := storeCtx(cmd.Context(), cfg, store)
			return tailStream(ctx, cmd.OutOrStdout(), client, stream, limit, interval)
		},
	}
	cmd.Flags().StringVarP(&store, "store", "s", "ledger_entries", "store (table/collection) name")
	cmd.Flags().StringVarP(&stream, "stream", "S", "", "stream ID (required)")
	cmd.Flags().DurationVar(&interval, "interval", 2*time.Second, "polling interval")
	cmd.Flags().Int64Var(&limit, "limit", 50, "entries per poll")
	return cmd
}

func tailStream(
	ctx context.Context,
	out io.Writer,
	client ledgerv1.LedgerServiceClient,
	streamID string,
	limit int64,
	interval time.Duration,
) error {
	enc := json.NewEncoder(out)
	var cursor string
	backoff := interval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		resp, err := client.Read(ctx, &ledgerv1.ReadRequest{
			Stream: streamID,
			Options: &ledgerv1.ReadOptions{
				After: cursor,
				Limit: limit,
			},
		})
		if err != nil {
			if isRetryable(err) {
				backoff = min(backoff*2, 30*time.Second)
				ticker.Reset(backoff)
				continue
			}
			return err
		}
		if backoff != interval {
			backoff = interval
			ticker.Reset(backoff)
		}
		for _, e := range resp.Entries {
			if err := enc.Encode(e); err != nil {
				return err
			}
			cursor = e.Id
		}
	}
}

// isRetryable reports whether a gRPC error is worth retrying in tail.
func isRetryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

// parseKV converts "key=value" strings to a map.
// Returns an error if any pair does not contain "=".
func parseKV(pairs []string) (map[string]string, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	m := make(map[string]string, len(pairs))
	for _, kv := range pairs {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return nil, fmt.Errorf("invalid metadata pair %q: expected key=value", kv)
		}
		m[k] = v
	}
	return m, nil
}
