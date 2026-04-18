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
)

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
	cmd.Flags().StringVar(&store, "store", "ledger_entries", "store (table/collection) name")
	cmd.Flags().StringVar(&after, "after", "", "cursor: list stream IDs after this value")
	cmd.Flags().Int64Var(&limit, "limit", 0, "max results (0 = server default)")
	return cmd
}

// ── append ──────────────────────────────────────────────────────────────────

func newStreamAppendCmd() *cobra.Command {
	var store, orderKey, dedupKey string
	var meta, tags []string

	cmd := &cobra.Command{
		Use:   "append <stream-id> <json-payload>",
		Short: "Append an entry to a stream",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			payload := []byte(args[1])
			if !json.Valid(payload) {
				return fmt.Errorf("payload is not valid JSON")
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
				Stream: args[0],
				Entries: []*ledgerv1.EntryInput{{
					Payload:  payload,
					OrderKey: orderKey,
					DedupKey: dedupKey,
					Metadata: parseKV(meta),
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
	cmd.Flags().StringVar(&store, "store", "ledger_entries", "store (table/collection) name")
	cmd.Flags().StringVar(&orderKey, "order-key", "", "ordering key")
	cmd.Flags().StringVar(&dedupKey, "dedup-key", "", "deduplication key")
	cmd.Flags().StringArrayVar(&meta, "meta", nil, "metadata key=value pairs")
	cmd.Flags().StringArrayVar(&tags, "tag", nil, "tags")
	return cmd
}

// ── read ─────────────────────────────────────────────────────────────────────

func newStreamReadCmd() *cobra.Command {
	var store, after, orderKey, tag string
	var limit int64
	var desc bool

	cmd := &cobra.Command{
		Use:   "read <stream-id>",
		Short: "Read entries from a stream",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
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
				Stream: args[0],
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
				_ = enc.Encode(e)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&store, "store", "ledger_entries", "store (table/collection) name")
	cmd.Flags().StringVar(&after, "after", "", "cursor: only entries after this ID")
	cmd.Flags().Int64Var(&limit, "limit", 0, "max entries (0 = server default)")
	cmd.Flags().BoolVar(&desc, "desc", false, "newest first")
	cmd.Flags().StringVar(&orderKey, "order-key", "", "filter by order key")
	cmd.Flags().StringVar(&tag, "tag", "", "filter by tag")
	return cmd
}

// ── count ─────────────────────────────────────────────────────────────────────

func newStreamCountCmd() *cobra.Command {
	var store string

	cmd := &cobra.Command{
		Use:   "count <stream-id>",
		Short: "Count entries in a stream",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
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
			resp, err := client.Count(ctx, &ledgerv1.CountRequest{Stream: args[0]})
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), resp.Count)
			return nil
		},
	}
	cmd.Flags().StringVar(&store, "store", "ledger_entries", "store (table/collection) name")
	return cmd
}

// ── tag ──────────────────────────────────────────────────────────────────────

func newStreamTagCmd() *cobra.Command {
	var store string

	cmd := &cobra.Command{
		Use:   "tag <stream-id> <entry-id> [tag ...]",
		Short: "Replace all tags on an entry",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
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
				Stream: args[0],
				Id:     args[1],
				Tags:   args[2:],
			})
			return err
		},
	}
	cmd.Flags().StringVar(&store, "store", "ledger_entries", "store (table/collection) name")
	return cmd
}

// ── annotate ─────────────────────────────────────────────────────────────────

func newStreamAnnotateCmd() *cobra.Command {
	var store string

	cmd := &cobra.Command{
		Use:   "annotate <stream-id> <entry-id> <key=value|key=>...",
		Short: "Merge annotations into an entry (key= deletes the annotation)",
		Args:  cobra.MinimumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
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
			for _, kv := range args[2:] {
				k, v, hasVal := strings.Cut(kv, "=")
				if hasVal && v == "" {
					del = append(del, k)
				} else {
					set[k] = v
				}
			}

			ctx := storeCtx(cmd.Context(), cfg, store)
			_, err = client.SetAnnotations(ctx, &ledgerv1.SetAnnotationsRequest{
				Stream: args[0],
				Id:     args[1],
				Set:    set,
				Delete: del,
			})
			return err
		},
	}
	cmd.Flags().StringVar(&store, "store", "ledger_entries", "store (table/collection) name")
	return cmd
}

// ── trim ─────────────────────────────────────────────────────────────────────

func newStreamTrimCmd() *cobra.Command {
	var store string

	cmd := &cobra.Command{
		Use:   "trim <stream-id> <before-id>",
		Short: "Trim entries with ID <= before-id from a stream",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
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
				Stream:   args[0],
				BeforeId: args[1],
			})
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "trimmed %d entries\n", resp.Deleted)
			return nil
		},
	}
	cmd.Flags().StringVar(&store, "store", "ledger_entries", "store (table/collection) name")
	return cmd
}

// ── tail ─────────────────────────────────────────────────────────────────────

func newStreamTailCmd() *cobra.Command {
	var store string
	var interval time.Duration
	var limit int64

	cmd := &cobra.Command{
		Use:   "tail <stream-id>",
		Short: "Continuously poll a stream and print new entries",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
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
			return tailStream(ctx, cmd.OutOrStdout(), client, args[0], limit, interval)
		},
	}
	cmd.Flags().StringVar(&store, "store", "ledger_entries", "store (table/collection) name")
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
			return err
		}
		for _, e := range resp.Entries {
			_ = enc.Encode(e)
			cursor = e.Id
		}
	}
}

// parseKV converts "key=value" strings to a map.
func parseKV(pairs []string) map[string]string {
	if len(pairs) == 0 {
		return nil
	}
	m := make(map[string]string, len(pairs))
	for _, kv := range pairs {
		k, v, _ := strings.Cut(kv, "=")
		m[k] = v
	}
	return m
}
