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

// UsageError is returned by commands when the user supplies bad flags or
// arguments. main.go detects this type and skips the duplicate error print
// (the error is already shown inline before the usage block).
type UsageError struct{ err error }

func (e UsageError) Error() string { return e.err.Error() }
func (e UsageError) Unwrap() error { return e.err }

// usageErrorf prints "Error: <msg>" followed by the command usage, then
// returns a UsageError so main.go does not print the message a second time.
func usageErrorf(cmd *cobra.Command, format string, args ...any) error {
	err := fmt.Errorf(format, args...)
	_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Error: %s\n\n", err)
	_ = cmd.Usage()
	return UsageError{err: err}
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
		newStreamSearchCmd(),
		newStreamCountCmd(),
		newStreamTagCmd(),
		newStreamAnnotateCmd(),
		newStreamTrimCmd(),
		newStreamTailCmd(),
		newStreamRenameCmd(),
		newStreamStatCmd(),
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
	var jsonMode bool

	cmd := &cobra.Command{
		Use:   "append <payload>",
		Short: "Append an entry to a stream",
		Long: `Append an entry to a stream.

By default the payload is treated as plain text and stored as a JSON string.
Pass --json to supply a raw JSON value (object, array, number, etc.).

Examples:
  ledger stream append "this is my note"
  ledger stream append --json '{"event":"login","user":"alice"}'
  ledger stream append --stream orders --json '{"amount":42}'`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var (
				payload []byte
				err     error
			)
			if jsonMode {
				payload = []byte(args[0])
				if !json.Valid(payload) {
					return fmt.Errorf("payload is not valid JSON (omit --json to store as plain text)")
				}
			} else {
				payload, err = json.Marshal(args[0])
				if err != nil {
					return err
				}
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
	cmd.Flags().StringVarP(&stream, "stream", "S", "default", "stream ID")
	cmd.Flags().BoolVar(&jsonMode, "json", false, "treat payload as a raw JSON value instead of plain text")
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
	var desc, jsonMode bool

	cmd := &cobra.Command{
		Use:   "read",
		Short: "Read entries from a stream",
		Long: `Read entries from a stream.

By default each entry's payload is printed as plain text (one line per entry,
prefixed with its ID). Pass --json to print the full entry as JSON instead.`,
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
			return printEntries(cmd.OutOrStdout(), resp.Entries, jsonMode)
		},
	}
	cmd.Flags().StringVarP(&store, "store", "s", "ledger_entries", "store (table/collection) name")
	cmd.Flags().StringVarP(&stream, "stream", "S", "default", "stream ID")
	cmd.Flags().BoolVar(&jsonMode, "json", false, "print full JSON entry instead of plain text payload")
	cmd.Flags().StringVar(&after, "after", "", "cursor: only entries after this ID")
	cmd.Flags().Int64Var(&limit, "limit", 0, "max entries (0 = server default)")
	cmd.Flags().BoolVar(&desc, "desc", false, "newest first")
	cmd.Flags().StringVar(&orderKey, "order-key", "", "filter by order key")
	cmd.Flags().StringVar(&tag, "tag", "", "filter by tag")
	return cmd
}

// ── search ───────────────────────────────────────────────────────────────────

func newStreamSearchCmd() *cobra.Command {
	var store, stream string
	var limit int64
	var desc, jsonMode bool

	cmd := &cobra.Command{
		Use:   "search <query>",
		Short: "Search for entries matching a query",
		Long: `Search for entries whose payload contains the query string.

Query syntax depends on the backend: SQLite and PostgreSQL use substring
matching (LIKE), MongoDB uses $text (requires a text index on the payload).
If --stream is omitted, all streams in the store are searched.

Examples:
  ledger stream search "failed"
  ledger stream search --stream orders "amount"
  ledger stream search --store events --json "error"`,
		Args: cobra.ExactArgs(1),
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
			resp, err := client.Search(ctx, &ledgerv1.SearchRequest{
				Stream: stream,
				Query:  args[0],
				Options: &ledgerv1.ReadOptions{
					Limit: limit,
					Desc:  desc,
				},
			})
			if err != nil {
				return err
			}
			return printEntries(cmd.OutOrStdout(), resp.Entries, jsonMode)
		},
	}
	cmd.Flags().StringVarP(&store, "store", "s", "ledger_entries", "store (table/collection) name")
	cmd.Flags().StringVarP(&stream, "stream", "S", "", "stream to search (empty = all streams)")
	cmd.Flags().BoolVar(&jsonMode, "json", false, "print full JSON entry instead of plain text payload")
	cmd.Flags().Int64Var(&limit, "limit", 0, "max results (0 = server default)")
	cmd.Flags().BoolVar(&desc, "desc", false, "newest first")
	return cmd
}

// ── count ─────────────────────────────────────────────────────────────────────

func newStreamCountCmd() *cobra.Command {
	var store, stream string

	cmd := &cobra.Command{
		Use:   "count",
		Short: "Count entries in a stream",
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
			resp, err := client.Count(ctx, &ledgerv1.CountRequest{Stream: stream})
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), resp.Count)
			return nil
		},
	}
	cmd.Flags().StringVarP(&store, "store", "s", "ledger_entries", "store (table/collection) name")
	cmd.Flags().StringVarP(&stream, "stream", "S", "default", "stream ID")
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
	cmd.Flags().StringVarP(&stream, "stream", "S", "default", "stream ID")
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
			if entryID == "" {
				return usageErrorf(cmd, "--id is required")
			}
			if len(pairs) == 0 {
				return fmt.Errorf("at least one --set key=value is required (use --set key= to delete an annotation)")
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
	cmd.Flags().StringVarP(&stream, "stream", "S", "default", "stream ID")
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
	cmd.Flags().StringVarP(&stream, "stream", "S", "default", "stream ID")
	cmd.Flags().StringVar(&beforeID, "before", "", "trim entries with ID <= this value (required)")
	return cmd
}

// ── tail ─────────────────────────────────────────────────────────────────────

func newStreamTailCmd() *cobra.Command {
	var store, stream string
	var interval time.Duration
	var limit int64
	var jsonMode bool

	cmd := &cobra.Command{
		Use:   "tail",
		Short: "Continuously poll a stream and print new entries",
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
			return tailStream(ctx, cmd.OutOrStdout(), client, stream, limit, interval, jsonMode)
		},
	}
	cmd.Flags().StringVarP(&store, "store", "s", "ledger_entries", "store (table/collection) name")
	cmd.Flags().StringVarP(&stream, "stream", "S", "default", "stream ID")
	cmd.Flags().BoolVar(&jsonMode, "json", false, "print full JSON entry instead of plain text payload")
	cmd.Flags().DurationVar(&interval, "interval", 2*time.Second, "polling interval")
	cmd.Flags().Int64Var(&limit, "limit", 50, "entries per poll")
	return cmd
}

// ── rename ────────────────────────────────────────────────────────────────────

func newStreamRenameCmd() *cobra.Command {
	var store, stream, to string

	cmd := &cobra.Command{
		Use:   "rename",
		Short: "Rename a stream (updates name only; entries are unchanged)",
		Long: `Rename a stream by changing its human-readable name.

The stream's entries are not moved or modified. The old name immediately
becomes available for reuse after a successful rename.

Example:
  ledger stream rename --stream notes --to journal`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if to == "" {
				return usageErrorf(cmd, "--to is required")
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
			_, err = client.RenameStream(ctx, &ledgerv1.RenameStreamRequest{
				Name:    stream,
				NewName: to,
			})
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "stream %q renamed to %q\n", stream, to)
			return nil
		},
	}
	cmd.Flags().StringVarP(&store, "store", "s", "ledger_entries", "store (table/collection) name")
	cmd.Flags().StringVarP(&stream, "stream", "S", "default", "current stream name")
	cmd.Flags().StringVar(&to, "to", "", "new stream name (required)")
	return cmd
}

func tailStream(
	ctx context.Context,
	out io.Writer,
	client ledgerv1.LedgerServiceClient,
	streamID string,
	limit int64,
	interval time.Duration,
	jsonMode bool,
) error {
	// Guard against a zero or negative interval which would starve select
	// on the ticker channel.
	if interval <= 0 {
		interval = 2 * time.Second
	}
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
		if err := printEntries(out, resp.Entries, jsonMode); err != nil {
			return err
		}
		if len(resp.Entries) > 0 {
			cursor = resp.Entries[len(resp.Entries)-1].Id
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

// printEntries writes entries to out. With jsonMode each full entry is written
// as JSON (one object per line). Without jsonMode only the decoded payload is
// printed, prefixed by the entry ID, making output easy to read and pipe.
func printEntries(out io.Writer, entries []*ledgerv1.Entry, jsonMode bool) error {
	if jsonMode {
		enc := json.NewEncoder(out)
		for _, e := range entries {
			if err := enc.Encode(e); err != nil {
				return err
			}
		}
		return nil
	}
	for _, e := range entries {
		text := decodePayload(e.Payload)
		if _, err := fmt.Fprintf(out, "%s\t%s\n", e.Id, text); err != nil {
			return err
		}
	}
	return nil
}

// decodePayload converts a raw JSON payload byte slice to a human-readable
// string. JSON strings are unwrapped (quotes removed); other values are
// printed as compact JSON.
func decodePayload(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw)
}

// ── stat ─────────────────────────────────────────────────────────────────────

func newStreamStatCmd() *cobra.Command {
	var store, stream string

	cmd := &cobra.Command{
		Use:   "stat",
		Short: "Get stream metrics (count, range)",
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
			resp, err := client.Stat(ctx, &ledgerv1.StatRequest{Stream: stream})
			if err != nil {
				return err
			}

			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Stream:  %s\n", resp.Stream)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Count:   %d\n", resp.Count)
			if resp.Count > 0 {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "FirstID: %s\n", resp.FirstId)
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "LastID:  %s\n", resp.LastId)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&store, "store", "s", "ledger_entries", "store (table/collection) name")
	cmd.Flags().StringVarP(&stream, "stream", "S", "default", "stream ID")
	return cmd
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
