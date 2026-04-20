package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Populated via -ldflags at build time.
var (
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, _ []string) {
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "ledger %s (commit %s, built %s)\n", version, commit, buildDate)
		},
	}
}
