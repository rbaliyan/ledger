package cli

import (
	"github.com/spf13/cobra"
)

var (
	flagConfig string
	flagAPIKey string
	flagAddr   string
)

// Root returns the top-level cobra command.
func Root() *cobra.Command {
	root := &cobra.Command{
		Use:   "ledger",
		Short: "Ledger daemon and stream management CLI",
		Long: `ledger is an append-only log daemon with a gRPC API.

Use 'ledger start' to run the daemon in the background,
'ledger start --foreground' to run it in the foreground,
'ledger stop' to shut it down, and 'ledger status' to check if it is running.
Stream subcommands let you append, read, tag, annotate, trim, and tail streams.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.PersistentFlags().StringVarP(&flagConfig, "config", "c", "", "config file path (default: ~/.ledger/config.yaml)")
	root.PersistentFlags().StringVar(&flagAPIKey, "api-key", "", "API key for authentication (overrides config)")
	root.PersistentFlags().StringVar(&flagAddr, "addr", "", "daemon address (overrides config listen address)")

	root.AddCommand(
		newStartCmd(),
		newStopCmd(),
		newStatusCmd(),
		newStreamCmd(),
	)

	return root
}
