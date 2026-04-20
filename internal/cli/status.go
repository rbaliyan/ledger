package cli

import (
	"fmt"

	"github.com/rbaliyan/ledger/internal/daemon"
	"github.com/spf13/cobra"
)

func newStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show the status of the ledger daemon",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := clientConfig()
			if err != nil {
				return err
			}

			pid, err := daemon.ReadPID(cfg.PIDFile())
			if err != nil {
				return err
			}
			if pid == 0 || !daemon.IsAlive(pid) {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "ledger daemon is not running")
				return nil
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "ledger daemon is running (pid %d, addr %s)\n", pid, cfg.Listen)
			return nil
		},
	}
}
