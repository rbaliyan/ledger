package cli

import (
	"fmt"
	"os"
	"syscall"

	"github.com/rbaliyan/ledger/internal/daemon"
	"github.com/spf13/cobra"
)

func newStopCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stop",
		Short: "Stop the running ledger daemon",
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

			proc, err := os.FindProcess(pid)
			if err != nil {
				return fmt.Errorf("find process %d: %w", pid, err)
			}
			if err := proc.Signal(syscall.SIGTERM); err != nil {
				return fmt.Errorf("signal %d: %w", pid, err)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "sent SIGTERM to ledger daemon (pid %d)\n", pid)
			return nil
		},
	}
}
