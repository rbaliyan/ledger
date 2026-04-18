package cli

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/rbaliyan/ledger/internal/daemon"
	"github.com/spf13/cobra"
)

func newStartCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Start the ledger daemon in the background",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := clientConfig()
			if err != nil {
				return err
			}

			pidFile := cfg.PIDFile()
			pid, err := daemon.ReadPID(pidFile)
			if err != nil {
				return err
			}
			if daemon.IsAlive(pid) {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "ledger daemon already running (pid %d)\n", pid)
				return nil
			}

			self, err := os.Executable()
			if err != nil {
				return fmt.Errorf("cannot find executable: %w", err)
			}

			args := []string{"serve", "--foreground"}
			if flagConfig != "" {
				args = append(args, "--config", flagConfig)
			}
			if flagAPIKey != "" {
				args = append(args, "--api-key", flagAPIKey)
			}
			if flagAddr != "" {
				args = append(args, "--addr", flagAddr)
			}

			proc := exec.Command(self, args...)
			proc.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
			proc.Stdout = nil
			proc.Stderr = nil
			if err := proc.Start(); err != nil {
				return fmt.Errorf("start daemon: %w", err)
			}

			// Wait briefly for the PID file to appear.
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				newPID, err := daemon.ReadPID(pidFile)
				if err == nil && newPID > 0 && daemon.IsAlive(newPID) {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "ledger daemon started (pid %d)\n", newPID)
					return nil
				}
				time.Sleep(100 * time.Millisecond)
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "ledger daemon started (pid file not yet written)")
			return nil
		},
	}
}
