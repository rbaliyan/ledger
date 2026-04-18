package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/rbaliyan/ledger/internal/config"
	"github.com/rbaliyan/ledger/internal/daemon"
	"github.com/rbaliyan/ledger/internal/server"
	"github.com/spf13/cobra"
)

func newStartCmd() *cobra.Command {
	var foreground bool

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the ledger daemon",
		Long: `Start the ledger daemon.

Without --foreground the daemon is launched as a background process and
the command returns immediately. Use 'ledger stop' to shut it down.

With --foreground the daemon runs in the current process and logs to
stderr (or log_file if configured). Useful for containers or systemd.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := clientConfig()
			if err != nil {
				return err
			}
			if foreground {
				return runDaemon(cmd.Context(), cfg)
			}
			return startBackground(cmd, cfg)
		},
	}
	cmd.Flags().BoolVar(&foreground, "foreground", false, "run in the foreground (logs to stderr or log_file)")
	return cmd
}

// runDaemon is the actual daemon loop — called when --foreground is set.
func runDaemon(ctx context.Context, cfg *config.Config) error {
	firstRun := false
	if _, err := os.Stat(filepath.Join(cfg.ConfigDir(), "config.yaml")); errors.Is(err, os.ErrNotExist) {
		firstRun = true
	}
	if err := os.MkdirAll(cfg.ConfigDir(), 0o755); err != nil {
		return err
	}
	if firstRun {
		if err := writeDefaultConfig(cfg); err != nil {
			slog.Warn("could not write default config", "err", err)
		} else {
			slog.Info("first run: created default config",
				"config", filepath.Join(cfg.ConfigDir(), "config.yaml"),
				"db", cfg.DB.SQLite.Path,
				"listen", cfg.Listen,
			)
		}
	}

	pidFile := cfg.PIDFile()
	if err := daemon.AcquirePID(pidFile); err != nil {
		if errors.Is(err, os.ErrExist) {
			pid, _ := daemon.ReadPID(pidFile)
			if daemon.IsAlive(pid) {
				return errors.New("ledger daemon is already running")
			}
			_ = daemon.RemovePID(pidFile)
			if err := daemon.AcquirePID(pidFile); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	defer func() {
		if err := daemon.RemovePID(pidFile); err != nil {
			slog.Warn("failed to remove pid file", "path", pidFile, "err", err)
		}
	}()

	if cfg.LogFile != "" {
		f, err := os.OpenFile(cfg.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return err
		}
		defer f.Close()
		slog.SetDefault(slog.New(slog.NewJSONHandler(f, nil)))
	}

	srv, err := server.New(ctx, cfg)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve() }()

	select {
	case <-ctx.Done():
		slog.Info("shutting down ledger daemon")
		srv.Stop(context.Background())
		return nil
	case err := <-errCh:
		return err
	}
}

// startBackground re-executes the binary with 'start --foreground' as a
// detached background process.
func startBackground(cmd *cobra.Command, cfg *config.Config) error {
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

	args := []string{"start", "--foreground"}
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
}

// writeDefaultConfig writes an annotated config.yaml so the user can see and
// customise all available options.
func writeDefaultConfig(cfg *config.Config) error {
	path := filepath.Join(cfg.ConfigDir(), "config.yaml")
	content := fmt.Sprintf(`# Ledger daemon configuration
# Edit this file to customise the daemon, then restart with: ledger stop && ledger start

# Address the gRPC server listens on.
listen: %q

# Optional: write daemon logs to a file instead of stderr.
# log_file: ""

# Optional: protect all RPC calls with a shared API key.
# Clients must supply the key in the x-api-key gRPC metadata header.
# api_key: ""

# TLS configuration (leave blank to use plain-text gRPC).
# tls:
#   cert: "/path/to/server.crt"
#   key:  "/path/to/server.key"
#   ca:   "/path/to/ca.crt"   # enables mutual TLS when set

# Database backend.
db:
  # Backend type: sqlite | postgres | mongodb
  type: sqlite
  sqlite:
    path: %q

  # PostgreSQL — used when type: postgres
  # postgres:
  #   dsn: "postgres://user:pass@localhost:5432/ledger?sslmode=disable"

  # MongoDB — connect via the direct library, not the daemon
  # mongodb:
  #   uri:      "mongodb://localhost:27017"
  #   database: "ledger"
`, cfg.Listen, cfg.DB.SQLite.Path)
	return os.WriteFile(path, []byte(content), 0o600)
}
