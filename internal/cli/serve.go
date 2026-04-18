package cli

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/rbaliyan/ledger/internal/config"
	"github.com/rbaliyan/ledger/internal/daemon"
	"github.com/rbaliyan/ledger/internal/server"
	"github.com/spf13/cobra"
)

func newServeCmd() *cobra.Command {
	var foreground bool

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the ledger daemon in the foreground",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := clientConfig()
			if err != nil {
				return err
			}
			return runServe(cmd.Context(), cfg, foreground)
		},
	}
	cmd.Flags().BoolVar(&foreground, "foreground", false, "keep logs on stderr instead of the configured log_file")
	return cmd
}

func runServe(ctx context.Context, cfg *config.Config, foreground bool) error {
	if err := os.MkdirAll(cfg.ConfigDir(), 0o755); err != nil {
		return err
	}

	pidFile := cfg.PIDFile()
	if err := daemon.AcquirePID(pidFile); err != nil {
		if errors.Is(err, os.ErrExist) {
			// Stale PID file — check if the process is still alive.
			pid, _ := daemon.ReadPID(pidFile)
			if daemon.IsAlive(pid) {
				return errors.New("ledger daemon is already running")
			}
			// Stale file: remove and retry.
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

	if !foreground && cfg.LogFile != "" {
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
