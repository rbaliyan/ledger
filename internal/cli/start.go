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
	var readyFD int

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
				return runDaemon(cmd.Context(), cfg, flagConfig, readyFD)
			}
			return startBackground(cmd, cfg)
		},
	}
	cmd.Flags().BoolVar(&foreground, "foreground", false, "run in the foreground (logs to stderr or log_file)")
	cmd.Flags().IntVar(&readyFD, "ready-fd", 0, "")
	_ = cmd.Flags().MarkHidden("ready-fd")
	return cmd
}

// runDaemon is the actual daemon loop — called when --foreground is set.
// readyFD, when non-zero, is a writable file descriptor; a single byte is
// written to it once the server is listening so the parent knows it is ready.
func runDaemon(ctx context.Context, cfg *config.Config, cfgPath string, readyFD int) error {
	// Apply API key from environment (set by startBackground to avoid argv exposure).
	if key := os.Getenv("LEDGER_API_KEY"); key != "" {
		cfg.APIKey = key
		_ = os.Unsetenv("LEDGER_API_KEY") // clear so child processes don't inherit it
	}
	firstRun := false
	if _, err := os.Stat(filepath.Join(cfg.ConfigDir(), "config.yaml")); errors.Is(err, os.ErrNotExist) {
		firstRun = true
	}
	if err := os.MkdirAll(cfg.ConfigDir(), 0o750); err != nil {
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
	if err := acquirePIDSafe(pidFile); err != nil {
		return err
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

	// Signal readiness to the parent process before blocking on Serve.
	if readyFD > 0 {
		pipe := os.NewFile(uintptr(readyFD), "ready")
		_, _ = pipe.Write([]byte{1})
		_ = pipe.Close()
	}

	sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// SIGHUP reloads the configuration and rebuilds hooks without restarting
	// the gRPC listener or dropping the backend connection.
	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	defer signal.Stop(hupCh)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve() }()

	for {
		select {
		case <-sigCtx.Done():
			slog.Info("shutting down ledger daemon")
			srv.Stop(context.Background())
			return nil
		case err := <-errCh:
			return err
		case <-hupCh:
			slog.Info("SIGHUP received: reloading config and hooks")
			newCfg, err := reloadConfig(cfgPath, cfg)
			if err != nil {
				slog.Warn("SIGHUP config reload failed, keeping current hooks", "err", err)
				continue
			}
			cfg = newCfg
			srv.ReloadHooks(cfg)
		}
	}
}

// reloadConfig re-parses the config file from disk. If no explicit path was given
// (i.e. the daemon was started with defaults), the default config file location
// within the config directory is tried. Returns the original cfg on any error so
// the caller can decide whether to continue with the stale config.
func reloadConfig(cfgPath string, current *config.Config) (*config.Config, error) {
	path := cfgPath
	if path == "" {
		path = filepath.Join(current.ConfigDir(), "config.yaml")
	}
	return config.LoadFrom(path)
}

// acquirePIDSafe acquires the PID file, handling the stale-PID race atomically.
// If the file exists and the recorded process is dead, it renames the stale file
// and retries, avoiding the TOCTOU window of read-check-delete-recreate.
func acquirePIDSafe(pidFile string) error {
	err := daemon.AcquirePID(pidFile)
	if err == nil {
		return nil
	}
	if !errors.Is(err, os.ErrExist) {
		return err
	}
	pid, _ := daemon.ReadPID(pidFile)
	if daemon.IsAlive(pid) {
		return errors.New("ledger daemon is already running")
	}
	// Atomically move the stale file aside, then retry O_EXCL creation.
	// Remove the stale file after a successful acquire to avoid leaving debris.
	stale := pidFile + ".stale"
	if renameErr := os.Rename(pidFile, stale); renameErr != nil && !errors.Is(renameErr, os.ErrNotExist) {
		return renameErr
	}
	if err := daemon.AcquirePID(pidFile); err != nil {
		return err
	}
	_ = os.Remove(stale)
	return nil
}

// startBackground re-executes the binary with 'start --foreground' as a
// detached background process. A pipe signals readiness so we do not
// rely on polling the PID file.
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

	// Create a pipe: parent reads, child writes one byte when ready.
	pr, pw, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create ready pipe: %w", err)
	}

	args := []string{"start", "--foreground", "--ready-fd", "3"}
	if flagConfig != "" {
		args = append(args, "--config", flagConfig)
	}
	if flagAddr != "" {
		args = append(args, "--addr", flagAddr)
	}
	// API key is passed via environment, not argv, to avoid exposure in `ps` output.
	env := os.Environ()
	if flagAPIKey != "" {
		env = append(env, "LEDGER_API_KEY="+flagAPIKey)
	}

	proc := exec.Command(self, args...) // #nosec G204 -- self is os.Executable(), not user input
	proc.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	proc.Stdout = nil
	proc.Stderr = nil
	proc.Env = env
	proc.ExtraFiles = []*os.File{pw} // becomes FD 3 in child
	if err := proc.Start(); err != nil {
		_ = pr.Close()
		_ = pw.Close()
		return fmt.Errorf("start daemon: %w", err)
	}
	// Close the parent's write end: if the child exits without writing,
	// Read below returns EOF which we treat as a startup failure.
	_ = pw.Close()
	_ = proc.Process.Release()

	readyTimeout := startTimeout(cfg)
	type result struct{ err error }
	ch := make(chan result, 1)
	go func() {
		buf := make([]byte, 1)
		_, err := pr.Read(buf)
		_ = pr.Close()
		if err != nil {
			ch <- result{fmt.Errorf("daemon failed to start (check %s)", logFileHint(cfg))}
		} else {
			ch <- result{}
		}
	}()

	select {
	case res := <-ch:
		if res.err != nil {
			return res.err
		}
		newPID, _ := daemon.ReadPID(pidFile)
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "ledger daemon started (pid %d)\n", newPID)
		return nil
	case <-time.After(readyTimeout):
		_ = pr.Close()
		return fmt.Errorf("daemon did not report ready within %s (check %s)", readyTimeout, logFileHint(cfg))
	}
}

// startTimeout returns the readiness timeout for background start, using the
// configured value or a sensible per-driver default.
func startTimeout(cfg *config.Config) time.Duration {
	if cfg.StartTimeout != "" {
		if d, err := time.ParseDuration(cfg.StartTimeout); err == nil && d > 0 {
			return d
		}
	}
	switch cfg.DB.Type {
	case "postgres", "mongodb":
		return 15 * time.Second
	case "clickhouse":
		return 20 * time.Second
	default: // sqlite
		return 5 * time.Second
	}
}

func logFileHint(cfg *config.Config) string {
	if cfg.LogFile != "" {
		return cfg.LogFile
	}
	return "stderr (no log_file configured)"
}

// writeDefaultConfig writes an annotated config.yaml so the user can see and
// customise all available options.
func writeDefaultConfig(cfg *config.Config) error {
	path := filepath.Join(cfg.ConfigDir(), "config.yaml")
	content := fmt.Sprintf(`# Ledger daemon configuration
# Edit this file to customise the daemon, then reload with: kill -HUP <pid>
# or restart with: ledger stop && ledger start

# Address the gRPC server listens on.
listen: %q

# Optional: write daemon logs to a file instead of stderr.
# log_file: ""

# Optional: protect all RPC calls with a shared API key.
# Clients must supply the key in the x-api-key gRPC metadata header.
# api_key: ""

# Optional: restrict the API key to specific store names.
# allowed_stores: ["orders", "users"]

# Optional: override the background-start readiness timeout (default is
# driver-specific: sqlite=5s, postgres/mongodb=15s, clickhouse=20s).
# start_timeout: "10s"

# TLS configuration (leave blank to use plain-text gRPC).
# tls:
#   cert: "/path/to/server.crt"
#   key:  "/path/to/server.key"
#   ca:   "/path/to/ca.crt"   # enables mutual TLS when set

# Database backend.
db:
  # Backend type: sqlite | postgres | mongodb | clickhouse
  type: sqlite
  sqlite:
    path: %q

  # PostgreSQL — used when type: postgres
  # postgres:
  #   dsn: "postgres://user:pass@localhost:5432/ledger?sslmode=disable"

  # MongoDB — used when type: mongodb
  # mongodb:
  #   uri:      "mongodb://localhost:27017"
  #   database: "ledger"

  # ClickHouse — used when type: clickhouse
  # clickhouse:
  #   dsn: "tcp://localhost:9000?database=ledger"

# Optional: webhook event hooks that deliver entries to HTTP endpoints.
# Hooks poll the configured store and POST new entries as they appear.
# Note: cursors are in-memory; re-delivery occurs on daemon restart.
# hooks:
#   - name: audit
#     store: orders
#     url: "https://example.com/webhooks/ledger"
#     secret: ""               # signs each POST with X-Ledger-Signature (HMAC-SHA256)
#     max_retries: 5
#     interval: "5s"
#     # ca: "/path/to/ca.crt"
#     # insecure_skip_verify: false
`, cfg.Listen, cfg.DB.SQLite.Path)
	return os.WriteFile(path, []byte(content), 0o600)
}
