package daemon

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// AcquirePID atomically creates the PID file and writes the current PID.
// Returns an error (wrapping os.ErrExist) if the file already exists so
// callers can detect a concurrent daemon start without a TOCTOU race.
func AcquirePID(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) // #nosec G304 -- path is the daemon's own PID file
	if err != nil {
		return fmt.Errorf("ledger: acquire pid file %s: %w", path, err)
	}
	defer f.Close()
	if _, err := fmt.Fprintf(f, "%d", os.Getpid()); err != nil {
		return fmt.Errorf("ledger: write pid file %s: %w", path, err)
	}
	return nil
}

// ReadPID reads and parses the PID from path. Returns 0, nil when the file
// does not exist (daemon not running).
func ReadPID(path string) (int, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path is the daemon's own PID file
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("ledger: read pid file %s: %w", path, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("ledger: invalid pid file %s: %w", path, err)
	}
	return pid, nil
}

// RemovePID deletes the PID file. Returns nil if the file does not exist.
func RemovePID(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("ledger: remove pid file %s: %w", path, err)
	}
	return nil
}

// IsAlive returns true if a process with the given PID exists and is running.
func IsAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}
