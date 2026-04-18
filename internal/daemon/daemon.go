package daemon

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// WritePID writes the current process PID to path, creating the file if needed.
func WritePID(path string) error {
	data := strconv.AppendInt(nil, int64(os.Getpid()), 10)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("ledger: write pid file %s: %w", path, err)
	}
	return nil
}

// ReadPID reads and parses the PID from path.
func ReadPID(path string) (int, error) {
	data, err := os.ReadFile(path)
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
