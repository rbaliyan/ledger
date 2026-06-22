package daemon

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestPIDLifecycle(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "ledger.pid")

	// Acquire writes the current PID.
	if err := AcquirePID(path); err != nil {
		t.Fatalf("AcquirePID: %v", err)
	}

	// ReadPID returns the current process PID.
	pid, err := ReadPID(path)
	if err != nil {
		t.Fatalf("ReadPID: %v", err)
	}
	if pid != os.Getpid() {
		t.Errorf("ReadPID = %d, want %d", pid, os.Getpid())
	}

	// Self is alive.
	if !IsAlive(os.Getpid()) {
		t.Error("IsAlive(self) = false, want true")
	}

	// Remove deletes the file.
	if err := RemovePID(path); err != nil {
		t.Fatalf("RemovePID: %v", err)
	}

	// ReadPID after removal reports not running (0, nil).
	pid, err = ReadPID(path)
	if err != nil {
		t.Fatalf("ReadPID after remove: %v", err)
	}
	if pid != 0 {
		t.Errorf("ReadPID after remove = %d, want 0", pid)
	}

	// Reacquire succeeds once the file is gone.
	if err := AcquirePID(path); err != nil {
		t.Fatalf("re-AcquirePID after remove: %v", err)
	}
}

func TestAcquirePID_ExistingFileFails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "ledger.pid")

	if err := AcquirePID(path); err != nil {
		t.Fatalf("first AcquirePID: %v", err)
	}
	err := AcquirePID(path)
	if err == nil {
		t.Fatal("second AcquirePID on existing file = nil, want error")
	}
	// The error wraps os.ErrExist so callers can detect a concurrent daemon.
	if !errors.Is(err, os.ErrExist) {
		t.Errorf("AcquirePID error = %v, want wrapped os.ErrExist", err)
	}
}

func TestIsAlive(t *testing.T) {
	t.Parallel()
	if IsAlive(os.Getpid()) != true {
		t.Error("IsAlive(self) = false, want true")
	}
	if IsAlive(0) {
		t.Error("IsAlive(0) = true, want false")
	}
	if IsAlive(-1) {
		t.Error("IsAlive(-1) = true, want false")
	}
	// A PID that is almost certainly not a live process. IsAlive must report false.
	if IsAlive(deadPID(t)) {
		t.Error("IsAlive(dead pid) = true, want false")
	}
}

func TestReadPID_StaleLockReclaim(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "ledger.pid")

	dead := deadPID(t)
	if err := os.WriteFile(path, []byte("  "+strconv.Itoa(dead)+"\n"), 0o600); err != nil {
		t.Fatalf("write stale pid file: %v", err)
	}

	// ReadPID parses the stale PID (whitespace tolerant).
	pid, err := ReadPID(path)
	if err != nil {
		t.Fatalf("ReadPID stale: %v", err)
	}
	if pid != dead {
		t.Errorf("ReadPID stale = %d, want %d", pid, dead)
	}

	// The stale lock holder is not alive, so the caller may reclaim by removing
	// and reacquiring the PID file.
	if IsAlive(pid) {
		t.Fatalf("stale pid %d unexpectedly alive", pid)
	}
	if err := RemovePID(path); err != nil {
		t.Fatalf("RemovePID stale: %v", err)
	}
	if err := AcquirePID(path); err != nil {
		t.Fatalf("AcquirePID after reclaiming stale lock: %v", err)
	}
}

func TestReadPID_MalformedContent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "ledger.pid")
	if err := os.WriteFile(path, []byte("not-a-number"), 0o600); err != nil {
		t.Fatalf("write malformed pid file: %v", err)
	}
	if _, err := ReadPID(path); err == nil {
		t.Fatal("ReadPID on malformed content = nil error, want error")
	}
}

func TestRemovePID_Missing(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "does-not-exist.pid")
	if err := RemovePID(path); err != nil {
		t.Errorf("RemovePID on missing file = %v, want nil", err)
	}
}

// deadPID finds a PID that is not currently a live process. It probes a handful
// of high PID values and returns the first that is not alive.
func deadPID(t *testing.T) int {
	t.Helper()
	for _, candidate := range []int{999999, 888888, 777777, 666666} {
		if !IsAlive(candidate) {
			return candidate
		}
	}
	t.Skip("could not find a definitely-dead PID on this system")
	return 0
}
