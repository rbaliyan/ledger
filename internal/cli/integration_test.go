//go:build integration

package cli_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var ledgerBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "ledger-bin-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "mktemp: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)

	ledgerBin = filepath.Join(dir, "ledger")
	wd, _ := os.Getwd()
	build := exec.Command("go", "build", "-o", ledgerBin, "./cmd")
	build.Dir = filepath.Join(wd, "..", "..")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build: %v\n%s\n", err, out)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// harness manages a temporary ledger config and optional daemon process.
type harness struct {
	t       *testing.T
	cfgPath string
	addr    string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := t.TempDir()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	cfgPath := filepath.Join(dir, "config.yaml")
	content := fmt.Sprintf("listen: %q\ndb:\n  type: sqlite\n  sqlite:\n    path: %q\n",
		addr, filepath.Join(dir, "test.db"))
	if err := os.WriteFile(cfgPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return &harness{t: t, cfgPath: cfgPath, addr: addr}
}

// startDaemon starts the daemon in --foreground mode and waits until the TCP
// port accepts connections. The process is killed when the test's context is
// cancelled (i.e. when the test ends).
func (h *harness) startDaemon() {
	h.t.Helper()
	cmd := exec.CommandContext(h.t.Context(), ledgerBin,
		"--config", h.cfgPath, "start", "--foreground")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		h.t.Fatalf("start daemon: %v", err)
	}
	h.t.Cleanup(func() { _ = cmd.Wait() })

	deadline := time.Now().Add(10 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", h.addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("daemon not ready within 10s at %s", h.addr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// run executes the ledger binary with --config prepended. Returns stdout,
// stderr, and exit code.
func (h *harness) run(args ...string) (stdout, stderr string, code int) {
	h.t.Helper()
	cmd := exec.Command(ledgerBin, append([]string{"--config", h.cfgPath}, args...)...)
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		var e *exec.ExitError
		if errors.As(err, &e) {
			code = e.ExitCode()
		} else {
			code = 1
		}
	}
	return outBuf.String(), errBuf.String(), code
}

func (h *harness) mustRun(args ...string) string {
	h.t.Helper()
	out, errOut, code := h.run(args...)
	if code != 0 {
		h.t.Fatalf("ledger %s: exit %d\nstdout: %s\nstderr: %s",
			strings.Join(args, " "), code, out, errOut)
	}
	return out
}

// mustFail asserts the command exits non-zero and returns combined output.
func (h *harness) mustFail(args ...string) string {
	h.t.Helper()
	out, errOut, code := h.run(args...)
	if code == 0 {
		h.t.Fatalf("ledger %s: expected failure but succeeded\nstdout: %s\nstderr: %s",
			strings.Join(args, " "), out, errOut)
	}
	return out + errOut
}

func assertContains(t *testing.T, output, want string) {
	t.Helper()
	if !strings.Contains(output, want) {
		t.Errorf("expected %q in output, got:\n%s", want, output)
	}
}

// ── stream tests ─────────────────────────────────────────────────────────────

func TestCLIStream(t *testing.T) {
	h := newHarness(t)
	h.startDaemon()

	t.Run("AppendPlainText", func(t *testing.T) {
		out := h.mustRun("stream", "append", "--stream", "txt", "hello world")
		id := strings.TrimSpace(out)
		if id == "" {
			t.Fatal("expected entry ID in output")
		}
	})

	t.Run("AppendJSON", func(t *testing.T) {
		out := h.mustRun("stream", "append", "--stream", "json-s",
			"--json", `{"event":"login","user":"alice"}`)
		if strings.TrimSpace(out) == "" {
			t.Fatal("expected entry ID")
		}
	})

	t.Run("AppendInvalidJSON", func(t *testing.T) {
		out := h.mustFail("stream", "append", "--stream", "json-s",
			"--json", "not-json")
		assertContains(t, out, "not valid JSON")
	})

	t.Run("AppendWithAllFlags", func(t *testing.T) {
		out := h.mustRun("stream", "append",
			"--stream", "flagged",
			"--order-key", "ord1",
			"--dedup-key", "dup1",
			"--meta", "env=test",
			"--meta", "region=us",
			"--tag", "important",
			"--tag", "urgent",
			"entry payload",
		)
		if strings.TrimSpace(out) == "" {
			t.Fatal("expected entry ID")
		}
	})

	t.Run("AppendDedup", func(t *testing.T) {
		h.mustRun("stream", "append", "--stream", "dedup-s", "--dedup-key", "k1", "payload")
		h.mustRun("stream", "append", "--stream", "dedup-s", "--dedup-key", "k1", "payload")
		out := h.mustRun("stream", "count", "--stream", "dedup-s")
		if strings.TrimSpace(out) != "1" {
			t.Errorf("dedup: expected count=1, got %q", strings.TrimSpace(out))
		}
	})

	t.Run("AppendInvalidMeta", func(t *testing.T) {
		out := h.mustFail("stream", "append", "--stream", "s",
			"--meta", "bad-no-equals", "payload")
		assertContains(t, out, "key=value")
	})

	t.Run("Read", func(t *testing.T) {
		h.mustRun("stream", "append", "--stream", "read-s", "alpha")
		h.mustRun("stream", "append", "--stream", "read-s", "beta")
		h.mustRun("stream", "append", "--stream", "read-s", "gamma")

		out := h.mustRun("stream", "read", "--stream", "read-s")
		assertContains(t, out, "alpha")
		assertContains(t, out, "beta")
		assertContains(t, out, "gamma")
	})

	t.Run("ReadDesc", func(t *testing.T) {
		h.mustRun("stream", "append", "--stream", "read-desc", "first")
		h.mustRun("stream", "append", "--stream", "read-desc", "second")

		out := h.mustRun("stream", "read", "--stream", "read-desc", "--desc")
		lines := strings.Split(strings.TrimSpace(out), "\n")
		if len(lines) < 2 {
			t.Fatalf("expected 2 lines, got %d: %s", len(lines), out)
		}
		if !strings.Contains(lines[0], "second") {
			t.Errorf("desc: expected 'second' first, got: %s", lines[0])
		}
	})

	t.Run("ReadAfterCursor", func(t *testing.T) {
		h.mustRun("stream", "append", "--stream", "cursor-s", "e1")
		id2raw := h.mustRun("stream", "append", "--stream", "cursor-s", "e2")
		id2 := strings.TrimSpace(id2raw)
		h.mustRun("stream", "append", "--stream", "cursor-s", "e3")

		out := h.mustRun("stream", "read", "--stream", "cursor-s", "--after", id2)
		if strings.Contains(out, "e1") || strings.Contains(out, "e2") {
			t.Errorf("after cursor: unexpected early entries in output: %s", out)
		}
		assertContains(t, out, "e3")
	})

	t.Run("ReadLimit", func(t *testing.T) {
		for _, v := range []string{"x1", "x2", "x3", "x4", "x5"} {
			h.mustRun("stream", "append", "--stream", "limit-s", v)
		}
		out := h.mustRun("stream", "read", "--stream", "limit-s", "--limit", "2")
		lines := strings.Split(strings.TrimSpace(out), "\n")
		if len(lines) != 2 {
			t.Errorf("limit=2: expected 2 lines, got %d", len(lines))
		}
	})

	t.Run("ReadTagFilter", func(t *testing.T) {
		h.mustRun("stream", "append", "--stream", "tag-filter-s",
			"--tag", "red", "entry-red")
		h.mustRun("stream", "append", "--stream", "tag-filter-s",
			"--tag", "blue", "entry-blue")

		out := h.mustRun("stream", "read", "--stream", "tag-filter-s", "--tag", "red")
		assertContains(t, out, "entry-red")
		if strings.Contains(out, "entry-blue") {
			t.Errorf("tag filter: unexpected blue entry in output: %s", out)
		}
	})

	t.Run("ReadJSON", func(t *testing.T) {
		h.mustRun("stream", "append", "--stream", "json-read", "json-payload")
		out := h.mustRun("stream", "read", "--stream", "json-read", "--json")
		if !strings.HasPrefix(strings.TrimSpace(out), "{") {
			t.Errorf("--json: expected JSON output, got: %s", out)
		}
	})

	t.Run("ReadEmptyStream", func(t *testing.T) {
		out := h.mustRun("stream", "read", "--stream", "nonexistent-stream-xyz")
		if strings.TrimSpace(out) != "" {
			t.Errorf("expected empty output for unknown stream, got: %s", out)
		}
	})

	t.Run("Search", func(t *testing.T) {
		h.mustRun("stream", "append", "--stream", "search-s", "unique-needle-7q3r")
		h.mustRun("stream", "append", "--stream", "search-s", "haystack only")

		out := h.mustRun("stream", "search", "--stream", "search-s", "unique-needle-7q3r")
		assertContains(t, out, "unique-needle-7q3r")
		if strings.Contains(out, "haystack") {
			t.Errorf("search: unexpected haystack entry: %s", out)
		}
	})

	t.Run("SearchAllStreams", func(t *testing.T) {
		h.mustRun("stream", "append", "--stream", "sa1", "global-marker-5x9")
		h.mustRun("stream", "append", "--stream", "sa2", "global-marker-5x9")

		out := h.mustRun("stream", "search", "global-marker-5x9")
		lines := strings.Split(strings.TrimSpace(out), "\n")
		if len(lines) < 2 {
			t.Errorf("cross-stream search: expected >=2 results, got %d: %s", len(lines), out)
		}
	})

	t.Run("SearchNoResults", func(t *testing.T) {
		out := h.mustRun("stream", "search", "zzz-no-match-qqqq-impossible-string")
		if strings.TrimSpace(out) != "" {
			t.Errorf("expected empty output for no-match search, got: %s", out)
		}
	})

	t.Run("SearchEmptyQuery", func(t *testing.T) {
		out := h.mustFail("stream", "search", "")
		assertContains(t, out, "query")
	})

	t.Run("SearchJSON", func(t *testing.T) {
		h.mustRun("stream", "append", "--stream", "json-search", "json-needle-abc")
		out := h.mustRun("stream", "search", "--stream", "json-search",
			"--json", "json-needle-abc")
		if !strings.HasPrefix(strings.TrimSpace(out), "{") {
			t.Errorf("search --json: expected JSON output, got: %s", out)
		}
	})

	t.Run("SearchDesc", func(t *testing.T) {
		h.mustRun("stream", "append", "--stream", "search-desc", "desc-a")
		h.mustRun("stream", "append", "--stream", "search-desc", "desc-b")
		out := h.mustRun("stream", "search", "--stream", "search-desc",
			"--desc", "desc-")
		lines := strings.Split(strings.TrimSpace(out), "\n")
		if len(lines) < 2 {
			t.Fatalf("expected 2 lines, got %d: %s", len(lines), out)
		}
		if !strings.Contains(lines[0], "desc-b") {
			t.Errorf("search --desc: expected desc-b first, got: %s", lines[0])
		}
	})

	t.Run("Count", func(t *testing.T) {
		out := h.mustRun("stream", "count", "--stream", "count-empty")
		if strings.TrimSpace(out) != "0" {
			t.Errorf("empty count: expected 0, got %q", strings.TrimSpace(out))
		}

		for range 4 {
			h.mustRun("stream", "append", "--stream", "count-s", "e")
		}
		out = h.mustRun("stream", "count", "--stream", "count-s")
		if strings.TrimSpace(out) != "4" {
			t.Errorf("count: expected 4, got %q", strings.TrimSpace(out))
		}
	})

	t.Run("List", func(t *testing.T) {
		h.mustRun("stream", "append", "--stream", "list-s1", "e")
		h.mustRun("stream", "append", "--stream", "list-s2", "e")

		out := h.mustRun("stream", "list")
		assertContains(t, out, "list-s1")
		assertContains(t, out, "list-s2")
	})

	t.Run("ListAfterCursor", func(t *testing.T) {
		h.mustRun("stream", "append", "--stream", "cur-a", "e")
		h.mustRun("stream", "append", "--stream", "cur-b", "e")
		h.mustRun("stream", "append", "--stream", "cur-c", "e")

		out := h.mustRun("stream", "list", "--after", "cur-b")
		assertContains(t, out, "cur-c")
		if strings.Contains(out, "cur-a") {
			t.Errorf("list --after: unexpected cur-a: %s", out)
		}
	})

	t.Run("ListLimit", func(t *testing.T) {
		for _, s := range []string{"lim-1", "lim-2", "lim-3"} {
			h.mustRun("stream", "append", "--stream", s, "e")
		}
		out := h.mustRun("stream", "list", "--limit", "1")
		lines := strings.Split(strings.TrimSpace(out), "\n")
		if len(lines) != 1 {
			t.Errorf("list --limit 1: expected 1 line, got %d: %s", len(lines), out)
		}
	})

	t.Run("Stat", func(t *testing.T) {
		out := h.mustRun("stream", "stat", "--stream", "stat-empty")
		assertContains(t, out, "Count:   0")

		h.mustRun("stream", "append", "--stream", "stat-s", "first")
		h.mustRun("stream", "append", "--stream", "stat-s", "last")
		out = h.mustRun("stream", "stat", "--stream", "stat-s")
		assertContains(t, out, "Count:   2")
		assertContains(t, out, "FirstID:")
		assertContains(t, out, "LastID:")
	})

	t.Run("Tag", func(t *testing.T) {
		idRaw := h.mustRun("stream", "append", "--stream", "tag-s", "tagme")
		id := strings.TrimSpace(idRaw)

		h.mustRun("stream", "tag", "--stream", "tag-s", "--id", id,
			"--tag", "alpha", "--tag", "beta")

		out := h.mustRun("stream", "read", "--stream", "tag-s", "--json")
		assertContains(t, out, "alpha")
		assertContains(t, out, "beta")

		// Replace tags with a single new tag.
		h.mustRun("stream", "tag", "--stream", "tag-s", "--id", id, "--tag", "gamma")
		out = h.mustRun("stream", "read", "--stream", "tag-s", "--json")
		assertContains(t, out, "gamma")
		if strings.Contains(out, "alpha") {
			t.Errorf("tag replace: unexpected 'alpha' still present: %s", out)
		}

		// Clear all tags.
		h.mustRun("stream", "tag", "--stream", "tag-s", "--id", id)
	})

	t.Run("TagMissingID", func(t *testing.T) {
		out := h.mustFail("stream", "tag", "--stream", "tag-s")
		assertContains(t, out, "--id is required")
	})

	t.Run("Annotate", func(t *testing.T) {
		idRaw := h.mustRun("stream", "append", "--stream", "ann-s", "annotate-me")
		id := strings.TrimSpace(idRaw)

		h.mustRun("stream", "annotate", "--stream", "ann-s", "--id", id,
			"--set", "env=prod", "--set", "region=us")

		out := h.mustRun("stream", "read", "--stream", "ann-s", "--json")
		assertContains(t, out, "prod")
		assertContains(t, out, "region")

		// Delete annotation with key= form.
		h.mustRun("stream", "annotate", "--stream", "ann-s", "--id", id,
			"--set", "env=")
		out = h.mustRun("stream", "read", "--stream", "ann-s", "--json")
		if strings.Contains(out, `"env"`) {
			t.Errorf("annotate delete: env key still present: %s", out)
		}
	})

	t.Run("AnnotateMissingID", func(t *testing.T) {
		out := h.mustFail("stream", "annotate", "--stream", "ann-s",
			"--set", "k=v")
		assertContains(t, out, "--id is required")
	})

	t.Run("AnnotateNoPairs", func(t *testing.T) {
		idRaw := h.mustRun("stream", "append", "--stream", "ann2-s", "e")
		id := strings.TrimSpace(idRaw)
		out := h.mustFail("stream", "annotate", "--stream", "ann2-s", "--id", id)
		assertContains(t, out, "required")
	})

	t.Run("Trim", func(t *testing.T) {
		h.mustRun("stream", "append", "--stream", "trim-s", "t1")
		id2raw := h.mustRun("stream", "append", "--stream", "trim-s", "t2")
		id2 := strings.TrimSpace(id2raw)
		h.mustRun("stream", "append", "--stream", "trim-s", "t3")

		out := h.mustRun("stream", "trim", "--stream", "trim-s", "--before", id2)
		assertContains(t, out, "trimmed")

		remaining := h.mustRun("stream", "read", "--stream", "trim-s")
		if strings.Contains(remaining, "t1") {
			t.Errorf("trim: t1 still present after trim: %s", remaining)
		}
		assertContains(t, remaining, "t3")
	})

	t.Run("TrimMissingBefore", func(t *testing.T) {
		out := h.mustFail("stream", "trim", "--stream", "trim-s")
		assertContains(t, out, "--before is required")
	})

	t.Run("TrimNonexistentStream", func(t *testing.T) {
		// Trim on a stream that was never written returns "trimmed 0".
		out := h.mustRun("stream", "trim", "--stream", "nope-xyz", "--before", "999")
		assertContains(t, out, "trimmed 0")
	})

	t.Run("Rename", func(t *testing.T) {
		h.mustRun("stream", "append", "--stream", "rename-old", "payload-for-rename")

		out := h.mustRun("stream", "rename", "--stream", "rename-old", "--to", "rename-new")
		assertContains(t, out, "rename-old")
		assertContains(t, out, "rename-new")

		// Entries accessible under the new name.
		out = h.mustRun("stream", "read", "--stream", "rename-new")
		assertContains(t, out, "payload-for-rename")

		// Old name returns nothing.
		out = h.mustRun("stream", "read", "--stream", "rename-old")
		if strings.TrimSpace(out) != "" {
			t.Errorf("rename: old name still has entries: %s", out)
		}
	})

	t.Run("RenameConflict", func(t *testing.T) {
		h.mustRun("stream", "append", "--stream", "conflict-a", "e")
		h.mustRun("stream", "append", "--stream", "conflict-b", "e")

		out := h.mustFail("stream", "rename", "--stream", "conflict-a", "--to", "conflict-b")
		assertContains(t, out, "already exists")
	})

	t.Run("RenameMissingTo", func(t *testing.T) {
		out := h.mustFail("stream", "rename", "--stream", "rename-new")
		assertContains(t, out, "--to is required")
	})

	t.Run("Tail", func(t *testing.T) {
		h.mustRun("stream", "append", "--stream", "tail-s", "tail-entry-abc")

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		cmd := exec.CommandContext(ctx, ledgerBin, "--config", h.cfgPath,
			"stream", "tail", "--stream", "tail-s", "--interval", "100ms", "--limit", "10")
		var outBuf bytes.Buffer
		cmd.Stdout = &outBuf
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("tail start: %v", err)
		}

		time.Sleep(600 * time.Millisecond)
		cancel()
		_ = cmd.Wait()

		assertContains(t, outBuf.String(), "tail-entry-abc")
	})
}

// ── lifecycle tests ───────────────────────────────────────────────────────────

func TestCLILifecycle(t *testing.T) {
	h := newHarness(t)

	// Status before any daemon is started.
	out := h.mustRun("status")
	assertContains(t, out, "not running")

	// Start the daemon in the background (uses readiness pipe internally).
	out = h.mustRun("start")
	assertContains(t, out, "started")

	t.Cleanup(func() {
		// Ensure the daemon is stopped even if the test fails mid-way.
		h.run("stop") //nolint:errcheck
		// Give it a moment to shut down.
		time.Sleep(300 * time.Millisecond)
	})

	// Wait for TCP port to be accepting connections.
	deadline := time.Now().Add(10 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", h.addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("background daemon not ready within 10s at %s", h.addr)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Status while running.
	out = h.mustRun("status")
	assertContains(t, out, "running")

	// Starting again when already running prints "already running".
	out = h.mustRun("start")
	assertContains(t, out, "already running")

	// Stop the daemon.
	out = h.mustRun("stop")
	assertContains(t, out, "SIGTERM")

	// Poll until the port stops accepting connections.
	deadline = time.Now().Add(10 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", h.addr, 100*time.Millisecond)
		if err != nil {
			break
		}
		_ = conn.Close()
		if time.Now().After(deadline) {
			t.Fatalf("daemon still listening after stop")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Status after stop.
	out = h.mustRun("status")
	assertContains(t, out, "not running")

	// Stop when not running is a no-op.
	out = h.mustRun("stop")
	assertContains(t, out, "not running")
}

// ── commands without a running daemon ────────────────────────────────────────

func TestCLINoDaemon(t *testing.T) {
	h := newHarness(t) // daemon is NOT started

	cases := []struct {
		name string
		args []string
	}{
		{"list", []string{"stream", "list"}},
		{"append", []string{"stream", "append", "payload"}},
		{"read", []string{"stream", "read"}},
		{"search", []string{"stream", "search", "query"}},
		{"count", []string{"stream", "count"}},
		{"stat", []string{"stream", "stat"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := h.mustFail(tc.args...)
			// gRPC connection failure surfaces as Unavailable or connection refused.
			if !strings.Contains(out, "Unavailable") &&
				!strings.Contains(out, "refused") &&
				!strings.Contains(out, "unavailable") {
				t.Errorf("expected connection error, got: %s", out)
			}
		})
	}
}
