package cli_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rbaliyan/ledger/internal/cli"
)

// TestSmokeRootCommandTree verifies the command tree is wired up: the root
// command builds and registers the expected subcommands. This needs no daemon
// and no network.
func TestSmokeRootCommandTree(t *testing.T) {
	root := cli.Root()

	want := map[string]bool{
		"stream": false,
		"start":  false,
		"stop":   false,
		"status": false,
	}
	for _, c := range root.Commands() {
		if _, ok := want[c.Name()]; ok {
			want[c.Name()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("subcommand %q not registered", name)
		}
	}
}

// TestSmokeRootHelp verifies --help executes cleanly in-process without starting
// a server or touching the network.
func TestSmokeRootHelp(t *testing.T) {
	root := cli.Root()
	var out, errBuf bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errBuf)
	root.SetArgs([]string{"--help"})

	if err := root.Execute(); err != nil {
		t.Fatalf("--help: %v", err)
	}
	if !strings.Contains(out.String(), "ledger") {
		t.Errorf("help output missing 'ledger':\n%s", out.String())
	}
}

// TestSmokeStreamAppendInvalidJSON exercises a pure validation path: with --json
// set, an invalid JSON payload must be rejected before any daemon connection is
// attempted.
func TestSmokeStreamAppendInvalidJSON(t *testing.T) {
	root := cli.Root()
	var out, errBuf bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errBuf)
	root.SetArgs([]string{"stream", "append", "--json", "{not valid json"})

	err := root.Execute()
	if err == nil {
		t.Fatal("expected validation error for invalid --json payload, got nil")
	}
	if !strings.Contains(err.Error(), "valid JSON") {
		t.Errorf("expected JSON validation error, got: %v", err)
	}
}
