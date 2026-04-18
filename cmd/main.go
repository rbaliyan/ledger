// Command ledger is the ledger daemon and CLI.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/rbaliyan/ledger/internal/cli"
)

func main() {
	root := cli.Root()
	if err := root.ExecuteContext(context.Background()); err != nil {
		// UsageErrors are already printed with usage by the command itself.
		if !errors.As(err, &cli.UsageError{}) {
			fmt.Fprintln(os.Stderr, "Error:", err)
		}
		os.Exit(1)
	}
}
