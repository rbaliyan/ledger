// Command ledger is the ledger daemon and CLI.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/rbaliyan/ledger/internal/cli"
)

func main() {
	root := cli.Root()
	if err := root.ExecuteContext(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
