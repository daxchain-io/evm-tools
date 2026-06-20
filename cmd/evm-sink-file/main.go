// Command evm-sink-file is a sink CLI that reads the suite's JSONL record
// contract on stdin and appends each record to a rotating local file with
// at-least-once durability (line-atomic append, optional fsync per line,
// confirm-before-advance, blocking backoff on a transient full disk, fail-fast on
// a permanent filesystem fault). The active file rotates by size and/or age with
// optional gzip compression and retention; optional filters narrow which records
// are written by record type and name. See docs/design.md.
//
// This is a thin entrypoint over internal/cli, which builds the sink command tree
// (run, validate, version).
package main

import (
	"fmt"
	"os"

	"github.com/daxchain-io/evm-tools/internal/cli"
)

func main() {
	root := cli.NewSinkRootCommand(cli.ToolSinkFile)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
