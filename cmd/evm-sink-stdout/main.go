// Command evm-sink-stdout is a sink CLI that reads the suite's JSONL record
// contract from its input (a "unix:" socket, or stdin) and writes each record's
// verbatim line to stdout. It is the one sink whose delivery target IS stdout —
// the composability hatch that restores `… | jq` and piping into other tools
// (`evm-sink-stdout run --input unix:/run/evm/events.sock | jq`). Because it owns
// stdout for data, its own diagnostics are routed entirely to stderr. See
// docs/design.md.
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
	root := cli.NewSinkRootCommand(cli.ToolSinkStdout)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
