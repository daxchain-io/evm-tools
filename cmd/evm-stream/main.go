// Command evm-stream is a long-running CLI for live EVM activity monitoring.
// It follows the configured chain by HTTP polling and writes each observed
// contract event and native transfer to stdout as JSONL. See docs/design.md.
//
// This is a thin entrypoint over internal/cli, which builds the shared command
// tree (run, validate, check rpc, version).
package main

import (
	"fmt"
	"os"

	"github.com/daxchain-io/evm-tools/internal/cli"
)

func main() {
	root := cli.NewRootCommand(cli.ToolStream)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
