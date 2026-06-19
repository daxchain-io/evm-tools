// Command evm-balance is a CLI for polling EVM balances and contract state and
// writing the results to stdout as JSONL: balance samples/changes, ERC-721
// ownership, and contract state. See docs/design.md.
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
	root := cli.NewRootCommand(cli.ToolBalance)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
