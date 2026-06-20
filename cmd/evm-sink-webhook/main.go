// Command evm-sink-webhook is a sink CLI that reads the suite's JSONL record
// contract on stdin and forwards each record over HTTP to a configured URL with
// at-least-once delivery (POST application/json, confirm-before-advance, blocking
// exponential backoff on transient failures, fail-fast on a permanent HTTP 4xx).
// It is a forwarder with optional filters (include/exclude by record type and
// name plus one simple field condition) — not a rule DSL. See docs/design.md.
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
	root := cli.NewSinkRootCommand(cli.ToolSinkWebhook)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
