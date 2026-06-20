// Command evm-sink-kafka is a sink CLI that reads the suite's JSONL record
// contract on stdin and publishes each record to Kafka with at-least-once
// delivery (RequiredAcks=all, confirm-before-advance, blocking exponential
// backoff on transient failures). See docs/design.md.
//
// This is a thin entrypoint over internal/cli, which builds the sink command
// tree (run, validate, version).
package main

import (
	"fmt"
	"os"

	"github.com/daxchain-io/evm-tools/internal/cli"
)

func main() {
	root := cli.NewSinkRootCommand(cli.ToolSinkKafka)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
