// Command evm-sink-aws-sns is a sink CLI that reads the suite's JSONL record
// contract on stdin and publishes each record to an AWS SNS topic with
// at-least-once delivery (Publish, confirm-before-advance, blocking backoff on
// transient throttling/5xx/network errors, fail-fast on a permanent 4xx or an
// oversize message). A .fifo topic enables FIFO ordering (MessageGroupId from the
// record's partition identity) and dedup (MessageDeduplicationId from its dedup
// key). Credentials come from the AWS SDK default chain, never config. See
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
	root := cli.NewSinkRootCommand(cli.ToolSinkAWSSNS)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
