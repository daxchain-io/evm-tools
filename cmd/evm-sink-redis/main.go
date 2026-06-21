// Command evm-sink-redis is a sink CLI that reads the suite's JSONL record
// contract on stdin and appends each record to a Redis Stream (XADD). Delivery is
// at-least-once and, with dedup enabled (the default), effectively exactly-once in
// the stream: an atomic dedup-gated append keyed on the record's dedup identity
// makes a duplicate from a retry or an overlapping re-run a no-op rather than a
// second entry. A transient error (connection loss, LOADING, CLUSTERDOWN, a
// network/timeout) is retried with blocking backoff; a permanent error (a
// WRONGTYPE stream key, an auth failure) fails fast. The connection URL is a
// secret sourced via config _cmd/${VAR}, never an argument, and is never logged.
// See docs/design.md.
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
	root := cli.NewSinkRootCommand(cli.ToolSinkRedis)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
