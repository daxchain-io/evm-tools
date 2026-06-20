// Command evm-sink-postgres is a sink CLI that reads the suite's JSONL record
// contract on stdin and inserts each record into a PostgreSQL table. Inserts are
// idempotent (ON CONFLICT (dedup_key) DO NOTHING keyed on the record's dedup
// identity), so at-least-once delivery is effectively exactly-once in the table:
// a duplicate from a retry or an overlapping re-run is a no-op, not a duplicate
// row. A transient DB error (connection loss, deadlock, serialization failure) is
// retried with blocking backoff; a permanent error (schema/permission/data) fails
// fast. The connection DSN is a secret sourced via config _cmd/${VAR}, never an
// argument, and is never logged. See docs/design.md.
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
	root := cli.NewSinkRootCommand(cli.ToolSinkPostgres)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
