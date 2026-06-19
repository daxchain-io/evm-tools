# CLAUDE.md

Working context for this repository. Read [docs/design.md](docs/design.md) for
the full spec and [docs/plan.md](docs/plan.md) for the build sequence; this file
is the short version of how to work here.

## What this is

`evm-tools` is a Go monorepo of composable CLIs for observing Codex Chain (and
other EVM chains). Producers (`evm-stream`, `evm-balance`) emit newline-delimited
JSON to stdout; downstream sinks (`evm-sink-kafka`, `evm-sink-webhook`, roadmap)
consume it. Module path: `github.com/daxchain-io/evm-tools`. Go 1.22+ (toolchain
pinned in `go.mod`).

## Commands

```sh
go build ./...                 # build all binaries/packages
go test ./...                  # unit tests (golden + httptest); live-node tests are behind a build tag
go vet ./...
golangci-lint run              # v2 config in .golangci.yml
gofmt -l . && go mod tidy      # must be clean
goreleaser check               # validate release config
goreleaser release --snapshot --clean   # local release dry-run
```

Run a single package's tests: `go test ./internal/record -run TestName -v`.

## Layout

- `cmd/<tool>/` — thin entrypoints (`evm-stream`, `evm-balance`).
- `internal/record` — the JSONL contract: envelope + record types + the
  synchronized encoder. **Source of truth.**
- `internal/config` — Viper/TOML load, precedence, interpolation/`_cmd`,
  per-tool decode.
- `internal/rpc` — mTLS RPC transport + client.
- `internal/metrics` — Prometheus registry + HTTP server + health endpoints.
- `internal/chain` — chain metadata + block helpers.
- `internal/buildinfo` — version stamped via `-ldflags`.
- `internal/stream`, `internal/balance` — per-tool core logic.

## Load-bearing conventions

- **Records go through `internal/record` only.** All amounts (wei, raw units,
  formatted balances, counts, supply) are JSON **strings**; only `decimals`,
  `window_blocks`, and envelope counters are numbers. Each line is written
  atomically through one synchronized writer and flushed per line — never write
  to stdout directly from a monitor goroutine.
- **stdout is data, stderr is humans.** Diagnostics use `log/slog` on stderr
  (`--log-level`, `--log-format`). Never print logs to stdout.
- **mTLS is required** for HTTPS RPC; fail fast on missing/invalid material.
  **Never log secrets** — redact RPC URLs (strip query/userinfo), don't echo
  `${VAR}`/`_cmd` values, and keep secrets out of metric labels.
- **Lossless backpressure.** When stdout blocks, propagate it upstream; never
  drop records or buffer unbounded.
- **Metric naming:** counters end `_total`, gauges are bare, durations are
  `_seconds` histograms; labels stay low-cardinality (no `tx_hash`/address
  firehoses).
- **Config:** precedence is flags > env (`EVM_TOOLS_` prefix, needs a key
  replacer for nested keys) > TOML > defaults. Each tool strict-decodes its own
  subtree and ignores sibling tools' sections.
- **Commands:** `run`, `validate`, `check rpc`, `version` (Cobra). `validate`
  checks config + mTLS + ABI resolution without connecting to monitor.

## Workflow

- The repo is closed to outside contributions (see `CONTRIBUTING.md`); the
  default branch `main` has no protection, so maintainers commit directly.
- End AI-authored commit messages with
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Keep the TOC in `docs/design.md` in sync when adding/removing `##` sections.
- Build the plan milestone by milestone (see `docs/plan.md`); each must end
  build/vet/test/lint green.
