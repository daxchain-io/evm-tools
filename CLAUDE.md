# CLAUDE.md

Working context for this repository. Read [docs/design.md](docs/design.md) for
the full spec and [docs/plan.md](docs/plan.md) for the build sequence; this file
is the short version of how to work here.

## What this is

`evm-tools` is a Go monorepo of composable CLIs for observing EVM chains.
Producers (`evm-stream`, `evm-balance`) emit newline-delimited JSON over the
record transport (a Unix socket via `--output`); downstream sinks
(`evm-sink-kafka`, `evm-sink-webhook`, `evm-sink-file`, `evm-sink-aws-sqs`,
`evm-sink-aws-sns`, `evm-sink-postgres`, `evm-sink-redis`, `evm-sink-stdout`) dial
in and consume it. stdout carries logs, not records (except `evm-sink-stdout`,
whose job is to write records to stdout — it logs to stderr). Module path:
`github.com/daxchain-io/evm-tools`.
Go 1.22+ (toolchain pinned in `go.mod`).

## Commands

```sh
go build ./...                 # build all binaries/packages
go test ./...                  # unit tests (golden + httptest); live-node tests are behind a build tag
make integration               # compose stack (Kafka/Redis/Postgres/LocalStack/anvil) + `-tags integration` live tests
go vet ./...
golangci-lint run              # v2 config in .golangci.yml
gofmt -l . && go mod tidy      # must be clean
go run golang.org/x/vuln/cmd/govulncheck@latest ./...   # CI runs this as a job
go test ./internal/record -run='^$' -fuzz=FuzzReaderNext -fuzztime=20s  # fuzz the JSONL parser
goreleaser check               # validate release config
goreleaser release --snapshot --clean   # local release dry-run
```

Run a single package's tests: `go test ./internal/record -run TestName -v`.

## Layout

- `cmd/<tool>/` — thin entrypoints (`evm-stream`, `evm-balance`,
  `evm-sink-kafka`, `evm-sink-webhook`, `evm-sink-file`, `evm-sink-aws-sqs`,
  `evm-sink-aws-sns`, `evm-sink-postgres`, `evm-sink-redis`, `evm-sink-stdout`).
- `internal/record` — the JSONL contract: envelope + record types + the
  synchronized encoder/reader. **Source of truth.**
- `internal/config` — Viper/TOML load, precedence, interpolation/`_cmd`,
  per-tool decode.
- `internal/rpc` — TLS RPC transport + client (server-auth by default, optional mTLS).
- `internal/metrics` — Prometheus registry + HTTP server + health endpoints.
- `internal/chain` — chain metadata + block helpers.
- `internal/buildinfo` — version stamped via `-ldflags`.
- `internal/cli` — shared Cobra command trees for producers and sinks.
- `internal/stream`, `internal/balance` — producer core logic.
- `internal/kafkasink`, `internal/webhooksink`, `internal/filesink` — sink core
  logic (`filesink` = rotating writer + filter + at-least-once run loop).
- `internal/awssink` — shared AWS SQS/SNS sink core (FIFO-aware, 256 KB guard).
- `internal/pgsink` — Postgres sink core (idempotent `ON CONFLICT` insert via pgx).
- `internal/redissink` — Redis Streams sink core (dedup-gated `XADD` via go-redis).
- `internal/stdoutsink` — stdout sink core (verbatim record line → stdout; the
  composability hatch for `| jq`/piping; logs to stderr).
- `internal/checkpoint` — durable resume cursor for evm-stream (atomic temp+fsync+rename).
- `internal/keyperm` — shared private-key file-mode warning.
- `internal/deadletter` — opt-in poison-record quarantine (file-backed,
  lossless base64 JSONL); wired onto `record.Reader.Quarantine` by the sink CLI.
- `internal/integration` — build-tagged (`integration`) live tests against the
  `compose.yaml` stack; run via `make integration`. Offline `go test` skips them.

## Load-bearing conventions

- **Records go through `internal/record` only.** All amounts (wei, raw units,
  formatted balances, counts, supply) are JSON **strings**; only `decimals`,
  `window_blocks`, and envelope counters are numbers. Each line is written
  atomically through one synchronized writer and flushed per line — never write
  records directly to the output from a monitor goroutine.
- **stdout is logs; records go over the transport.** Records never touch stdout —
  they travel over the record transport (`internal/transport`). A producer opts in
  with `--output socket` (the well-known per-host socket via `transport.DefaultSpec`)
  or `--output unix:/path`; empty `--output` = exporter-only. A sink's `--input`
  **defaults** to that socket (`-`/`stdin` reads stdin for replay). Logs use
  `log/slog` split by level: `debug`/`info`/`warn` on stdout, `error` on stderr
  (`--log-level`, `--log-format`). Never write records to stdout — `evm-sink-stdout`
  is the lone tool that does (its job), and it logs to stderr.
- **TLS for HTTPS RPC; mTLS when configured.** Public endpoints use server-auth
  TLS (no client cert); a `client_cert`/`client_key` pair upgrades to mTLS, and
  `require_mtls` (or `--rpc-require-mtls`) makes a missing client cert fail fast
  for private nodes. Fail fast on invalid/partial material. **Never log secrets**
  — redact RPC URLs (strip query/userinfo), don't echo `${VAR}`/`_cmd` values,
  and keep secrets out of metric labels.
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
