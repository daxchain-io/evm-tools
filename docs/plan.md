# evm-tools Implementation Plan & Roadmap

Derived from [design.md](design.md): the design is the *what and why*, this is
the *how and in what order*. The suite was built milestone by milestone
(summarized below); the forward-looking backlog lives under
[Deferred](#deferred) and the design's [Open Questions](design.md#open-questions).

Every milestone landed green —
`go build ./... && go vet ./... && go test -race ./... && golangci-lint run && goreleaser check` —
and the `record` package is the contract: any change to it requires updated golden
tests. Full per-task detail is in the git history; design rationale is in
[design.md](design.md).

## Status

Shipped through **v0.5.2**: all nine CLIs (two producers, seven sinks), signed
releases (cosign keyless), a single Homebrew cask, and a container image.
Governance in place — `CONTRIBUTING.md`, `SECURITY.md`, `LICENSE` (Apache-2.0),
private vulnerability reporting, org base permission read-only.

## Shipped milestones

| Milestone | Delivered |
| --- | --- |
| M0 | Repo scaffold: the `internal/record` contract (envelope + 8 record types + a synchronized, line-atomic encoder), config/CLI/metrics/buildinfo skeletons, CI + release tooling. |
| M1 | `evm-stream`: mTLS RPC client, chunked `eth_getLogs` backfill → gap-free head-following, ABI decode, success-gated native-transfer detection, lossless backpressure, metrics + `/healthz`/`/readyz`. |
| M2 | `evm-balance`: native/ERC-20 balances + contract state (balance / supply / transfer-count window), decimals resolution, interval-XOR-block cadence, change detection. |
| M3 | Release dry-run: GoReleaser snapshot (linux/darwin × amd64/arm64), cosign keyless signing, `install.sh` (signed-checksum verify, fail-closed), Homebrew cask smoke test. |
| M4 | ERC-721 balance + ownership runtime; config value interpolation (`${VAR}` / `_cmd`, opt-in behind `--allow-exec`). |
| S1 | `evm-sink-kafka`: shared JSONL `record.Reader` + dedup/partition keys; at-least-once publish, SASL/TLS, topic routing. |
| S2 | `evm-sink-webhook`: at-least-once HTTP forward with optional type/name/field filters. |
| S3 | Container + release polish: multi-stage `Dockerfile`, container-logging guidance, sink config/README coverage. |
| S4 | `evm-sink-file`: rotating file sink (size/age rotation, gzip, retention, fsync, filters). |
| S5 | `evm-sink-aws-sqs` / `evm-sink-aws-sns`: shared AWS core, FIFO-aware, credentials from the SDK default chain. |
| S6 | `evm-sink-postgres`: idempotent `ON CONFLICT (dedup_key)` insert (pgx), injection-safe table name. |
| S7 | Near-head reorg detection + `reorg` marker; head-staleness readiness; balance per-target parallelism; `evm-sink-redis` (atomic dedup-gated `XADD`). |
| post-S7 | `config.toml` auto-discovery from `~/.evm-tools` (legacy `evm-tools.toml` fallback). |

## Deferred

Not yet built — each needs a design decision or is deliberately out of scope for
now (see design [Open Questions](design.md#open-questions)):

- **Internal/trace native transfers** (`include_internal`) — provider-dependent
  (needs a trace RPC many endpoints don't expose); intentionally not built.
- **Finality signaling** — the additive `finalized` envelope field. Near-head reorg
  *detection* shipped in S7 and the finalized-block *metric* shipped post-S7;
  *waiting* for finality stays a non-goal for the low-latency default, so the field
  itself is deferred.
- **Config reload — log level/format only (shipped); entry reload deferred.**
  `SIGHUP` live-applies `log.level`/`log.format`; hot-swapping contracts/targets
  (and resetting their metrics) is deferred in favour of restart + resume, which is
  now gap-free via the checkpoint cursor.
- **Kafka exactly-once** — idempotent/transactional producer (the other half of
  the old #6; checkpoint/resume below shipped).
- **Consolidated metrics endpoint** — resolved at the scrape layer (one logical
  endpoint per pod); documented in `deploy/README.md`, no code.

Shipped since the milestones above:

- **Checkpoint / resume cursor** (`internal/checkpoint`, `stream.checkpoint_file`):
  durable atomic cursor; restart resumes gap-free instead of jumping to head.
- **Live / integration test harness** — still open: a docker-compose stack (a dev
  chain + Kafka + Redis + Postgres + LocalStack) plus live tests for the sinks that
  lack them, as an optional CI job. The default suite is unit-level (fakes / golden
  / httptest); the largest gap before a confident 1.0.
- **Operator kit** — shipped in `deploy/` (Prometheus config + recording/alert
  rules, Grafana dashboard, runbook).
