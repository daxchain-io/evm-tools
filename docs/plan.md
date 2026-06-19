# evm-tools Implementation Plan

This is the build plan derived from [design.md](design.md). The design is the
*what and why*; this plan is the *how and in what order*. Work proceeds in
milestones; each task cites the design section it implements, and a milestone is
"done" only when its acceptance criteria pass.

Conventions:

- `[ ]` open, `[x]` done.
- Every milestone ends green:
  `go build ./... && go vet ./... && go test ./... && golangci-lint run && goreleaser check`.
- The `record` package is the contract; any change to it requires updated golden
  tests.
- One milestone at a time; pause for review at each milestone boundary.

## Current state

- [x] `git init`; `main` pushed to `github.com/daxchain-io/evm-tools` (public).
- [x] `README.md` and `docs/design.md` (full spec).
- [x] Governance: `CONTRIBUTING.md`, `SECURITY.md`, `LICENSE` (Apache-2.0),
      private vulnerability reporting enabled, org base permission read-only.
- [ ] Everything below.

## M0 — Repository scaffold (the spine)

Goal: a compiling, CI-green skeleton with the data contract and command surface
in place — no real RPC logic yet.

- [x] `go.mod` (module `github.com/daxchain-io/evm-tools`, Go 1.22+ `go`
      directive) and `.gitignore`.
- [x] Layout: `cmd/evm-stream`, `cmd/evm-balance`;
      `internal/{config,rpc,record,metrics,chain,buildinfo,stream,balance}`.
- [x] **`internal/record` built for real** — envelope + all 8 record types + a
      single synchronized, line-atomic, per-line-flushing encoder; amounts
      string-encoded. → Record Contract, Output discipline.
- [x] Golden-file tests for `record` (envelope fields, RFC 3339, omit-empty,
      string amounts, `schema_version`). → Testing.
- [x] `internal/buildinfo` — version/commit/date vars; `version` command prints
      them (+ `--json`); `-ldflags -X` wiring. → Version stamping.
- [x] `internal/config` — typed structs for shared + `[stream]`/`[balance]`;
      Viper load skeleton (precedence, per-tool strict decode of own subtree,
      `EVM_TOOLS_` prefix + key replacer); interpolation/`_cmd` may start
      stubbed. → Configuration.
- [x] Cobra trees for both CLIs — `run`, `validate`, `check rpc`, `version` with
      the full shared flag set (`--config`, `--rpc-*`, `--metrics*`,
      `--log-level/-format`, `--allow-exec`); unimplemented paths return a clear
      "not implemented" error. → Command shape.
- [x] `--log-level`/`--log-format` wired to `log/slog` on stderr. → Logging.
- [x] Tooling: `.github/workflows/ci.yml` + `release.yml`, `.goreleaser.yaml`,
      `.golangci.yml`, markdownlint config. → Quality and CI, Release.
- [x] **Acceptance:** build/vet/test/lint green; `goreleaser check` passes;
      `--help` and `version` work for both binaries; record golden tests pass.

## M1 — evm-stream vertical slice

Goal: real monitoring end to end — connect, follow the head, emit decoded
events and native transfers as JSONL, expose metrics.

- [ ] `internal/rpc` — mTLS transport + JSON-RPC client (`eth_chainId`,
      `eth_blockNumber`, `eth_getBlockByNumber`, `eth_getLogs`); fail-fast mTLS
      validation; URL redaction on errors. → RPC Transport Security, Secret
      Handling.
- [ ] `internal/chain` — chain ID resolution, block/header helpers. → chain.
- [ ] `internal/metrics` — registry + HTTP server; the stream metric set;
      `/healthz` + `/readyz` (RPC + emit-blocked + lag), independent of scraping.
      → Metrics, RPC Health Checks.
- [ ] `internal/stream` — event resolution (built-in ERC-20/721/1155 ABIs +
      per-contract `abi`/`signatures` override), `topic0` match + ABI decode to
      `params`; HTTP poll loop at `poll_interval`; chunked `eth_getLogs` backfill
      (`log_chunk_blocks`) with gap-free handoff to head-following; native
      transfer detection (status==1, contract-creation, optional from/to
      allowlist); emit via `record`; lossless backpressure + `emit_blocked`
      gauge; exponential-backoff retry; graceful shutdown. → evm-stream.
- [ ] `check rpc` implemented (one-shot, exit codes). → RPC Health Checks.
- [ ] Tests: unit (httptest + generated certs) in default run; live-node
      (`anvil`/`geth --dev`) behind a build tag. → Testing.
- [ ] **Acceptance:** against a local node, emits decoded events + native
      transfers as JSONL; metrics show progress/lag; `check rpc` exits correctly;
      `validate` catches bad config/ABIs.

## M2 — evm-balance vertical slice

Goal: poll account/contract state and emit samples + change records.

- [ ] `internal/balance` — poll native + ERC-20 balances; contract state
      (`native_balance`, `token_total_supply`, `transfer_count` window);
      decimals resolution (`eth_call decimals()` cached + config override);
      sampling cadence (`interval` xor `every_blocks`); change detection; emit
      `balance_*` and `contract_*` records. → evm-balance.
- [ ] Balance metrics (account/contract gauges + transfer count). → Balance
      metrics.
- [ ] Tests as in M1.
- [ ] **Acceptance:** emits `*_sample` every tick and `*_change` on movement;
      metrics reflect configured entries; `validate`/`check rpc` work.

## M3 — Release dry-run and install paths

Goal: prove the brew/curl artifacts build before a real tag.

- [ ] `goreleaser release --snapshot --clean` builds the full OS/arch matrix.
- [ ] Verify archives, `checksums.txt`, cosign config, rendered Homebrew
      formulae, and `install.sh` (OS/arch detection, checksum verify). → Release
      and Distribution.
- [ ] First real tag `v0.1.0` once the org adds `HOMEBREW_TAP_GITHUB_TOKEN` +
      cosign secrets (maintainer task). → Release automation.
- [ ] **Acceptance:** snapshot succeeds; `install.sh` resolves the matching
      artifact; `brew install` works from the tap after the tagged release.

## Deferred (post-spine, per design)

Native transfer internal/trace transfers; ERC-721 ownership runtime; config
reload (+ metric reset); reorg handling and the additive `finalized`/`removed`
field; checkpointing/resume; the sinks (`evm-sink-kafka`, `evm-sink-webhook`)
and the webhook sink's scope. See design [Open Questions](design.md#open-questions).
