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

- [x] `internal/rpc` — mTLS transport + JSON-RPC client (`eth_chainId`,
      `eth_blockNumber`, `eth_getBlockByNumber`, `eth_getLogs`,
      `eth_getTransactionReceipt`); fail-fast mTLS validation; URL redaction on
      errors; coarse `error_type` classification. URL redaction preserves the
      wrapped error chain (`redactedError` keeps `Unwrap`) so a token-bearing
      transport failure still classifies as `connection_error` rather than
      `unknown`; covered by an end-to-end refused-connection test. → RPC
      Transport Security, Secret Handling.
- [x] `internal/chain` — chain ID resolution (with JSON safe-integer guard),
      block/header helpers. → chain.
- [x] `internal/metrics` — registry + HTTP server; the stream metric set
      (chain-health head-timestamp/age gauges wired from the head header each
      poll; `blockchain_chain_finalized_block_number` is reserved and stays 0
      until finality signaling lands — design Open Question 4);
      `/healthz` + `/readyz` (RPC + emit-blocked + lag), independent of scraping.
      → Metrics, RPC Health Checks.
- [x] `internal/stream` — event resolution (built-in ERC-20/721/1155 ABIs +
      per-contract `abi`/`abi_file`/`signatures` override), `topic0` match + ABI
      decode to `params`; HTTP poll loop at `poll_interval`; chunked
      `eth_getLogs` backfill (`log_chunk_blocks`) with gap-free handoff to
      head-following; native transfer detection (status==1, contract-creation,
      optional from/to allowlist); emit via `record`; lossless backpressure +
      `emit_blocked` gauge updated by a concurrent watchdog so an in-flight
      wedge (not just a completed write) grows the gauge and trips `/readyz`;
      `from_block = "latest"` resolves to head+1 (strictly-new blocks);
      exponential-backoff retry with jitter; graceful shutdown. → evm-stream.
- [x] `check rpc` implemented (one-shot, redacted JSON status, exit codes). →
      RPC Health Checks.
- [x] Tests: unit (httptest + generated certs) in default run; live-node
      (`anvil`/`geth --dev`) behind the `livenode` build tag. → Testing.
- [x] **Acceptance:** against a local node, emits decoded events + native
      transfers as JSONL; metrics show progress/lag; `check rpc` exits correctly;
      `validate` catches bad config/ABIs.

## M2 — evm-balance vertical slice

Goal: poll account/contract state and emit samples + change records.

- [x] `internal/balance` — poll native + ERC-20 balances; contract state
      (`native_balance`, `token_total_supply`, `transfer_count` window);
      decimals resolution (`eth_call decimals()` cached + config override);
      sampling cadence (`interval` xor `every_blocks`); change detection; emit
      `balance_*` and `contract_*` records. Added `eth_call`/`eth_getBalance` to
      `internal/rpc`; lossless backpressure + emit-blocked watchdog reused from
      M1; decimals resolved once at startup and cached per token (a token that
      omits `decimals()` with no override emits raw-only + a stderr warning).
      ERC-721 balance/ownership runtime landed later in M4. → evm-balance.
- [x] Balance metrics (account/contract gauges + transfer count) on a private
      registry, mirroring the M1 stream set; shared chain/RPC histograms +
      sample/change record counters. → Balance metrics.
- [x] Tests as in M1: unit (fakes + httptest end-to-end run) in the default run;
      live-node (`anvil`/`geth --dev`) behind the `livenode` build tag.
- [x] **Acceptance:** emits `*_sample` every tick and `*_change` on movement
      (with the prior value carried); metrics reflect configured entries;
      `validate` (cadence XOR, target/decimals checks, mTLS) and `check rpc`
      work.

## M3 — Release dry-run and install paths

Goal: prove the brew/curl artifacts build before a real tag.

- [x] `goreleaser release --snapshot --clean` builds the full OS/arch matrix
      (linux/darwin × amd64/arm64 for both `evm-stream` and `evm-balance`).
      Keyless cosign signing is delegated to `scripts/cosign-sign.sh`, which
      signs only when a GitHub OIDC identity (or a `COSIGN_PRIVATE_KEY` fallback)
      is present and otherwise skips with a clear message, so the bare snapshot
      command stays offline-safe while real tagged releases in CI still sign.
      → Release and Distribution.
- [x] Verify archives, `checksums.txt`, cosign config, rendered Homebrew casks
      (the tap standardizes on casks for pre-compiled binaries; GoReleaser's
      `brews` formula generator is deprecated), and `install.sh` (OS/arch
      detection, signed-checksums verify). `install.sh` was exercised end-to-end
      against the snapshot artifacts over a local mirror (`EVM_TOOLS_BASE_URL`):
      it detects OS/arch, downloads the matching archive, verifies the
      `checksums.txt` cosign signature against the pinned release identity/issuer
      with `cosign verify-blob` (failing closed if `cosign` is absent unless
      `EVM_TOOLS_SKIP_SIGNATURE=1` is set), verifies the SHA-256 from the now
      trusted `checksums.txt`, installs the binary, and fails closed on checksum
      mismatch / signature mismatch / unsupported binary / unsupported arch. The
      `cosign-sign.sh` skip branches write empty placeholders for the registered
      `signs.output` paths so a signing-skipped release never references a
      missing `checksums.txt.sig`/`.pem` upload asset. → Release and
      Distribution.
- [ ] First real tag `v0.1.0` once the org adds `HOMEBREW_TAP_GITHUB_TOKEN` +
      cosign secrets (maintainer task — needs an external secret). → Release
      automation.
- [ ] **Acceptance:** snapshot succeeds (done); `install.sh` resolves the
      matching artifact (verified against the snapshot); `brew install` works
      from the tap after the tagged release (blocked on the real tag + tap
      token, a maintainer task).

## M4 — Specified deferred producer features

Goal: land the deferred *producer* features the design and record contract
already specify and that need no open product decision — the ERC-721 ownership
runtime above all. Scope is deliberately limited to clearly-specified work; the
sinks and anything tied to a design Open Question stay deferred (see below).

- [x] `internal/balance` ERC-721 balance runtime
      (`[[balance.erc721_balances]]`, mode `balance_of`): `balanceOf(owner)` →
      `balance_sample`/`balance_change` with `kind: erc721` and a `count` (no
      decimals — ERC-721 carries counts, not decimals). Reuses the existing
      change-detection + lossless-emit path. → evm-balance, Record Contract.
- [x] `internal/balance` ERC-721 ownership runtime
      (`[[balance.erc721_ownership]]`): `ownerOf(token_id)` →
      `ownership_sample`/`ownership_change` (the latter carrying `previous_owner`).
      Owner comparison is case-insensitive so a checksum-vs-lowercase RPC
      difference is not mistaken for a transfer; the configured `token_id` is
      carried verbatim. → evm-balance, Record Contract.
- [x] `ownerOf(uint256)` selector + token-ID encoding (decimal or `0x`-hex) and
      an address decoder added to `internal/balance/abi.go`; `Resolve` validates
      the new sections (required token/owner/token_id, numeric token_id, and the
      only supported `balance_of` mode) so typos/unsupported modes fail fast in
      `validate`. → evm-balance.
- [x] Configured-count gauges `evm_balance_configured_erc721_balances` /
      `evm_balance_configured_erc721_ownership`; the ERC-721 token count reuses
      the exporter-aligned `blockchain_account_token_balance_raw` gauge.
      `token_id`/`owner` stay out of labels (high-cardinality / counterparty per
      the metric rules). → Balance metrics.
- [x] Tests: ABI selector/encoding/decoder units; resolve happy-path + rejection
      cases; poller sample/change for both ERC-721 kinds (httptest + fakes);
      CLI `validate` + end-to-end `run` emitting erc721 `balance_sample` and
      `ownership_sample`; an env-gated `livenode` ERC-721 ownership test.
- [x] **Acceptance:** build/vet/test/lint green offline; `evm-balance` emits
      `balance_sample`/`balance_change` (kind erc721) and
      `ownership_sample`/`ownership_change` for configured ERC-721 entries, with
      the prior value carried on change; `validate` catches bad ERC-721 config.

Intentionally **out of M4** (each needs a design Open Question resolved or an
external secret, so each is recorded as a blocker rather than guessed):
internal/trace native transfers (Open Question 3); reorg handling + the additive
`finalized`/`removed` field and reorg re-emission (Open Question 4); config
reload (+ metric reset); checkpointing/resume; the sinks `evm-sink-kafka` and
`evm-sink-webhook` and the webhook sink's scope/delivery semantics (Open
Questions 1, 2). Value interpolation (`${VAR}`/`_cmd`) is now implemented — see
Post-M4 follow-ups below.

## S1 — evm-sink-kafka

Goal: the first sink — read the suite's JSONL contract on stdin and publish each
record to Kafka with at-least-once delivery, settling design Open Question 2
(delivery semantics) for this build. Pure-Go client only (segmentio/kafka-go), so
the static cross-compiled release binaries are unaffected.

- [x] **`internal/record` JSONL Reader/decoder** (`reader.go`) shared by both
      sinks: streaming, line-oriented `Reader.Next()` returning `Envelope`,
      `Reader.Raw()` for verbatim forwarding, blank-line skip, hard error on a
      malformed line, and `schema_version` accept/reject (`ErrSchemaUnsupported`).
      Round-trip + golden-driven tests prove it is the faithful inverse of the
      encoder, which stays the source of truth. → Record Contract, Testing.
- [x] **Dedup / partition-key identity on the contract** (`key.go`):
      `Envelope.DedupKey()` per the documented per-class composition (event:
      chain_id+tx_hash+log_index, reorg-stable; native_transfer: chain_id+tx_hash;
      `*_sample`: +block_number+emitted_at; `*_change`: +block_number) and
      `Envelope.PartitionIdentity()` for per-key ordering. Tested for reorg
      stability and ordering. → Deduplication and resume keys.
- [x] **`internal/kafkasink`** core: `Publisher` interface wrapping the actual
      publish (real segmentio/kafka-go `*Writer`, `RequiredAcks=all`, `Hash`
      balancer for per-key ordering) behind it so default tests use an in-memory
      fake (no broker); at-least-once loop (confirm-before-advance, blocking
      exponential backoff + full jitter on transient failure, fail-fast on a
      permanent broker rejection, never drop a record); topic routing
      (default + per-type map); partition key = dedup identity (configurable:
      identity|dedup|none). → Sink delivery semantics, Topic routing.
- [x] **SASL/TLS auth** (`writer.go`): SASL PLAIN and SCRAM-SHA-256/512 over TLS;
      password sourced through the existing config env-interpolation/`_cmd`
      machinery (`password_cmd`), never hardcoded or logged; SASL requires TLS
      (fail fast otherwise). → Auth, Secret Handling.
- [x] **`[kafka]` config + flags**: `config.DecodeKafka` (shared keys +
      `[kafka]`, strict, sibling sections ignored) with `brokers`, `topic`,
      `topic_by_type`, `partition_key`, `required_acks` (only `all`), backoff/
      batch tuning, `[kafka.sasl]`, `[kafka.tls]`, `[kafka.metrics]`; env binding
      (`EVM_TOOLS_KAFKA_*`) and `--brokers`/`--topic` flags. → Configuration.
- [x] **`cmd/evm-sink-kafka` + sink CLI** (`internal/cli/sink.go`,
      `internal/cli/kafka.go`): a sink-shaped command tree (run, validate,
      version — no `check rpc`/`--rpc-*`), per-suite Prometheus metric set
      (`internal/metrics/kafka.go`: records consumed/published/failed, publish
      duration histogram, retry/backoff/blocked gauges) on the metrics server
      with `/healthz` + `/readyz` (broker-reachable + publish-blocked), graceful
      shutdown that flushes/closes the writer; slog on stderr. → Metrics.
- [x] **Tests**: record Reader round-trip + schema-reject + dedup/partition key;
      kafkasink loop with a fake (publishes every record, topic routing,
      partition keying, transient-retry, permanent fail-fast, confirm-before-
      advance, malformed-line fail-fast); config decode/defaults/sibling-ignore/
      env-override/`password_cmd`; metrics endpoint + sink `/readyz`; CLI
      version/help/validate/run (fake publisher); a `livekafka`-tagged real-broker
      test. All offline-safe in the default `go test ./...`. → Testing.
- [x] **Release**: `evm-sink-kafka` build + archive + Homebrew cask in
      `.goreleaser.yaml`; `install.sh` aware of the new binary. → Release.
- [x] **Acceptance:** build/vet/test/lint green offline; `goreleaser check`
      passes; `evm-sink-kafka --help`/`version` work; validate catches bad
      config (missing brokers/topic, unsupported `required_acks`/`partition_key`,
      SASL-without-TLS); run publishes every stdin record at-least-once and blocks
      (never drops) on a stalled broker.

## S2 — evm-sink-webhook

Goal: the second sink — read the suite's JSONL contract on stdin and forward each
record over HTTP with at-least-once delivery, settling design Open Question 1
(webhook scope) for this build: a FORWARDER with OPTIONAL FILTERS, not a rule
DSL. Reuses the shared JSONL `record.Reader` and the sink command tree from S1.

- [x] **`internal/webhooksink` core** (`webhooksink.go`): read JSONL via
      `record.Reader`, forward each record (that passes the filter) over HTTP;
      at-least-once loop (confirm-before-advance, blocking exponential backoff +
      full jitter on transient failure, fail-fast on a permanent failure, never
      drop a record). The actual HTTP delivery is behind a `Poster` interface so
      default tests use `net/http/httptest` (no external endpoint). → Sink
      delivery semantics.
- [x] **HTTP poster** (`poster.go`): real `net/http`-backed `Poster` — POST
      (default; PUT/PATCH allowed) the verbatim JSONL payload as
      `application/json`; 2xx is success; a transient error (network/timeout/HTTP
      5xx) surfaces for retry; a permanent HTTP 4xx is wrapped in
      `*PermanentError` so the sink fails fast (exit non-zero) rather than
      silently dropping (preserves losslessness). URL redaction (query/userinfo
      stripped) for safe logging. → Secret Handling.
- [x] **Forwarder filters** (`filter.go`): forward all by default; optional
      include/exclude by record type and name plus a single simple field
      condition (`eq`/`gt`/`lt` on one named data field, numeric/range-safe via
      `big.Float`). AND semantics; an unsupported op / empty field name fails
      fast in `validate`. → design Open Question 1 (settled).
- [x] **Optional auth header**: a configurable request header (e.g.
      `Authorization: Bearer …`) whose value is sourced through the existing
      env-interpolation/`_cmd` machinery (`value_cmd`), never hardcoded or
      logged. → Auth, Secret Handling.
- [x] **`[webhook]` config + flags**: `config.DecodeWebhook` (shared keys +
      `[webhook]`, strict, sibling sections ignored) with `url`, `method`,
      `headers`, `timeout`, backoff tuning, `[webhook.auth]`,
      `[webhook.filters]` (+ `[webhook.filters.field]`), `[webhook.metrics]`; env
      binding (`EVM_TOOLS_WEBHOOK_*`) and a `--url` flag. → Configuration.
- [x] **`cmd/evm-sink-webhook` + sink CLI** (`internal/cli/webhook.go`, reusing
      `internal/cli/sink.go`): the same sink-shaped command tree (run, validate,
      version — no `check rpc`/`--rpc-*`), per-suite Prometheus metric set
      (`internal/metrics/webhook.go`: records consumed/filtered/forwarded/failed,
      POST duration histogram, retry/backoff/blocked gauges) on the metrics
      server with `/healthz` + `/readyz` (endpoint-reachable + post-blocked),
      graceful shutdown; slog on stderr. → Metrics.
- [x] **Tests**: webhooksink loop with httptest (forward all, 5xx-retry,
      4xx-fail-fast, malformed-line fail-fast, confirm-before-advance, auth
      header); filter include/exclude by type/name + field-condition eq/gt/lt +
      missing-field + combined; poster URL/method validation + redaction; config
      decode/defaults/sibling-ignore/env-override/`value_cmd`; metrics endpoint +
      sink `/readyz`; CLI version/help/validate/run (fake poster) + filter run.
      All offline-safe in the default `go test ./...`. → Testing.
- [x] **Release**: `evm-sink-webhook` build + archive + Homebrew cask in
      `.goreleaser.yaml`; `install.sh` aware of the new binary. → Release.
- [x] **Acceptance:** build/vet/test/lint green offline; `goreleaser check`
      passes; `evm-sink-webhook --help`/`version` work; validate catches bad
      config (missing url, unsupported field-condition op); run forwards every
      passing stdin record at-least-once (verbatim, `application/json`), blocks
      (never drops) on a 5xx/stalled endpoint, and fails fast on a 4xx.

## S3 — Container and release polish

Goal: production polish for the sinks now that S1/S2 are built — container image,
container-logging guidance, and config/README coverage of the sinks. No new
product decisions.

- [x] **Logging in containers** — extend the design's Logging subsection with a
      `#### Logging in containers` paragraph: stdout carries the JSONL data and
      stderr carries the `log/slog` diagnostics; Docker/Kubernetes capture both,
      so `docker logs` / `kubectl logs` surface stderr without putting logs on
      stdout (which would corrupt the contract) — the 12-factor "logs as a
      stream" expectation is met. Recommend `--log-format json` for aggregation;
      run a producer's stdout into a sink (or redirect it), never `2>&1`; note the
      distroless/scratch no-shell caveat for `_cmd`. Added as a `####` subsection
      so the Contents TOC needs no change. → Logging.
- [x] **Multi-stage `Dockerfile`** producing one image with all four binaries on
      an `alpine` base (has a shell, so `_cmd` works); distroless caveat
      documented in a comment; `+ .dockerignore`. Built with `docker build` and
      smoke-tested (all four `version`s, shell present, non-root, JSON output on
      stdout); `hadolint` clean. → Release and Distribution, Configuration.
- [x] **Config + README sink coverage** — `[kafka]`/`[webhook]` sample config
      added to the design Configuration example and to the README, plus
      producer | sink pipeline usage and the container-image section in the
      README. → Configuration.
- [x] **Plan + Tool Suite statuses** — this S1/S2/S3 section; the design Tool
      Suite table marks `evm-sink-kafka` Built (S1) and `evm-sink-webhook`
      Built (S2). → Tool Suite.
- [x] **Acceptance:** build/vet/test/lint green offline; `goreleaser check`
      passes; `Dockerfile` builds and all four binaries run in the image;
      `hadolint` clean; docs render and lint.

## Deferred (post-spine, per design)

Native transfer internal/trace transfers; config reload (+ metric reset); reorg
handling and the additive `finalized`/`removed` field; checkpointing/resume. See
design [Open Questions](design.md#open-questions). (ERC-721 balance/ownership
runtime is done — see M4; the `evm-sink-kafka` sink is done — see S1; the
`evm-sink-webhook` sink is done — see S2.)

## Post-M4 follow-ups

- [x] **Config value interpolation + `_cmd` execution** (`internal/config/resolve.go`):
  `${VAR}`, `${VAR:-default}`, and `$$` expand on file-sourced values; `<field>_cmd`
  keys run via `sh -c` (trimmed stdout), opt-in behind `--allow-exec` /
  `EVM_TOOLS_ALLOW_EXEC`. A `_cmd` while exec is disabled is fatal; setting both
  `<field>` and `<field>_cmd` is an error; a flag/env binding short-circuits a
  `_cmd` (a built-in default does not); a non-zero exit is fatal with the
  command's stderr surfaced; a missing `sh` is fatal. Interpolation applies to
  file-sourced values only — binding values are left literal. Covered by
  `resolve_test.go`. Implements design [Configuration](design.md#configuration)
  and [Secret Handling](design.md#secret-handling).
