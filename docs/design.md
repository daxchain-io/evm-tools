# EVM Tools

This repository is a suite of composable command-line tools for observing EVM
chains and moving that data into downstream systems. The tools follow a
Unix-pipeline philosophy: each binary does one job
well, reads its settings from a shared configuration namespace, and speaks a
single common data contract — newline-delimited JSON (JSONL) emitted over a
record transport (a Unix socket); stdout carries logs, not records.

The first two tools are producers:

- `evm-stream`: live EVM activity monitoring (contract events and native ETH
  transfers) emitted as JSONL.
- `evm-balance`: balance and contract-state polling emitted as JSONL.

Downstream sink tools consume that JSONL and deliver it somewhere useful. No
producer owns a delivery system; that separation is deliberate and is what
makes the suite composable.

## Contents

- [Design Principles](#design-principles)
- [Tool Suite](#tool-suite)
- [Record Contract](#record-contract)
- [evm-stream](#evm-stream)
- [evm-balance](#evm-balance)
- [Configuration](#configuration)
- [Record Transport](#record-transport)
- [RPC Transport Security](#rpc-transport-security)
- [Secret Handling](#secret-handling)
- [RPC Health Checks](#rpc-health-checks)
- [Metrics](#metrics)
- [Implementation](#implementation)
- [Build Milestones](#build-milestones)
- [Quality and CI](#quality-and-ci)
- [Release and Distribution](#release-and-distribution)
- [Naming Conventions](#naming-conventions)
- [Governance](#governance)
- [License](#license)
- [Operational Notes and Known Limitations](#operational-notes-and-known-limitations)
- [Open Questions](#open-questions)

## Design Principles

These principles apply to every tool in the repo.

1. **JSONL is the contract.** Every long-running tool writes one
   complete JSON object per line, and each line is written atomically so records
   from concurrent workers never interleave. JSONL keeps the records easy to
   tail, archive, inspect with `jq`, and forward to another process.
2. **Stdout is for logs; records travel over the transport.** The JSONL record
   stream never touches stdout — it flows over the record transport (a Unix
   socket). Logs are the process's normal output, split the conventional way:
   `debug`/`info`/`warn` on stdout, `error` on stderr. This is exactly what a
   container runtime expects, so logs land in `kubectl logs`/`docker logs`
   without any extra wiring.
3. **One job per tool.** Producers observe the chain. Sinks deliver data. A
   producer never embeds a Kafka client, a database driver, or a webhook
   sender; those are separate tools downstream of the pipe.
4. **Shared foundation, independent tools.** Config loading, mTLS RPC
   transport, the record contract, metrics, and chain helpers live in shared
   packages. Each tool is a thin entrypoint over that foundation.
5. **Operate from metrics alone.** Each tool can expose a Prometheus endpoint
   rich enough to debug a stuck or lagging process without reading logs.
6. **Lossless by default.** Producers never silently drop records. When a
   downstream sink stalls, backpressure propagates upstream (see
   [Output discipline](#output-discipline-and-backpressure)) rather than
   discarding data.
7. **Fail fast and clearly.** Misconfiguration — invalid TLS material, or a
   client certificate that `require_mtls` demands but that is absent — is
   reported with an explicit error before work starts, not midway through a run.

## Tool Suite

All tools live in this one repository as separate binaries under `cmd/`,
sharing the internal packages described in [Implementation](#implementation).
A single binary release and a single shared config file cover the whole suite.

| Tool | Role |
| --- | --- |
| `evm-stream` | Producer — live contract events and native ETH transfers |
| `evm-balance` | Producer — native/ERC-20/ERC-721 balance and contract-state polling |
| `evm-sink-kafka` | Sink — publish JSONL records to Kafka topics |
| `evm-sink-webhook` | Sink — forward records over HTTP with optional filters |
| `evm-sink-file` | Sink — append records to a rotating local file (gzip, retention) |
| `evm-sink-aws-sqs` | Sink — send records to an AWS SQS queue (FIFO-aware) |
| `evm-sink-aws-sns` | Sink — publish records to an AWS SNS topic (FIFO-aware) |
| `evm-sink-postgres` | Sink — idempotent insert into a PostgreSQL table |
| `evm-sink-redis` | Sink — append records to a Redis Stream (XADD, idempotent) |
| `evm-sink-stdout` | Sink — write records to stdout (the `\| jq`/piping hatch; logs to stderr) |

The pipeline shape is always the same: a producer emits JSONL records over a Unix
socket and a sink dials in to consume them (stdout carries logs, not records).

```sh
# Stream contract events straight into Kafka. --output socket listens on the
# well-known socket; the sink's --input defaults to it, so they auto-pair.
evm-stream run -c ~/.config/evm-tools/my-chain.toml --output socket &
evm-sink-kafka run --topic evm.events

# Poll balances and forward changes to an alerting webhook.
evm-balance run -c ~/.config/evm-tools/my-chain.toml --output socket &
evm-sink-webhook run --url https://hooks.internal.example.com/evm

# Or inspect records locally — evm-sink-stdout writes them to stdout for jq.
evm-stream run -c ~/.config/evm-tools/my-chain.toml --output socket &
evm-sink-stdout run | jq
```

Because the producers and sinks share the same monorepo and the same internal
`record` package, the JSONL contract stays in sync across the suite by
construction. Additional sinks (for example a SQL/columnar store or an
object-storage archiver) can be added later as new `cmd/` binaries without
changing the producers.

## Record Contract

Every tool reads and writes the same versioned record format. This is the
integration point for the entire suite, so it is defined explicitly rather than
left to each tool.

Each record line is a single JSON object with a common **envelope** plus a
type-specific **`data`** payload.

### Envelope fields

| Field | Type | Notes |
| --- | --- | --- |
| `schema_version` | integer | Contract version. Starts at `1`. Bumped only on a breaking change to the envelope or an existing type's payload. |
| `type` | string | Record type, e.g. `event`, `native_transfer`, `balance_sample`. Selects the `data` shape. |
| `tool` | string | Producing tool, e.g. `evm-stream`. |
| `name` | string | Name of the configured entry (stream contract, balance, token, ownership, or contract-state check) that produced the record. |
| `chain` | string | Chain label: the configured name (`--chain` / `[chain]`), or — when blank — one derived from the resolved chain id (e.g. `ethereum`, `base`, else `chain-<id>`). |
| `chain_id` | integer | Resolved EVM chain ID (guaranteed within the JSON safe-integer range). |
| `block_number` | integer | Source block number. |
| `block_hash` | string | Source block hash, for provenance; not part of the dedup key. |
| `tx_hash` | string | Transaction hash when the record is transaction-backed. |
| `log_index` | integer | Log index within the block for `event` records. |
| `trace_address` | array of integers | EVM call path of an `internal_transfer` record (e.g. `[0,2,1]`); the per-transfer identity that makes sibling internal transfers within one tx unique (the analogue of `log_index`). Omitted for every other record class. |
| `timestamp` | string | Block timestamp (RFC 3339) when available. |
| `emitted_at` | string | Wall-clock time the producer emitted the record (RFC 3339). Useful for latency and ordering at sinks. |
| `finalized` | boolean | Additive, best-effort (`omitempty`): `true` when the record's block is at or below the chain's finalized height at emit time (can no longer be reorged out); absent otherwise. |
| `data` | object | Type-specific payload (see [Record types and payloads](#record-types-and-payloads)). |

Fields that do not apply to a record type are omitted rather than emitted as
null.

### Deduplication and resume keys

Records carry no explicit `id`; a sink that needs a dedup or resume key derives
one from the envelope. The components differ by record class so the key is
actually unique and reorg-stable for each:

- **Transaction-backed log records** (`event`): `chain_id` + `tx_hash` +
  `log_index`. This is reorg-stable — the same log re-observed after a reorg
  yields the same key even though its `block_hash` changed, which is why
  `block_hash` is carried only for provenance and excluded from the key.
- **Native transfers** (`native_transfer`): `chain_id` + `tx_hash` (one
  top-level value transfer per transaction).
- **Internal transfers** (`internal_transfer`): `chain_id` + `tx_hash` +
  `trace_address`. A transaction can move ETH at many points in its call tree, so
  the EVM call path makes each one unique. It is reorg-stable for the same reason
  `log_index` is — it is a function of the transaction's execution, not of
  `block_hash` — so a re-included tx re-derives identical keys.
- **Sampled records** (`*_sample`): `chain_id` + `type` + `name` +
  `block_number`. Under interval cadence a single block can be sampled more than
  once, so `emitted_at` disambiguates; a sink that wants one row per block per
  entry can ignore it.
- **Change records** (`*_change`): `chain_id` + `type` + `name` +
  `block_number` (the block at which the new value was first observed).
- **Reorg markers** (`reorg`): `chain_id` + `type` + `block_number` (the orphaned
  tip) + `block_hash` (the canonical hash now at that height); a later re-reorg
  over the same range resolves to a different canonical hash and so a distinct
  key.

Head records are best-effort and non-final: `evm-stream` follows the chain head
and may emit records that a later reorg removes. The stream now *detects* reorgs
near the head and emits a [`reorg` marker](#record-types-and-payloads) over the
orphaned range before re-scanning the new canonical chain (see [Operational Notes
and Known Limitations](#operational-notes-and-known-limitations)); a re-included
transaction dedups against its first emission because event/native dedup keys are
reorg-stable. Each record also carries an additive, best-effort **`finalized`**
field (`omitempty`, no version bump): `evm-stream` stamps `finalized: true` when a
record's block is at or below the chain's finalized height at emit time — it can
no longer be reorged out — and omits the field otherwise (a still-reorganizable
head record, a chain without a `finalized` tag, or a producer that does not track
finality). A reorg-sensitive sink can trust a `finalized` record unconditionally
and gate the rest on the `reorg` marker or a confirmation lag.

### Numeric encoding

All 256-bit and token-precision amounts in `data` are encoded as JSON
**strings** to preserve values beyond the 2^53 safe-integer range that
float64/JS parsers mangle. This covers every wei value, raw token unit, total
supply, count, and formatted decimal balance (`value`, `value_wei`, `balance`,
`balance_wei`, `balance_raw`, `previous_raw`, `total_supply_raw`, `count`, and
so on). Only small bounded integers are JSON numbers: `decimals`,
`window_blocks`, and the envelope counters `schema_version`, `chain_id`,
`block_number`, and `log_index`. `chain_id` is guaranteed to stay within the
JSON safe-integer range, so its bare-number encoding is safe to use in a key.

### Versioning rules

- Additive changes (new optional fields, new record types) do **not** bump
  `schema_version`. Consumers must ignore unknown fields.
- Removing or repurposing an existing field, or changing its type, **does** bump
  `schema_version`.
- A sink accepts the `schema_version` values it understands and rejects any
  higher (newer) version with a clear error rather than silently mishandling it.
  Because the integer is bumped only on breaking changes, a sink should likewise
  reject a lower (older) version it no longer supports rather than best-effort
  parse it.

### Discriminators

`type` is the top-level record discriminator and selects the `data` shape. Two
record classes carry a secondary discriminator inside `data`:

- `kind` (`native` | `erc20` | `erc721`) tags the asset class on `balance_*` and
  `ownership_*` records.
- `field` (`native_balance` | `token_total_supply` | `transfer_count`) tags
  which contract observation a `contract_*` record carries.

Stream records (`event`, `native_transfer`) carry neither. A consumer switches
on `type` first, then on `kind`/`field` where present.

### Record types and payloads

Across all payloads, addresses and hashes are `0x`-hex strings and amounts
follow the [numeric encoding](#numeric-encoding) rule (strings).

**`event`** (evm-stream) — a decoded contract log.

- `event` (string): configured event name.
- `signature` (string): canonical event signature, e.g.
  `Transfer(address,address,uint256)`.
- `contract` (string): emitting contract address.
- `params` (object): decoded event arguments keyed by ABI parameter name; all
  values are strings.

Envelope carries `tx_hash`, `log_index`, and `block_hash`.

**`native_transfer`** (evm-stream) — a successful top-level ETH transfer.

- `from` (string); `to` (string, omitted for contract creation); `value_wei`
  (string); `value` (string, ether-decimal).
- `contract_creation` (bool, optional): true when the transaction created a
  contract (`to` is null).

Envelope carries `tx_hash`; no `log_index`.

**`internal_transfer`** (evm-stream) — a native ETH value movement that happened
*inside* a transaction's execution (a value-bearing `CALL`/`CALLCODE`, an internal
`CREATE`/`CREATE2` endowment, or a `SELFDESTRUCT` sweep). These emit no log, so
only trace RPC surfaces them; they are opt-in behind
`[stream.native_transfers].include_internal` (see [evm-stream](#evm-stream)). One
transaction can produce many.

- `from` (string); `to` (string, the destination/beneficiary or the created
  contract address); `value_wei` (string); `value` (string, ether-decimal).
- `call_type` (string): the EVM frame type that moved the value — `call`,
  `callcode`, `create`, `create2`, or `selfdestruct`.
- `contract_creation` (bool, optional): true for an internal `CREATE`/`CREATE2`.

Envelope carries `tx_hash` and `trace_address` (the call path); no `log_index`.

**`reorg`** (evm-stream) — a chain reorganization the stream detected near the
head. It marks an orphaned block range so a sink can retract the records of
transactions that did not survive the reorg, before the stream re-scans the new
canonical chain.

- `fork_block` (number): highest still-canonical block (the common ancestor); the
  orphaned range begins at `fork_block + 1`.
- `from_block` / `to_block` (numbers): inclusive orphaned range.
- `depth` (number): orphaned block count (`to_block - from_block + 1`).
- `old_hash` (string): the block hash the stream had recorded at `to_block`.
- `new_hash` (string, optional): the canonical hash now at `to_block` (empty when
  the reorg shortened the chain past it).
- `depth_exceeded` (bool, optional): true when the reorg ran deeper than the
  tracked depth, so `fork_block` is the floor of the tracked window rather than a
  proven ancestor (records below `from_block` may also be affected).

Envelope carries `block_number` (= `to_block`) and `block_hash` (= `new_hash`).

**`balance_sample` / `balance_change`** (evm-balance) — an account/wallet
balance.

- `kind`: `native` | `erc20` | `erc721`; `address` (holder).
- native: `balance_wei`, `balance`.
- erc20: `token`, `balance_raw`, `balance`, `decimals`.
- erc721: `token`, `count` (number of tokens owned).
- `*_change` adds the prior value: `previous_wei` / `previous_raw` /
  `previous_count`.

**`ownership_sample` / `ownership_change`** (evm-balance) — ERC-721 ownership of
a specific token.

- `kind`: `erc721`; `token`; `token_id`; `owner`.
- `ownership_change` adds `previous_owner`.

**`contract_sample` / `contract_change`** (evm-balance) — contract state from
`[[balance.contracts]]`.

- `address` (contract); `field`:
  - `native_balance`: `balance_wei`, `balance`.
  - `token_total_supply`: `total_supply_raw`, `total_supply`, `decimals`.
  - `transfer_count`: `count`, `window_blocks`.
- `contract_change` adds the matching `previous_*`.

### Examples

```json
{"schema_version":1,"type":"event","tool":"evm-stream","name":"usdc","chain":"my-chain","chain_id":4242,"block_number":19000001,"block_hash":"0x...","tx_hash":"0x...","log_index":12,"timestamp":"2026-06-19T12:00:00Z","emitted_at":"2026-06-19T12:00:03Z","data":{"event":"Transfer","signature":"Transfer(address,address,uint256)","contract":"0x...","params":{"from":"0x...","to":"0x...","value":"1250000"}}}
{"schema_version":1,"type":"native_transfer","tool":"evm-stream","name":"native","chain":"my-chain","chain_id":4242,"block_number":19000002,"block_hash":"0x...","tx_hash":"0x...","timestamp":"2026-06-19T12:00:12Z","emitted_at":"2026-06-19T12:00:13Z","data":{"from":"0x...","to":"0x...","value_wei":"1250000000000000000","value":"1.25"}}
{"schema_version":1,"type":"balance_sample","tool":"evm-balance","name":"treasury-eth","chain":"my-chain","chain_id":4242,"block_number":19000050,"block_hash":"0x...","timestamp":"2026-06-19T12:05:00Z","emitted_at":"2026-06-19T12:05:01Z","data":{"kind":"native","address":"0x...","balance_wei":"4200000000000000000","balance":"4.2"}}
{"schema_version":1,"type":"balance_change","tool":"evm-balance","name":"treasury-usdc","chain":"my-chain","chain_id":4242,"block_number":19000061,"block_hash":"0x...","timestamp":"2026-06-19T12:06:00Z","emitted_at":"2026-06-19T12:06:01Z","data":{"kind":"erc20","token":"0x...","address":"0x...","previous_raw":"1000000","balance_raw":"2000000","balance":"2.0","decimals":6}}
{"schema_version":1,"type":"ownership_change","tool":"evm-balance","name":"special-token-owner","chain":"my-chain","chain_id":4242,"block_number":19000070,"block_hash":"0x...","timestamp":"2026-06-19T12:07:00Z","emitted_at":"2026-06-19T12:07:01Z","data":{"kind":"erc721","token":"0x...","token_id":"1234","previous_owner":"0x...","owner":"0x..."}}
{"schema_version":1,"type":"contract_sample","tool":"evm-balance","name":"usdc","chain":"my-chain","chain_id":4242,"block_number":19000080,"block_hash":"0x...","timestamp":"2026-06-19T12:08:00Z","emitted_at":"2026-06-19T12:08:01Z","data":{"address":"0x...","field":"token_total_supply","total_supply_raw":"50000000000000","total_supply":"50000000.0","decimals":6}}
```

The first version prefers stable, descriptive JSON field names over compact
output. Downstream tools can transform or compress records later. The internal
`record` package is the single source of truth for these types; producers
construct records through it, and any sink that needs to parse records can
depend on it directly.

### Output discipline and backpressure

All records are emitted through a single synchronized writer in the `record`
package, so each JSONL line (object plus trailing newline) is written as one
atomic operation. This matters because a producer runs many concurrent monitors
(per-contract watchers, per-account pollers) sharing one record output — without
serialization, writes larger than the OS pipe-atomic size would interleave and
corrupt the stream. The writer flushes after every line so a downstream `jq`,
`tail`, or sink sees each record promptly and `emitted_at` reflects the true
emit time, including the low-volume case (e.g. `evm-balance` on a 1-minute
interval).

Producers are lossless. When a downstream sink is slow or stalled the output
socket fills and the record write blocks; that backpressure propagates upstream
and throttles RPC reads rather than dropping records or buffering without bound. A
blocked writer is observable: `evm_stream_emit_blocked_seconds` reports how long
the current or last write has been blocked, and `/readyz` flips to not-ready
once a write has been blocked beyond a threshold, so a wedged producer is
distinguishable from one that is merely lagging.

## evm-stream

`evm-stream` is a long-running CLI for live EVM activity monitoring. It reads a
configuration file, watches configured chain activity, and emits each observed
record as JSONL over its configured output.

### Monitoring model

`evm-stream` follows **streamable** chain activity:

- Contract logs/events emitted by one or more configured EVM contracts
  (`type: event`).
- Native ETH transfers from one address to another (`type: native_transfer`).

**Event identification and decoding.** A log carries no event name — only
`topics` and `data`. `evm-stream` resolves each configured event name to its
canonical signature and `topic0` (the keccak-256 of that signature), matches
logs by `topic0`, and decodes `topics`/`data` into named `params` using the
event's ABI. Names resolve in this order: (1) a per-contract `abi`/`abi_file`
when provided; otherwise (2) built-in ABIs for the standard interfaces (ERC-20,
ERC-721, ERC-1155), so `events = ["Transfer", "Approval"]` works for standard
tokens with no extra config. A configured event name that resolves to no
signature — unknown, or an overloaded name with no disambiguating ABI — is a
fatal config error at startup. For anonymous events or contracts whose ABI is
unavailable, supply explicit `signature`/`topic0` entries.

**Native transfer detection.** Top-level transfers are read from block and
transaction data: each transaction with non-zero `value` whose receipt status
is success (`status == 1`) emits one `native_transfer`. Reverted transactions
carry value in the tx body but transfer nothing, so they are not emitted. A
contract-creation transaction (`to` is null) that carries value is emitted with
`to` omitted and `contract_creation: true`. By default every value-bearing
transaction is emitted; `[stream.native_transfers]` accepts an optional
`from`/`to` allowlist to scope the stream — without one, this is the full
per-block value-transfer firehose, which is high-volume on busy chains.

**Internal transfers (opt-in, trace-based).** ETH moved *inside* a transaction —
a value-bearing `CALL`, an internal `CREATE`/`CREATE2` endowment, or a
`SELFDESTRUCT` sweep — emits no log, so top-level scanning misses it (a contract
payout typically rides on a `value: 0` outer tx). Setting
`[stream.native_transfers].include_internal = true` (or `--include-internal`,
which requires `--native-transfers`) traces each block — reusing the block the
top-level path already fetched — and emits an `internal_transfer` per value-bearing
sub-call that passes the same `from`/`to` allowlist. Frames are gated like the
top-level path: a reverted frame (and its whole subtree) moved nothing and is
skipped, as is a fully-reverted transaction; `DELEGATECALL`/`STATICCALL`/`CALLCODE`
are excluded by type (they move no value to a third party).

Trace RPC is **provider-dependent** and unevenly exposed, so the stream cascades
through three backends on first use and caches whichever the node serves:
geth's block-level `debug_traceBlockByNumber` (callTracer, one call/block) → the
parity `trace_block` (Erigon/Nethermind/Besu/anvil, one call/block) → per-tx
`debug_traceTransaction` (one call/tx). It is capability-aware and never fails an
otherwise-healthy run: if a node serves *none* of them, internal detection
**self-disables** for the run (logged once, `evm_stream_internal_transfers_disabled`)
while top-level transfers and logs keep flowing; a persistent per-block trace
failure (an oversized response, a tracer crash) **skips that block's** internal
transfers after bounded retries (logged, counted in
`evm_stream_internal_trace_blocks_skipped_total`) rather than wedging the producer;
a transient error is retried losslessly. `evm-stream check rpc` probes the cascade
and reports `trace_supported` / `trace_backend` when `include_internal` is set, so
an operator learns at config time which backend (if any) the endpoint supports.

Native transfers belong in `evm-stream`, not `evm-balance`, because they are
streamable chain activity rather than sampled account state.

**Following the chain (transport).** `evm-stream` follows the chain by HTTP
polling over the same HTTPS+mTLS endpoint used for everything else; it does not
open WebSocket subscriptions, so a single `[rpc].url` suffices. On each tick
(every `stream.poll_interval`) it reads the head block number and queries the
new logs/blocks since the last processed block. When `from_block` is behind the
head it backfills with chunked `eth_getLogs` (chunk span
`stream.log_chunk_blocks`), then transitions to head-following once it catches
up — the next poll simply continues from the last processed block, so there is
no gap or duplicate at the boundary.

### Command shape

```sh
evm-stream run -c ~/.config/evm-tools/my-chain.toml
evm-stream validate -c ~/.config/evm-tools/my-chain.toml
evm-stream check rpc -c ~/.config/evm-tools/my-chain.toml
evm-stream version

# Config-free: drive evm-stream entirely from flags (no config file). Keep the
# endpoint's API key out of argv/history by exporting it and referencing $RPC_URL.
export RPC_URL="https://rpc.example/v2/<your-key>"
evm-stream run --rpc-url "${RPC_URL}" --chain ethereum --contract 0xToken --events Transfer
evm-stream run --rpc-url "${RPC_URL}" --native-transfers
# Backfill from a height and tune the poll cadence, still config-free:
evm-stream run --rpc-url "${RPC_URL}" --contract 0xToken --from-block 19000000 --poll-interval 1s
```

`evm-stream` can run with no config file at all: `--rpc-url` gives the endpoint
and `--contract` (repeatable) / `--events` (default `Transfer`, resolved against
the built-in standard ABIs) and/or `--native-transfers` give it something to
monitor; `--chain` sets the record/metric label (when omitted it is derived from
the resolved chain id — e.g. `ethereum`, `base` — and the chain id always comes
from RPC). `--from-block` (`"latest"` or a block number) and `--poll-interval`
override `stream.from_block` / `stream.poll_interval`, so backfill height and
head-poll cadence are reachable without a config file too. These flags merge on
top of a config file when both are present, so a file's contracts and a
`--contract` are additive. For custom ABIs, per-contract event sets, or the
balance poller, use the config file. The endpoint is taken verbatim from the
flag — the shell expands `${RPC_URL}`, not `evm-tools` (its own `${VAR}`
interpolation rewrites config-file values only; see "Value interpolation").

`validate` loads and checks the configuration — including mTLS material and
event/ABI resolution — and exits without connecting to monitor, which makes it
useful in CI and pre-deploy. `stream.from_block` gives the stream an explicit
starting point: a block number to replay inclusively from a known point, or
`"latest"` to monitor only new activity — strictly the blocks mined after
startup (it begins at head+1, so the head block that already existed when the
stream started is not re-emitted).

## evm-balance

`evm-balance` is a separate CLI for polling balances and contract state and
writing the results as JSONL. It covers native ETH balances, ERC-20 token
balances, ERC-721 balances and ownership, and contract state. Where `evm-stream`
follows logs, `evm-balance` samples state.

It emits regular samples for charting and change records when a polled value
moves. The record types are:

- `balance_sample` / `balance_change`: native ETH, ERC-20, and ERC-721
  (token-count) balances for configured accounts.
- `ownership_sample` / `ownership_change`: ERC-721 ownership of a configured
  token ID.
- `contract_sample` / `contract_change`: configured contract state — native
  contract balance, ERC token total supply, and transfer-count windows.

`*_sample` records are emitted every cadence tick; `*_change` records are
emitted only when the polled value moves. Sampling cadence is time-based
(`[balance].interval`, e.g. `"1m"`) or block-based (`[balance].every_blocks`,
e.g. `50`); set exactly one.

Like `evm-stream`, `evm-balance` can run with no config file: `--rpc-url` gives
the endpoint, `--native <address>` and `--erc20 <token>:<holder>` (both
repeatable) name the targets, `--interval` / `--every-blocks` set the cadence
(exactly one), and `--chain` sets the record/metric label (derived from the chain
id when omitted, as in `evm-stream`). These flags merge on top of a config file,
so flag targets add to configured ones. The ERC-721 and
contract-state targets stay config-file-only (the analog of `evm-stream` keeping
custom ABIs in the file).

```sh
# Config-free: sample one address's native + USDT balance every 30s.
export RPC_URL="https://rpc.example/v2/<your-key>"
evm-balance run --rpc-url "${RPC_URL}" --chain ethereum --interval 30s \
  --native 0xADDR \
  --erc20 0xdAC17F958D2ee523a2206206994597C13D831ec7:0xADDR --output socket &
evm-sink-stdout run | jq
```

**Token decimals.** To produce human-readable balances and the
`decimals`/`total_supply` fields, `evm-balance` calls each token's `decimals()`
once at startup and caches it for the run. `decimals()` is optional in ERC-20;
for tokens that omit it, set `decimals` explicitly on the
`[[balance.erc20]]`/contract entry, otherwise the tool emits only the raw value
and logs a stderr warning. ERC-721 records carry counts and ownership, not
decimals.

Both producers use the same `evm-tools` config file for the shared chain and
RPC transport details.

## Configuration

The tools share an `evm-tools` configuration namespace. Shared chain, RPC, and
metrics settings live at the top level; each tool owns a namespaced section.
`evm-stream` reads the shared settings plus `[stream]`; `evm-balance` reads the
shared settings plus `[balance]`. Unknown sections owned by another tool must
not prevent the current command from running.

Default config locations, searched in this directory order (the first directory
holding a config file wins):

- `~/.evm-tools/` — the primary home-directory location (e.g.
  `~/.evm-tools/config.toml`).
- `~/.config/evm-tools/` (or the OS user-config dir) for an XDG-style workstation
  config.
- `/etc/evm-tools/` for a host-level or container config.

In each directory the file stem `config` is preferred (so `config.toml`), and the
legacy `evm-tools.toml` is still accepted as a backward-compatible fallback;
`config.toml` wins when both are present in the same directory. Directory order
takes precedence over the filename, so a home/user config beats a host-level
`/etc` config regardless of which name each uses. The home directory is resolved
at startup, so a `HOME` set after the process starts (common in containers) is
honored. Every command also accepts `-c`/`--config` to point at an explicit file.

### Precedence

1. Command-line flags.
2. Environment variables.
3. TOML config file.
4. Built-in defaults.

Config is decoded into typed Go structs before work starts. Each CLI decodes
only the shared top-level keys plus its own namespaced subtree, so sibling-tool
sections are ignored rather than rejected and one shared file serves every tool.
Unknown keys *within* a tool's own section are a fatal error, so typos are
caught at startup instead of silently dropped.

### Example

```toml
chain = "my-chain"

[rpc]
url = "https://rpc.internal.example.com:8545"
client_cert = "/path/to/client.crt"
client_key = "/path/to/client.key"
ca_cert = "/path/to/ca.crt"
server_name = "rpc.internal.example.com"

[metrics]
enabled = false
path = "/metrics"

[stream]
from_block = "latest"
poll_interval = "2s"
log_chunk_blocks = 2000
reorg_depth = 64                    # max reorg (blocks) detected/rewound near head; 0 disables
# head_staleness_threshold = "90s"  # /readyz not-ready when the head stops advancing; unset disables
# checkpoint_file = "/var/lib/evm-tools/cursor.json"  # durable resume cursor; resumes gap-free on restart (overrides from_block)

[stream.metrics]
enabled = true
addr = ":9000"

[[stream.contracts]]
name = "usdc"
address = "0x..."
events = ["Transfer", "Approval"]   # resolved via the built-in ERC-20 ABI
# For a non-standard contract, point at its ABI (then `events` names resolve
# from it), or give explicit signatures:
# abi_file = "/etc/evm-tools/abis/myproto.json"
# signatures = { Settled = "Settled(address,uint256,bytes32)" }

[stream.native_transfers]
enabled = true
include_internal = false   # also emit internal transfers via trace RPC (debug_traceBlockByNumber);
                           # needs a trace-capable endpoint, else it self-disables for the run
# Optional allowlist; without it, every value-bearing tx is emitted. Applies to
# internal-transfer frame addresses too when include_internal is on.
# from = ["0x..."]
# to = ["0x..."]

[balance]
interval = "1m"
# Or sample on a block cadence instead of a time interval (set exactly one):
# every_blocks = 50
# max_concurrency = 8               # parallel per-target reads each tick (0 = default)
# target_timeout = "15s"           # per-target read bound so one slow target can't stall the cycle
# head_staleness_threshold = "5m"  # /readyz not-ready when the head stops advancing; unset disables

[balance.metrics]
enabled = true
addr = ":9001"

[[balance.native]]
name = "treasury-eth"
address = "0x..."

[[balance.erc20]]
name = "treasury-usdc"
token = "0x..."
address = "0x..."
# decimals = 6   # override for tokens that don't implement decimals()

[[balance.contracts]]
name = "usdc"
address = "0x..."
native_balance = true
token_supply = true
transfer_count_window_blocks = 1000

[[balance.erc721_balances]]
name = "vault-nft-count"
token = "0x..."
owner = "0x..."
mode = "balance_of"

[[balance.erc721_ownership]]
name = "special-token-owner"
token = "0x..."
token_id = "1234"
```

`[[balance.erc721_balances]]` (mode `balance_of`) emits `balance_sample` /
`balance_change` records with `kind: erc721` and a `count`;
`[[balance.erc721_ownership]]` emits `ownership_sample` / `ownership_change`.

The sinks read from the same shared file. They use the shared `[metrics]`/`[log]`
defaults and their own namespaced section — `[kafka]` for `evm-sink-kafka`,
`[webhook]` for `evm-sink-webhook`, `[file]` for `evm-sink-file`, `[aws_sqs]` /
`[aws_sns]` for the AWS sinks, `[postgres]` for `evm-sink-postgres`, and `[redis]`
for `evm-sink-redis` — and ignore the producer-only `[rpc]`, `[stream]`, and
`[balance]` sections. Each sources its secrets (the Kafka SASL password, the
webhook auth-header value, the Postgres DSN, the Redis URL) through the same
[value interpolation](#value-interpolation) / `_cmd` machinery as the producers,
so nothing secret lands in the file. The AWS sinks take no credentials in config
at all — the AWS SDK default chain (environment, shared config, IRSA/web identity,
or an instance role) supplies them.

```toml
# evm-sink-kafka — publish each stdin record to Kafka, at-least-once.
[kafka]
brokers = ["broker-1.internal:9093", "broker-2.internal:9093"]
topic = "evm.events"                      # default topic; --topic overrides
# Optional per-record-type topic routing; unmapped types use `topic`.
# topic_by_type = { native_transfer = "evm.transfers", balance_change = "evm.balances" }
partition_key = "identity"                # identity (default) | dedup | none
required_acks = "all"                     # only "all" — the at-least-once contract
delivery_mode = "at-least-once"           # at-least-once (default) | idempotent
                                          # idempotent: KIP-98 producer, suppresses
                                          # in-session retry dupes (NOT cross-run EOS)
# backoff_base = "500ms"
# backoff_max  = "30s"

[kafka.sasl]
mechanism = "scram-sha-512"               # plain | scram-sha-256 | scram-sha-512
username = "evm-tools"
# Secret: pulled at startup, never written to the file or logged. On a
# distroless/scratch image (no shell) use ${KAFKA_PASSWORD} interpolation or a
# mounted secret file instead of _cmd.
password_cmd = "vault read -field=password secret/evm-tools/kafka"

[kafka.tls]
enabled = true                            # SASL requires TLS (fail fast otherwise)
ca_cert = "/etc/evm-tools/certs/kafka-ca.crt"
# Optional mutual TLS to the broker and an SNI override:
# client_cert = "/etc/evm-tools/certs/kafka-client.crt"
# client_key  = "/etc/evm-tools/certs/kafka-client.key"
# server_name = "kafka.internal.example.com"

[kafka.metrics]
enabled = true
addr = ":9002"

# evm-sink-webhook — forward each stdin record over HTTP, at-least-once.
[webhook]
url = "https://hooks.internal.example.com/evm"   # --url overrides
method = "POST"                           # POST (default) | PUT | PATCH
headers = { X-Source = "evm-tools" }      # static, non-secret headers
# timeout = "10s"

[webhook.auth]
header = "Authorization"
# Secret: sourced like the Kafka password — never written to the file or logged.
value_cmd = "printf 'Bearer %s' \"$(vault read -field=token secret/evm-tools/webhook)\""

# Optional filters — a FORWARDER WITH OPTIONAL FILTERS, not a rule DSL. All
# configured filters must pass for a record to be forwarded.
[webhook.filters]
include_types = ["balance_change", "native_transfer"]
# exclude_names = ["noisy-token"]

# A single simple field condition on one named data field (eq | gt | lt).
[webhook.filters.field]
field = "balance"
op = "gt"
value = "1000"

[webhook.metrics]
enabled = true
addr = ":9003"

# evm-sink-file — append each stdin record to a rotating local file, at-least-once.
[file]
path = "/var/log/evm-tools/events.jsonl"  # --path overrides; parent dirs created
max_size_mb = 100                         # rotate at this size; 0 disables size rotation
rotation_interval = "24h"                 # also rotate at this age; "off"/"0" disables
max_backups = 7                           # retain this many rotated segments; 0 keeps all
compress = true                           # gzip rotated segments (events-<ts>.jsonl.gz)
fsync = false                             # fsync each line before advancing (durability vs throughput)
# backoff_base = "500ms"                  # blocking retry bounds on a full disk (ENOSPC/EDQUOT)
# backoff_max  = "30s"

# Optional filters — type/name allow/deny lists only (no field condition; use the
# webhook sink for that). All configured filters must pass for a record to be written.
[file.filters]
include_types = ["event", "native_transfer"]
# exclude_names = ["noisy-token"]

[file.metrics]
enabled = true
addr = ":9004"

# evm-sink-aws-sqs — send each stdin record to SQS. No credentials here: the AWS
# SDK default chain (env, shared config, IRSA, instance role) supplies them.
[aws_sqs]
queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/evm-events"
region = "us-east-1"                       # optional; SDK resolves it if unset
# endpoint_url = "http://localhost:4566"   # optional; LocalStack/VPC endpoint
# A ".fifo" queue_url auto-enables MessageGroupId (record partition identity) and
# MessageDeduplicationId (record dedup key, hashed to a FIFO-safe id).

[aws_sqs.metrics]
enabled = true
addr = ":9005"

# evm-sink-aws-sns — publish each stdin record to an SNS topic (same AWS settings).
[aws_sns]
topic_arn = "arn:aws:sns:us-east-1:123456789012:evm-events"

[aws_sns.metrics]
enabled = true
addr = ":9006"

# evm-sink-postgres — idempotent insert into a table (ON CONFLICT (dedup_key) DO
# NOTHING), so at-least-once delivery is effectively exactly-once in the table.
[postgres]
# DSN is a secret: source it via _cmd/${VAR}, never inline. Never logged.
dsn_cmd = "vault read -field=dsn secret/evm-tools/postgres"
table = "evm_records"                      # may be schema.table; validated
create_table = true                        # CREATE TABLE IF NOT EXISTS on startup

[postgres.metrics]
enabled = true
addr = ":9007"

# evm-sink-redis — append each stdin record to a Redis Stream (XADD),
# at-least-once and (with dedup on) effectively exactly-once in the stream.
[redis]
# URL is a secret (it may carry a password): source it via _cmd/${VAR}, never
# inline. Never logged; rediss:// enables TLS.
url_cmd = "vault read -field=url secret/evm-tools/redis"
stream = "evm.events"                      # --stream overrides; required
# field = "data"                           # stream-entry field carrying the record JSON
# max_len = 1000000                        # approximate XADD MAXLEN cap; 0 keeps all
# dedup = true                             # dedup-gated append keyed on dedup_key
# dedup_ttl = "24h"                        # marker lifetime; "0"/"off" = never expire

[redis.metrics]
enabled = true
addr = ":9008"
```

### evm-sink-kafka

`evm-sink-kafka` reads the suite's JSONL contract on stdin and publishes each
record to Kafka (via the pure-Go franz-go client) with **at-least-once** delivery
by default, or an opt-in **idempotent producer** (`delivery_mode`; see
[Open Questions](#open-questions) #2). It reads the shared `[metrics]`/`[log]`
keys plus its own `[kafka]` section and ignores the producer-only `[rpc]`,
`[stream]`, and `[balance]` sections.

- `brokers` (required) — the bootstrap broker list (`host:port`). `--brokers`
  (a comma-separated string) and `EVM_TOOLS_KAFKA_BROKERS` override it.
- `topic` (required unless `topic_by_type` covers every record) — the default
  destination topic; `--topic` overrides it.
- `topic_by_type` — an optional map from record `type` (`event`,
  `native_transfer`, `balance_sample`, …) to a topic that overrides `topic` for
  that type. An unmapped type falls back to `topic`; a record with neither a
  mapped nor a default topic is a fatal error, never a silent drop.
- `partition_key` — how the Kafka message key is derived: `identity` (default)
  keys on the record's [partition identity](#deduplication-and-resume-keys) so
  every record sharing a logical identity lands on one partition and per-key
  order is preserved; `dedup` keys on the full dedup key (identity plus the
  sample `emitted_at` disambiguator); `none` sends no key (round-robin, no
  ordering guarantee).
- `required_acks` — only `"all"` is accepted (the default): the broker must
  acknowledge every in-sync replica before a publish is confirmed, which is the
  at-least-once contract. Any other value fails fast at startup rather than
  silently weakening the guarantee.
- `delivery_mode` — `"at-least-once"` (default; `"plain"` is an alias) or
  `"idempotent"`. at-least-once may put a duplicate on the topic on a producer
  retry, which a consumer dedups on the record identity key — the suite's standard
  posture, matching a non-FIFO SQS queue or Redis with dedup off. `idempotent`
  enables the KIP-98 idempotent producer, which suppresses the producer's **own
  in-session** retry duplicates; it is session-scoped (a restart/replay re-publishes,
  so it is **not** cross-run exactly-once) and requires `acks=all` (kept in both
  modes). It needs the `IDEMPOTENT_WRITE` ACL on locked-down clusters.
- `backoff_base` / `backoff_max` / `batch_timeout` — optional tuning of the
  blocking retry backoff and the writer's batch-flush window; each is a duration
  string (`"500ms"`, `"30s"`) and falls back to a built-in default.

**`[kafka.sasl]`** (optional) — `mechanism` is `plain`, `scram-sha-256`, or
`scram-sha-512` (empty disables SASL); `username` is the principal. The
`password` is a secret: source it through the shared
[interpolation/`_cmd`](#value-interpolation) machinery (`password_cmd` or a
`${VAR}`) so it never lands in the file, and it is never logged. SASL requires
TLS — a mechanism set with TLS disabled fails fast rather than send the password
in cleartext.

**`[kafka.tls]`** (optional, required when SASL is set) — `enabled` turns TLS on
(it defaults on when a SASL mechanism is configured); `ca_cert` trusts a private
broker CA; `client_cert`/`client_key` present a client certificate for mutual
TLS; `server_name` overrides the SNI/verification name. `insecure_skip_verify`
is a deliberate, dev-only escape hatch.

**`[kafka.metrics]`** — the standard per-tool metrics endpoint (see
[Metrics](#metrics)); the sink binds `:9002` by default so it runs alongside the
producers' `:9000`/`:9001`. The set covers records consumed/published/failed, a
publish-duration histogram, and retry/backoff/blocked gauges, plus `/healthz`
and `/readyz` — the latter flips to not-ready while a publish has been blocked on
a failing broker beyond its threshold. Because an idle pipe makes no publish
attempts, an active broker probe (a metadata request every
`[kafka].readiness_probe_interval`, default `15s`; `0` disables) keeps `/readyz`
reflecting "is the broker reachable" even when no records are flowing. The
webhook sink works the same way: it starts optimistically ready and, when an
optional `[webhook].health_url` is set, actively GET-probes it on the same
cadence.

`evm-sink-kafka validate` decodes the config and validates the broker list, SASL
mechanism, and TLS material — building the publisher loads the keypair and CA
without any network I/O — so a bad config is caught before the sink connects.

### evm-sink-file

`evm-sink-file` reads the suite's JSONL contract on stdin and appends each record
to a rotating local file with **at-least-once** durability. It reads the shared
`[metrics]`/`[log]` keys plus its own `[file]` section and ignores the
producer-only `[rpc]`, `[stream]`, and `[balance]` sections. It earns its place
over a shell `> file` redirect by adding rotation, compression, retention,
filtering, and the suite's health/metrics surface.

- `path` (required) — the active output file; `--path` and `EVM_TOOLS_FILE_PATH`
  override it. Parent directories are created on startup. Each record's verbatim
  JSONL line is appended as a single write (a line is never torn) followed by a
  newline.
- `max_size_mb` — rotate the active file once it reaches this size (MiB). `0` (the
  default) disables size-based rotation. A single line larger than the limit is
  still written whole (to its own segment), never split.
- `rotation_interval` — also rotate once the active file reaches this age (a
  duration like `"24h"`); `""`/`"0"`/`"off"` disables time-based rotation. Age is
  measured from when the sink opened the active file, so a restart resets it.
- `max_backups` — retain at most this many rotated segments, pruning the oldest
  first. `0` (the default) keeps all of them.
- `compress` — gzip each rotated segment to `events-<timestamp>.jsonl.gz` (the
  active file is always plain text). Compression is best-effort: a failure keeps
  the uncompressed segment rather than losing data.
- `fsync` — flush each line to stable storage before the cursor advances, trading
  throughput for durability. Off by default (the OS flushes on its own schedule
  and on clean shutdown).
- `backoff_base` / `backoff_max` — the blocking retry-backoff bounds applied when
  a write fails on a **full disk** (`ENOSPC`/`EDQUOT`), which is treated as
  transient: the sink blocks and retries (propagating backpressure up the pipe)
  rather than dropping records. Any other write error is permanent and fails fast.

**`[file.filters]`** (optional) — type/name allow- and deny-lists
(`include_types`/`exclude_types`/`include_names`/`exclude_names`) narrow which
records are written; all configured filters must pass. The file sink has **no
field condition** (unlike the webhook sink) — keep the recorded stream complete
and filter downstream, or forward through `evm-sink-webhook` for a field filter.

**`[file.metrics]`** — the standard per-tool metrics endpoint (see
[Metrics](#metrics)); the sink binds `:9004` by default so it runs alongside the
other sinks' `:9002`/`:9003`. The set covers records consumed/filtered/written/
failed, a write-duration histogram, retry/backoff/blocked gauges, a rotations
counter, and the active file's current size, plus `/healthz` and `/readyz` — the
latter flips to not-ready while a write has been blocked on a failing disk beyond
its threshold. It starts optimistically writable (there is no active disk probe);
a failed write flips readiness.

`evm-sink-file validate` decodes the config and validates the path, rotation
settings, and filters without creating the file or directory (validate performs
no filesystem writes), so a bad config is caught before the sink runs.

### Value interpolation

Two mechanisms resolve dynamic values in the config file. Both run on
file-sourced values after the TOML is read but before it is decoded into typed
structs, so every consumer of the config sees only final, resolved values.

**Environment variable interpolation.** Any string value may reference
environment variables, expanded at load time:

- `${VAR}` — the value of `VAR`; a fatal error if `VAR` is unset.
- `${VAR:-default}` — the value of `VAR`, or `default` when `VAR` is unset.
- `$$` — a literal `$`.

```toml
[rpc]
url = "https://rpc.internal.example.com:8545?token=${RPC_TOKEN}"
client_key = "${SECRETS_DIR:-/etc/evm-tools/certs}/client.key"
```

This is distinct from environment *binding* in the [precedence](#precedence)
rules. Binding lets an env var override a whole config key; interpolation
expands `${VAR}` *inside* a value. The env prefix for binding is `EVM_TOOLS_`
(for example, `EVM_TOOLS_RPC_URL` overrides `rpc.url`). Nested keys require
explicit wiring: an env var binds to a dotted key like `rpc.url` only through a
key replacer (`.`→`_`) and/or per-key binds that the loader installs — it is not
automatic. Flag and env bindings are merged *after* interpolation/`_cmd` resolve
and win per the precedence rules, so a binding that overrides a key
short-circuits that key's `_cmd` (the command is not run for a value that is
being replaced).

**Command execution (`_cmd` keys).** Any string field `<field>` may instead be
sourced from a companion `<field>_cmd` key. The command runs via `sh -c`, and
its trimmed standard output becomes the field's value. This pulls secrets from a
manager such as Vault without writing them into the file:

```toml
[rpc]
# Instead of url = "...", fetch it at startup:
url_cmd = "vault read -field=url secret/evm-tools/rpc"
```

Rules:

- Command execution is **opt-in**. It is disabled unless `--allow-exec` is
  passed or `EVM_TOOLS_ALLOW_EXEC=1` is set. A `_cmd` key encountered while exec
  is disabled is a fatal config error, not a silent skip.
- Setting both `<field>` and `<field>_cmd` for the same field is an error.
- A non-zero exit is a fatal config error, with the command's stderr surfaced in
  the message.
- **Trust boundary.** Enabling exec grants shell execution as the tool's user to
  anyone who can write the config file. Keep the config file operator-owned and
  not group/world-writable, and only enable exec where that holds.
- **Injection.** Environment interpolation also applies to the command string
  (`url_cmd = "vault read -field=url ${VAULT_PATH}"`), and an interpolated value
  is spliced in *before* `sh -c` runs and is not shell-escaped — a hostile
  `${VAR}` such as `; rm -rf …` would be executed. Interpolate only trusted
  variables into a command; prefer a static path or mounted secret where
  possible.
- `_cmd` produces the field's literal value. For fields that are file paths —
  the mTLS `client_cert`, `client_key`, and `ca_cert` — prefer an interpolated
  path or a mounted secret file; reserve `_cmd` for inline values such as tokens
  or URLs.
- If no `sh` is present (e.g. a distroless/scratch image), a `_cmd` key is a
  fatal config error with a clear "shell not found" message; use env
  interpolation or a mounted secret file instead.

## Record Transport

stdout never carries records — it carries logs ([Logging](#logging)). A producer
emits its JSONL record stream over a **Unix-domain-socket transport**, and the two
ends auto-pair on a **well-known default socket**: `--output socket` makes the
producer listen on it, and a sink's `--input` **defaults** to it, so neither side
types a path. (`socket` resolves per-host to a Unix socket under `$XDG_RUNTIME_DIR`
or the per-user temp dir — see `transport.DefaultSpec`; on Windows it is a named
pipe.) With no `--output`, a producer is exporter-only — it just serves `/metrics`
and discards records. Run more than one pipeline on a host and you give each an
explicit `--output unix:/path` / `--input unix:/path`. A sink's `--input -`/`stdin`
reads stdin instead, useful for replaying a JSONL file
(`cat records.jsonl | evm-sink-file run --input -`).

The transport sits behind the unchanged `record` contract: producers take
`--output` and sinks take `--input` (top-level `[output]`/`[input]` config keys,
or `EVM_TOOLS_OUTPUT`/`EVM_TOOLS_INPUT`; the value may be `socket`, a specific
`unix:/path`, or — for input — `-`). It only swaps the `io.Writer`/`io.Reader`
that `record.Writer`/`record.Reader` wrap, so the JSONL bytes on the wire are
unchanged.

```sh
evm-stream  run … --output socket     # producer listens on the well-known socket
evm-sink-*  run …                     # sink's --input defaults to it; dials, reconnects
```

Behavior (`internal/transport`):

- **Producer side** listens and fans each record out to every connected consumer.
  A slow consumer applies lockstep backpressure (the lossless invariant — the
  producer waits rather than dropping); a consumer whose write fails is dropped
  without affecting the others; a consumer that connects mid-stream gets the live
  tail. `--block-until-consumer` (default on) makes the producer wait for the
  first consumer before emitting and block while none are connected, so a sink
  that starts shortly after the producer loses nothing; `=false` is
  fire-and-forget (drop with no consumer). A stale socket left by an unclean exit
  is removed on startup (a live one is left in place so a second instance fails
  fast); the socket file is removed on clean shutdown.
- **Sink side** dials with jittered backoff until the producer is listening and a
  line-oriented reader transparently reconnects on disconnect, never splicing a
  partial record across a reconnect. `ctx` cancellation unblocks a blocked read so
  shutdown is prompt. Because it reconnects, a `unix:` sink does **not** exit when
  the producer stops (unlike a closed pipe, which yields EOF and ends the sink) —
  it waits for the producer to return; stop it with a signal.
- **Security**: the socket is created mode `0600` inside a `0700` directory, so
  only the producer's user can connect — no listening port and no TLS for a local
  hand-off. On Linux the socket file's mode gates `connect()`; on macOS that is
  gated by directory traversal, which the `0700` parent directory covers.

What the socket transport deliberately does **not** provide is durability or
replay: a sink that connects late or reconnects after downtime receives the live
tail, not the records it missed. Durable, replay-from-offset fan-out remains the
job of a broker sink (`evm-sink-kafka` / `evm-sink-redis`), which persists a log
that any number of independent consumers read at their own pace.

**Platforms.** `unix:` is the Linux/macOS carrier. On Windows, use `pipe:` — a
named pipe (`pipe:evm-events` expands to `\\.\pipe\evm-events`) whose ACL is the
access control: an SDDL built at startup from the launching user's own SID grants
full access to that user (set as owner), plus SYSTEM and the local Administrators
group — the analogue of the Unix socket's `0600`. (The user's SID is bound
explicitly rather than via the dynamic OWNER-RIGHTS alias, which would widen to
the Administrators group under an elevated/service token.) Both backends share the
same fan-out writer and reconnecting reader; the `pipe:` backend is built behind
`//go:build windows` via `github.com/Microsoft/go-winio` (on non-Windows it
returns a clear "Windows only" error). The `socket` keyword and an empty
`--output` (exporter-only) are the portable defaults everywhere — `socket`
resolves to the platform backend (`unix:` or `pipe:`). The full test suite runs on a `windows-latest` CI job, so every tool
(including `evm-sink-file` rotation/compression) is exercised on Windows.

### evm-sink-stdout

`evm-sink-stdout` is the one sanctioned tool that writes records to stdout (it
logs to stderr) — the deliberate carve-out to the suite-wide "records never touch
stdout" invariant. It reads the record transport like any other sink and writes
each record's verbatim JSONL line back to stdout, restoring the Unix `| jq`
composability that the socket transport otherwise routes around. Because it speaks
the same `--input` defaults as every sink, it is also the default sink in the
Helm charts and k8s deployments, where it pairs with a producer so records surface
in `kubectl logs` when no broker/database sink is wired up.

## RPC Transport Security

RPC access uses TLS, and mTLS is supported but not mandatory. HTTPS endpoints
connect with ordinary server-authenticated TLS by default, so public providers
(Alchemy, Infura, a public node) work with no extra material. Configuring a
client certificate and key upgrades the connection to mutual TLS for private
endpoints that require client authentication; an optional custom CA bundle and
server-name override apply in either mode. This is shared transport configuration
used for normal runs, balance polling, event backfills, RPC health checks, and
any metrics collection that reaches out to RPC.

The mTLS options use the same concepts operators expect from tools like `curl`:
a client certificate, a client private key, and a custom CA bundle when the RPC
endpoint is signed by a private CA.

The RPC port is part of the endpoint. The config takes a full URL — scheme,
host, port, and any provider-required path — and the tools never guess a
chain-specific port. Because `evm-stream` follows the chain by HTTP polling
rather than WebSocket subscriptions, this one HTTPS endpoint serves both
backfill and live following.

```toml
# Public provider (server-authenticated TLS — no client cert needed):
[rpc]
url = "https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}"

# Private node requiring mutual TLS:
[rpc]
url = "https://rpc.internal.example.com:8545"
client_cert = "/path/to/client.crt"
client_key = "/path/to/client.key"
ca_cert = "/path/to/ca.crt"
server_name = "rpc.internal.example.com"
require_mtls = true            # fail fast if the client cert/key are absent
```

Flags:

- `--rpc-url`: full EVM RPC endpoint, including port when needed.
- `--rpc-client-cert`: path to the mTLS client certificate.
- `--rpc-client-key`: path to the mTLS client private key.
- `--rpc-ca-cert`: path to a custom CA certificate bundle.
- `--rpc-server-name`: optional TLS server name override.
- `--rpc-require-mtls`: require a client cert/key for HTTPS (off by default).

The names stay explicit instead of bare `--cert`/`--key`, because these apply to
the outbound RPC client connection and may not be the only TLS options the tools
eventually support.

The tools fail fast with a clear error when a configured client certificate or
key is unreadable, mismatched, or invalid, when only one half of the pair is
given, or when `require_mtls` is set on an HTTPS endpoint with no client material.
Plain HTTP is allowed for local development. Operators of private,
mTLS-fronted nodes set `require_mtls` (or `--rpc-require-mtls`) so a missing
client certificate is rejected rather than silently downgraded to server-auth TLS.

## Secret Handling

Secret material — mTLS private keys, CA bundles, and any token embedded in an
interpolated value (the canonical `rpc.url` example carries `?token=${RPC_TOKEN}`) —
must never reach logs, error messages, banners, or metrics. These are
cross-cutting rules every package honors:

- Any log line, error, or startup banner that names the RPC endpoint logs a
  **redacted** URL: scheme, host, port, and path only, with the query string and
  any userinfo stripped, so a token in `rpc.url` never reaches stderr or log
  aggregation.
- Values resolved from `${VAR}` or a `_cmd` key are never echoed back in
  diagnostics. mTLS cert/key errors report the file path and a generic reason,
  never file contents.
- Metric and label values are drawn only from the enumerated low-cardinality
  vocabulary in [Metrics](#metrics). The RPC URL, query strings, tokens, mTLS
  material, and raw RPC error text are never used as label values or metric
  names — RPC errors are reduced to the coarse `error_type` categories.
- On non-Windows hosts, the tools warn when `client_key` is group- or
  world-readable; broaden the mode deliberately only where a deployment requires
  it.

## RPC Health Checks

Each tool provides a one-shot RPC health check for deployment smoke tests, init
containers, scripts, and exec-style health probes. It is a Cobra subcommand
rather than a flag on `run`.

```sh
evm-stream check rpc -c ~/.config/evm-tools/my-chain.toml
evm-stream check rpc --rpc-url https://rpc.internal.example.com:8545
```

The check uses the same RPC transport configuration as normal operation,
including mTLS. It:

- Connects to the configured RPC endpoint.
- Performs a lightweight method such as `eth_chainId` or `eth_blockNumber`.
- Prints a short JSON status object to stdout.
- Exits `0` when the endpoint is reachable and responds, non-zero otherwise.

The long-running `run` command also serves HTTP health endpoints, independent of
whether metrics scraping is enabled: `/healthz` for process liveness (the
process is up and not in an unrecoverable state) and `/readyz` for readiness.
`/readyz` returns `200` only when the RPC endpoint is reachable, the record/output
writer is not blocked beyond its threshold, and `evm_stream_lag_blocks` is
within a configured bound; otherwise it returns `503` — so a producer wedged on
a stalled sink or far behind the head reads as not-ready. The one-shot
`check rpc` command remains useful before the stream starts and as an exec-style
probe.

## Metrics

Every CLI can expose a Prometheus HTTP endpoint for operators who want to scrape
runtime health and monitored EVM state. Metrics are disabled unless configured
or explicitly enabled by a flag.

All metrics live under one `evm_` namespace with subsystem grouping: cross-tool
chain/state observables are `evm_chain_*`, `evm_rpc_*`, `evm_account_*`,
`evm_contract_*`, and `evm_log_*` (carrying the `blockchain` chain-name and
`chain_id` labels), while each tool's own lifecycle and poll cycle stay under
`evm_stream_*` / `evm_balance_*` (e.g. `evm_stream_poll_success`).
This supersedes the retired blockchain-exporter: every observable it tracked is
covered here (chain head/finality, account/contract balances and token supply,
transfer-count windows, poll success/timestamp/duration, RPC and log-chunk
timings), and evm-tools goes beyond it with reorg detection, internal
trace-derived transfers, finalized-block tracking, per-event-type record counts,
and the sinks' delivery/dead-letter metrics.

The shared `[metrics]` section provides defaults; each tool overrides them in
its own namespace so `evm-stream` and `evm-balance` can run on the same host
with independent switches and ports.

Precedence:

1. Command-line flags.
2. Tool-specific config, such as `[stream.metrics]` or `[balance.metrics]`.
3. Shared `[metrics]` defaults.
4. Built-in defaults.

```toml
[metrics]
enabled = false
path = "/metrics"

[stream.metrics]
enabled = true
addr = ":9000"

[balance.metrics]
enabled = true
addr = ":9001"
```

```sh
evm-stream run -c ~/.config/evm-tools/my-chain.toml --metrics
evm-stream run -c ~/.config/evm-tools/my-chain.toml --metrics-addr :9000
evm-stream run -c ~/.config/evm-tools/my-chain.toml --metrics-path /metrics
evm-balance run -c ~/.config/evm-tools/my-chain.toml --metrics --metrics-addr :9001
```

Flags:

- `--metrics`: enable the metrics endpoint.
- `--metrics-addr`: set the bind address, such as `:9000`.
- `--metrics-path`: set the route, such as `/metrics`.

Metrics are driven by the same config the CLI uses for work — there is no second
metrics-only watch list. If a tool is configured to watch a contract event or an
account balance, the endpoint exposes progress and state for exactly that
configured entry. The watched set is hot-reloadable: a producer re-reads its
config on `SIGHUP` and applies the new contract/target set at the next poll (see
[Config reload](#operational-notes-and-known-limitations)), and the per-entry
series of a contract/target the reload removes are dropped so a stale value does
not linger. `evm_stream_config_reloads_total` / `evm_balance_config_reloads_total`
count successful reloads and `_config_reload_errors_total` count failed ones (a
bad reload keeps the running set).

`9000` is the default bind port for `evm-stream` and `9001` for `evm-balance`
when both run on the same host; operators can choose any bind address per CLI.

### Style and labels

Metrics inherit the operational conventions of the retired `blockchain-exporter`
project: Prometheus-compatible metrics, stable names, low-cardinality labels,
and enough chain/RPC/runtime visibility to debug from metrics alone. Metric
types follow Prometheus convention: counters end in `_total`, gauges carry no
suffix, and durations are histograms suffixed `_seconds`; the reserved
`_count`/`_sum`/`_bucket` suffixes are used only by histograms and summaries.

Address and name labels (`*_address`, `*_name`) are attached only to metrics
keyed by a configured entry, so cardinality is bounded by config size.
Per-transaction or per-counterparty identifiers — `tx_hash`, `log_index`, and
the `from`/`to` of an observed transfer — are never labels. The shared label
vocabulary stays close to the retired `blockchain-exporter`:

- `blockchain`: chain label — the configured chain name (such as `my-chain`), or,
  when `--chain` / `[chain]` is blank, one derived from the resolved chain id
  (e.g. `ethereum`, `base`, else `chain-<id>`).
- `chain_id`: resolved EVM chain ID, or `unknown` before it is available.
- `operation`: RPC operation name, such as `eth_chainId`, `eth_blockNumber`,
  `eth_getLogs`, or `eth_getBlockByNumber`.
- `error_type`: coarse error category, such as `timeout`, `connection_error`,
  `rpc_error`, `decode_error`, or `unknown`.
- `contract_name`, `contract_address`: contract identity for contract-specific
  metrics.
- `account_name`, `account_address`: account identity for account-specific
  metrics.
- `token_name`, `token_address`, `token_decimals`: token identity for
  token-specific metrics.
- `event_name`: configured event name for event-specific stream metrics.

### Stream metrics

Process metrics:

- `evm_stream_up`: whether the stream process is available.
- `evm_stream_configured_contracts`: enabled stream contracts (gauge).
- `evm_stream_configured_native_transfers`: whether native transfer monitoring
  is enabled.
- `evm_stream_workers`: active stream workers/goroutines owned by monitors.

Chain health (mirroring `blockchain-exporter`):

- `evm_chain_head_block_number`: latest block number reported by RPC.
- `evm_chain_finalized_block_number`: finalized block number when the RPC
  endpoint supports it.
- `evm_chain_head_block_timestamp_seconds`: timestamp of the latest
  observed head block.
- `evm_chain_time_since_last_block_seconds`: wall-clock age of the latest
  head block.

Stream progress:

- `evm_stream_last_processed_block_number`: highest block processed.
- `evm_stream_last_emitted_block_number`: highest block that produced at least
  one emitted record.
- `evm_stream_lag_blocks`: difference between RPC head and last processed block.
- `evm_stream_emit_blocked_seconds`: time the current or last output write has
  been blocked by downstream backpressure.
- `evm_stream_records_emitted_total`: total JSONL records emitted.
- `evm_stream_event_records_emitted_total`: contract event records emitted.
- `evm_stream_contract_event_records_emitted_total`: contract event records by
  configured contract and event name.
- `evm_stream_native_transfer_records_emitted_total`: native transfer records
  emitted.
- `evm_stream_internal_transfer_records_emitted_total`: internal (trace-derived)
  native transfer records emitted (when `include_internal` is on).
- `evm_stream_internal_transfers_disabled`: `1` when internal-transfer detection
  self-disabled because the node serves no supported trace method, else `0`.
- `evm_stream_internal_trace_blocks_skipped_total`: blocks whose internal transfers
  were skipped after repeated trace failures (best-effort; the core stream advanced).
- `evm_stream_reorgs_detected_total`: detected chain reorganizations.
- `evm_stream_reconnects_total`: RPC reconnects after transport errors.

RPC and loop metrics:

- `evm_rpc_call_duration_seconds`: RPC call duration by chain, chain ID,
  operation.
- `evm_rpc_errors_total`: RPC errors by chain, chain ID, operation, error
  type.
- `evm_stream_poll_duration_seconds`: duration of each poll cycle.
- `evm_stream_poll_success`: whether the most recent poll cycle succeeded (1) or
  failed (0).
- `evm_stream_poll_timestamp_seconds`: Unix timestamp of the most recent
  successful poll cycle.
- `evm_stream_consecutive_failures`: current consecutive failure count.
- `evm_stream_backoff_duration_seconds`: retry backoff duration after failures.

Log query metrics (for chunked `eth_getLogs` backfill/replay):

- `evm_log_chunks_created_total`: log query chunks created.
- `evm_log_blocks_queried_per_chunk`: histogram of blocks covered per chunk.
- `evm_log_chunk_duration_seconds`: duration of each log chunk query.

### Balance metrics

`evm-balance` reuses the shared chain and RPC metrics, plus exporter-aligned
gauges emitted from the configured `[balance]` sections:

- `evm_account_balance_wei`.
- `evm_account_balance_eth`.
- `evm_account_token_balance_raw`.
- `evm_account_token_balance`.
- `evm_contract_balance_wei`.
- `evm_contract_balance_eth`.
- `evm_contract_token_total_supply`.
- `evm_contract_transfer_count_window`: transfers observed in the configured
  window, by contract; a `window_blocks` label carries the window width.

`evm-balance` also emits the shared poll-cycle metrics (`evm_balance_poll_duration_seconds`,
`evm_balance_poll_success`, `evm_balance_poll_timestamp_seconds`,
`evm_balance_consecutive_failures`, `evm_balance_backoff_duration_seconds`).
Where the blockchain-exporter put contract addresses behind an `is_contract` label
on a single account metric, evm-balance models accounts and contracts as separate
`evm_account_*` / `evm_contract_*` families, and exposes both raw and
decimals-applied token balances rather than carrying decimals as a label.

Contract metrics are first-class: configured contracts can expose native
contract balance, token total supply for ERC-compatible contracts, and transfer
count windows. The same configured entries that drive these metrics also drive
the `contract_sample`/`contract_change` records.

## Implementation

The CLIs are written in **Go 1.22+**: long-running commands, single-binary
distribution, good concurrency for watching many contracts or polling many
balances, and straightforward stdout streaming. The module path is
`github.com/daxchain-io/evm-tools`, so internal packages import as
`github.com/daxchain-io/evm-tools/internal/...`. The `go` directive in `go.mod`
pins the toolchain, and CI uses the same version via `go-version-file: go.mod`.

The tools use **Cobra** for command structure and flag handling, **Viper** for
configuration loading, and **TOML** as the primary config format. Cobra gives
each tool room to grow commands such as `run`, `check`, `validate`, and
`version`; Viper merges file config, environment variables, and explicit flags
into one runtime configuration. Config still decodes into typed structs before
work starts, so validation errors are explicit.

### Repository layout

```text
evm-tools/
  cmd/
    evm-stream/            # thin entrypoint
    evm-balance/           # thin entrypoint
    evm-sink-kafka/        # thin entrypoint
    evm-sink-webhook/      # thin entrypoint
    evm-sink-file/         # thin entrypoint
    evm-sink-aws-sqs/      # thin entrypoint
    evm-sink-aws-sns/      # thin entrypoint
    evm-sink-postgres/     # thin entrypoint
    evm-sink-redis/        # thin entrypoint
    evm-sink-stdout/       # thin entrypoint
  internal/
    config/                # shared loading, precedence, interpolation, per-tool decoding
    rpc/                   # TLS RPC transport + client (server-auth by default, optional mTLS)
    record/                # versioned JSONL envelope types + synchronized encoder/reader (the contract)
    metrics/               # Prometheus registry + HTTP server
    chain/                 # chain metadata + block helpers
    buildinfo/             # version/commit/date stamped via -ldflags
    cli/                   # shared Cobra command trees (producers + sinks)
    stream/                # evm-stream core logic
    balance/               # evm-balance core logic
    awssink/               # shared AWS SQS/SNS sink core
    pgsink/                # evm-sink-postgres core logic (pgx)
    redissink/             # evm-sink-redis core logic (go-redis; dedup-gated XADD)
    stdoutsink/            # evm-sink-stdout core logic (verbatim record line -> stdout)
    checkpoint/            # evm-stream durable resume cursor (atomic temp+fsync+rename)
    keyperm/               # shared private-key file-mode warning
    kafkasink/             # evm-sink-kafka core logic
    webhooksink/           # evm-sink-webhook core logic
    filesink/              # evm-sink-file core logic (rotating writer + filter + run loop)
  docs/
  .github/workflows/
```

The shared packages are the foundation that must land before the tools. The
`record` package is the single source of truth for the JSONL contract — both
producers and any sink that parses records depend on it.

### Logging

Logs use the standard library `log/slog` and are the process's normal output,
split by level: `debug`/`info`/`warn` go to **stdout** and `error` (and above)
go to **stderr** — the conventional Unix split. A `--log-level` flag
(`debug`/`info`/`warn`/`error`, default `info`) controls verbosity and
`--log-format` selects `text` (default) or `json`. Logging is configured once in
an internal package so every binary behaves identically. Per Principle 5,
metrics — not logs — are the primary operational surface.

#### Logging in containers

The JSONL record stream never shares stdout — it travels over the record
transport (a Unix socket; see [Record transport](#record-transport)) — so stdout
is free to carry logs the conventional 12-factor way. Docker and Kubernetes
capture *both* stdout and stderr, so `docker logs` and `kubectl logs` surface the
full log stream for free, with the level split preserved (stdout vs stderr). Set
`--log-format json` (or the `[log].format` key) when those diagnostics feed a log
aggregator such as Loki, Elasticsearch, or Cloud Logging, so each line parses as
structured JSON.

Records never touch stdout, so there is nothing to keep out of the log stream —
both stdout (info/warn) and stderr (error) are logs, and the runtime collects
them as the container's log stream. Wiring the record pipeline is independent of
logging:

- **Pipeline (producer → sink).** The producer listens on a socket
  (`--output socket`, or a `unix:/path` on a shared volume) and the sink dials it
  (`--input` defaults to that socket). Co-locate them in one pod — a sidecar
  container sharing an `emptyDir` for the socket, or both in one container — so
  the JSONL flows over the socket, never the log stream.
- **Standalone producer.** With no `--output` it is exporter-only: it just serves
  `/metrics` and logs, with no record stream at all.

One container caveat affects secret resolution: a distroless or `scratch` base
image has no shell, so config `_cmd` keys (which run via `sh -c`) fail with a
clear "shell not found" error there. In those images, source secrets through
environment-variable interpolation (`${VAR}`) or mounted secret files instead of
`_cmd`. A base image that ships a shell (for example `alpine`) keeps `_cmd`
working; see the suite `Dockerfile`, which uses such a base for that reason.

### Lifecycle and shutdown

The `run` commands derive a root context from `signal.NotifyContext` for
SIGINT/SIGTERM. On signal the producer stops accepting new work, finishes or
skips the in-flight line so a partial JSONL line is never emitted, flushes the
in-flight record write, and shuts down the metrics/health HTTP server cleanly. A bounded grace
period applies; a second signal forces exit. A clean shutdown exits `0`.

### Run-loop failure handling

Startup misconfiguration fails fast (Principle 7). At runtime, transient RPC
failures are retried with exponential backoff plus jitter (a base delay and a
capped maximum) rather than exiting; `evm_stream_consecutive_failures` and
`evm_stream_backoff_duration_seconds` expose the current state and
`evm_stream_reconnects_total` counts reconnects. The process does not
self-terminate on persistent RPC failure — it keeps retrying so an operator sees
a lagging-but-alive process — unless a fatal, non-retryable error occurs.

### Version stamping

The `version` command prints the semantic version, git commit, build date, and
Go version, and accepts `--json` for machine-readable output. These values are
stamped at build time via `-ldflags -X` into the internal `buildinfo` package,
which GoReleaser populates.

## Build Milestones

Build `evm-stream` and `evm-balance` together, keeping the first milestone
narrow: two working vertical slices that exercise the shared config, mTLS RPC
client, JSONL record output, and metrics server.

Initial `evm-stream` scope:

- Load shared `[rpc]` and `[metrics]` defaults plus `[stream]` and
  `[stream.metrics]` config.
- Connect to RPC with mTLS.
- Resolve chain ID and latest block.
- Match and decode configured contract events (built-in ERC ABIs) and emit them
  as JSON records, following the head by HTTP polling.
- Expose stream progress, RPC, log chunk, and record count metrics.

Initial `evm-balance` scope:

- Load shared `[rpc]` and `[metrics]` defaults plus `[balance]` and
  `[balance.metrics]` config.
- Connect to RPC with mTLS.
- Poll configured native balances, ERC-20 balances, and contract state.
- Emit balance and contract samples as JSON records.
- Expose account balance, token balance, contract balance, token supply, and RPC
  metrics.

Deferred until the shared spine is stable: native transfer streaming, ERC-721
ownership checks, config reload, checkpointing, richer event decoding, and the
downstream sinks (`evm-sink-kafka`, `evm-sink-webhook`). (Near-head reorg
detection landed in S7 — see [Operational Notes](#operational-notes-and-known-limitations).)

## Quality and CI

A tool is not complete until it is easy to verify, release, and install. The
repo runs two GitHub Actions workflows: a **CI** workflow (`ci.yml`) that gates
pull requests and pushes to `main`, and a **release** workflow (`release.yml`)
that publishes tagged versions (see
[Release and Distribution](#release-and-distribution)). Both pin the Go version
from `go.mod` (via `go-version-file`) so local, CI, and release builds match.

The CI baseline:

- `gofmt`/`go fmt` verification.
- `go mod tidy` verification.
- `go vet ./...`.
- `go test ./...`.
- `go build ./...`.
- `golangci-lint run` once the first Go packages exist.
- `govulncheck ./...` (a separate job) to fail on a known vulnerability in the
  module or its dependencies.
- Markdown linting (`markdownlint-cli2`) for README and docs.
- Shell linting (`shellcheck`) for installer scripts.
- `goreleaser check` to validate the release config, plus a
  `goreleaser release --snapshot --clean` dry-run so packaging breakage surfaces
  on pull requests rather than at tag time.

The first CI version can stay small but should be strict enough to catch local
formatting drift, broken docs, broken builds, and broken tests before release.

### Testing

The shared `record` package is covered by golden-file tests over its JSONL
output — envelope fields, RFC 3339 formatting, string-encoded amounts,
omitted-empty fields, and `schema_version` — so the contract cannot drift
silently. RPC-dependent code (mTLS transport, chunked `eth_getLogs`, the metrics
endpoint) is unit-tested against an in-process `httptest` server with generated
certs in the default `go test ./...` run; tests that need a real node
(`anvil`/`geth --dev`) live behind a build tag and run in a separate CI job. The
JSONL `record.Reader` — the parser for untrusted producer output — additionally
has a `go test -fuzz` target (`FuzzReaderNext`) whose seed corpus runs as
regression cases in the default test run.

## Release and Distribution

Tagged releases build cross-platform archives for at least:

- macOS arm64.
- macOS amd64.
- Linux arm64.
- Linux amd64.
- Windows arm64 (`.zip`).
- Windows amd64 (`.zip`).

Artifacts include the binaries, checksums, and the files installers need.
**GoReleaser** fits well: it builds the binaries, generates and signs the
checksums file (cosign), publishes GitHub releases, and updates the shared
Daxchain Homebrew tap from one workflow.

Homebrew publishes a single `evm-tools` cask to `daxchain-io/homebrew-tap` that
bundles all ten binaries, so one command installs the whole suite:

```sh
brew install --cask daxchain-io/tap/evm-tools
```

A universal installer supports a `curl | sh` workflow for hosts without
Homebrew. It detects the OS and CPU architecture, downloads the matching release
artifact, verifies the checksum, installs the requested binary, and fails
clearly on unsupported platforms:

```sh
curl -fsSL https://github.com/daxchain-io/evm-tools/releases/latest/download/install.sh | sh
```

The installer establishes trust in the checksum independently of the artifact:
the checksums file is signed keylessly with cosign in CI, and `install.sh`
verifies that signature with `cosign verify-blob` against a pinned signer
identity — the release workflow's certificate identity regexp and the GitHub
Actions OIDC issuer, both hard-coded in `install.sh` (the keyless equivalent of
a pinned public key). Only after the checksums file's signature verifies does
the installer compare the archive's SHA-256 against it, so a compromised release
channel cannot swap both the artifact and its checksum: forging the checksums
signature would also require this repo's GitHub Actions OIDC identity. If
`cosign` is not installed the installer fails closed with guidance to install
it; `EVM_TOOLS_SKIP_SIGNATURE=1` is an explicit, loudly warned opt-out that
downgrades to an unauthenticated same-channel SHA-256 check. Downloads are HTTPS
and fail closed. Because `curl | sh` runs `install.sh` before any verification,
a download-inspect-run alternative is documented for high-assurance
environments. The installer downloads the single bundle archive and installs all
ten binaries by default into the chosen directory; set `EVM_TOOLS_BIN` to a
single binary name to install just one.

### Release automation

Releases are fully automated. Pushing a semantic-version tag (`vX.Y.Z`) triggers
`.github/workflows/release.yml`, which checks out the repo, sets up the pinned Go
toolchain, and runs `goreleaser release --clean`. In that single run GoReleaser:

- builds the cross-platform matrix above and packages per-OS archives;
- generates `checksums.txt` and signs it with cosign;
- creates the GitHub Release with an auto-generated changelog and uploads the
  archives, checksums, and signature;
- publishes the universal `install.sh` as a release asset, reachable at the
  stable `releases/latest/download/install.sh` URL and regenerated each release
  so it resolves the matching versioned artifacts;
- updates the Homebrew tap — its `homebrew_casks` config renders the single
  `evm-tools` cask and commits it to `daxchain-io/homebrew-tap`, so `brew upgrade`
  sees the new version with no manual step.

Required workflow permissions and secrets:

- `permissions: { contents: write, id-token: write }` — to create the release
  and use keyless cosign signing via GitHub OIDC.
- `HOMEBREW_TAP_GITHUB_TOKEN` — a fine-grained token (or deploy key) with write
  access to `daxchain-io/homebrew-tap`; the workflow's default `GITHUB_TOKEN`
  cannot push to a different repository, so the tap update needs its own
  credential.
- cosign signing material — keyless OIDC is preferred; a stored
  `COSIGN_PRIVATE_KEY` + `COSIGN_PASSWORD` is the fallback when keyless is
  unavailable.

Adding a binary later (each new sink such as `evm-sink-kafka`) is a new
GoReleaser build target plus a tap formula — no new workflow. The
container-deployment path is covered by a companion workflow rather than a
GoReleaser `dockers` target (see below), so the two release artifacts stay
independent.

### Container images and Helm charts

On every `v*` tag a companion workflow,
`.github/workflows/publish-packages.yml`, publishes the container-deployment
artifacts to GHCR: ONE multi-arch (`linux/amd64`, `linux/arm64`) image,
`ghcr.io/daxchain-io/images/evm-tools`, and TWO Helm charts,
`ghcr.io/daxchain-io/charts/evm-stream` and
`ghcr.io/daxchain-io/charts/evm-balance`. It authenticates with the in-CI
`GITHUB_TOKEN` — both image and charts live in this org's GHCR, so no
cross-repository credential like the Homebrew tap's is needed.

The artifacts are **version-locked**: the chart `version`, the chart
`appVersion`, the image tag, and the release tag are all the same value. The
workflow stamps `--version`/`--app-version` from the tag at publish time (the
in-repo chart defaults are for local installs), so a deployed chart always pulls
the image built from the same source. A `workflow_dispatch` input republishes a
specific version on demand without re-tagging.

A second manual workflow, `.github/workflows/cleanup-packages.yml`
(`workflow_dispatch` only), prunes superseded GHCR package versions by exact tag;
it refuses to delete `latest`.

## Naming Conventions

- `evm-stream` — the primary behavior is long-running live monitoring. It keeps
  the Unix-style streaming/pipeline workflow while being clear about its purpose.
- `evm-balance` — singular and category-like, even though it can poll many
  accounts and tokens.
- `evm-tools` — the shared configuration namespace and repository name. One repo
  holds multiple binaries while operators maintain one shared config file.

New tools follow the same convention: an `evm-` prefix and a short, singular,
category-like name. Downstream sinks share an `evm-sink-<destination>` family so
they group together — `evm-sink-kafka`, `evm-sink-webhook` — keeping the
producer names (`evm-stream`, `evm-balance`) free to describe what they observe.

## Governance

This is an internally maintained Daxchain project. The repository is public so
the tools can be installed via Homebrew and the `curl` installer and so the code
is readable — not as an invitation to contribute. The repo is **closed to
outside contributions**:

- `CONTRIBUTING.md` states the policy: forks are welcome under Apache-2.0, but
  external pull requests are not reviewed or merged and public issues are not
  tracked.
- `SECURITY.md` defines **private** vulnerability reporting through GitHub
  security advisories (private vulnerability reporting is enabled on the repo);
  security issues are never filed as public issues or PRs.
- Write access is held only by Daxchain maintainers. The org's default member
  permission is read-only, and there are no outside collaborators, so a public
  fork is the most anyone outside the org can do.

## License

The repository is licensed under Apache-2.0; the full text is in the `LICENSE`
file at the repo root.

## Operational Notes and Known Limitations

Deployment notes and the constraints an enterprise sign-off should account for.

- **Single producer instance per chain (no built-in HA).** A producer is one poll
  loop with no leader election, so it does not self-coordinate with a second
  instance. Running two instances against the same chain/contracts double-emits
  records (event/native records dedup by the documented reorg-stable key, but
  balance `*_sample` rows differ by `emitted_at` and will not dedup). Run exactly
  one *active* producer per chain. Two HA patterns are supported with today's
  building blocks — see the runbook below.

  **Active/passive failover (recommended).** The durable resume cursor makes this
  safe and gap-free:

  1. Configure both the active and the standby identically, including the same
     `stream.checkpoint_file` on shared or replicated storage (an NFS/EFS mount, a
     replicated volume, or a path your orchestrator restores on failover).
  2. Run **exactly one** instance at a time. The checkpoint file is a resume
     cursor, **not a lock**, so mutual exclusion is the orchestrator's job: a
     systemd unit pinned to one node with failover, or a Kubernetes Deployment with
     `replicas: 1` and `strategy: Recreate` (which tears the old pod down before
     starting the new one). Do not run an active/active pair.
  3. On active failure the orchestrator starts the standby; it loads the shared
     cursor and resumes from `cursor+1`, re-emitting at most the boundary block
     (sinks dedup it). The only exposure is records produced during the failover
     window — bounded by how fast the standby starts, not lost once it does.

  **Work partitioning (no dedup needed).** Shard the watched set across instances
  with separate config files so each owns a **disjoint** slice — e.g. split
  `[[stream.contracts]]` (or `[balance]` targets) by contract/address across two
  producers. Because the slices do not overlap, there is no double-emit and no
  reliance on dedup; each shard still gets its own checkpoint for gap-free
  restart. This scales a busy chain horizontally but does not provide failover for
  a given shard (combine with active/passive per shard if you need both).

  Leader election (a lease/lock so exactly one instance self-selects as active)
  is a future enhancement; until then the orchestrator enforces single-active.
- **Restart coverage gap (closed by a checkpoint).** By default `stream.from_block`
  is `latest`, so a producer that restarts resumes at the current head and does not
  backfill blocks missed while it was down. Set `stream.checkpoint_file` to a
  durable path and the stream persists the highest processed block each poll
  (atomic temp-file + fsync + rename) and resumes from cursor+1 on restart —
  gap-free, re-emitting at most the boundary block (which sinks dedup via the
  reorg-stable dedup key, so the at-least-once contract holds). The cursor takes
  precedence over `from_block`, and a cursor whose `chain_id` does not match the
  configured chain is ignored (a guard against a reused path). It is the producer's
  own progress, not downstream confirmation, so a checkpoint plus a downstream sink
  that dedups gives end-to-end no-loss. Without a checkpoint, set `from_block` to a
  known-safe height or minimize downtime under a supervisor.
- **Config reload (log level/format + producer watched set).** `SIGHUP` makes a
  running tool re-read its config and live-apply the resolved `log.level`/`log.format`
  (e.g. bump to `debug` during an incident without a restart and without losing the
  resume position). On a **producer** it additionally hot-reloads the watched set:
  `evm-stream` re-resolves `[[stream.contracts]]` + native-transfer settings and
  `evm-balance` re-resolves its `[balance]` targets, applying the change at the next
  poll — added entries are watched from the current frontier forward (no historical
  backfill of a newly added entry), removed entries stop being watched and their
  per-entry metric series are dropped. The reload is staged on the signal handler
  and applied on the poll goroutine, so the watched state is swapped between polls,
  never mid-poll. A malformed reload is logged, counted
  (`*_config_reload_errors_total`), and the running set is kept; a successful one is
  counted (`*_config_reloads_total`). Connection-level and structural changes (RPC
  URL/TLS, chain, bind address, sampling cadence, sink destinations) are **not**
  hot-applied — change them in config and restart; with `checkpoint_file` set, a
  producer restart is gap-free.
- **Reorg handling: detect-and-re-scan, bounded depth, no finality wait.** Near the
  head the stream tracks the canonical hash of each confirmed tip (bounded to
  `stream.reorg_depth` blocks, default 64). When its processing frontier is
  orphaned it emits a [`reorg` marker](#record-types-and-payloads) over the orphaned
  range and re-scans the new canonical chain from the fork point; re-included
  transactions dedup against their first emission because event/native dedup keys
  are reorg-stable. Limitations remain: it does not *await* finality, so a record
  can still be emitted and then retracted by a later `reorg`; a reorg deeper than
  `reorg_depth` sets `depth_exceeded` and rewinds only to the tracked floor (records
  below `from_block` may be affected); and an in-place tip replacement is detected
  (the frontier is clamped to the head, so a reorg that does not advance the head is
  still caught), but blocks that were processed *above* the current head after the
  chain shortened are only re-scanned once the head climbs back. A reorg-sensitive
  sink should act on the `reorg` marker
  (retract the orphaned range), trust records stamped `finalized: true`, or run
  behind a confirmation lag; `reorg_depth = 0` disables the feature for chains
  where it is unneeded.
- **Head-staleness readiness.** Both producers expose `head_staleness_threshold`
  (unset/`0`/`off` disables it). When set, `/readyz` flips to not-ready once the
  latest chain head block ages past the threshold — catching a halted chain or a
  load balancer pinned to a frozen/lagging node even while RPC calls still succeed.
  The age is computed against the wall clock at probe time, so it also trips if the
  poll loop itself wedges. It is chain-agnostic and has no default; set it to
  roughly 5–10× the chain's block time to avoid flapping on normal block jitter.
- **Balance per-target parallelism.** `evm-balance` reads its targets in parallel
  each tick (bounded by `max_concurrency`, default 8) with an optional
  `target_timeout`, so the cycle's wall-clock is the slowest single target rather
  than the sum, and one hung target is bounded by the timeout instead of stalling
  the cycle. Reads run concurrently but emission and change detection stay
  sequential and deterministic, and a failed read aborts the whole tick before any
  record is emitted — so a tick is all-or-nothing and no change is silently
  swallowed. On rate-limited RPC, lower `max_concurrency` to avoid 429s.
- **Native-transfer cost.** Native-transfer detection fetches one receipt per
  value-bearing transaction (bounded-parallel, capped per block). On a high-volume
  chain with an unfiltered `[stream.native_transfers]` allowlist this is the
  dominant RPC cost; scope it with `from`/`to` allowlists and watch lag.
- **Metrics/health endpoint exposure.** The metrics/health server binds the
  configured address (default `:900x`, i.e. all interfaces) as plaintext HTTP with
  no authentication — appropriate for an in-cluster Prometheus scrape, but it
  exposes operational labels (chain, configured addresses, lag, failure counts).
  Restrict it with network policy / a private interface, or bind `127.0.0.1` when
  only a co-located scraper needs it. No secret values are ever metric labels.
- **Secret exposure surface.** Secrets sourced via `${VAR}` environment
  interpolation are readable for the process lifetime via `/proc/<pid>/environ`
  and would appear in a core dump; they are never logged. For hardened deployments
  prefer `_cmd`/file-based sourcing and disable core dumps.
- **Malformed input is fail-fast by default, with opt-in quarantine.** A sink
  treats any line it cannot parse (bad JSON, unsupported `schema_version`,
  trailing data) as a permanent error and exits non-zero — the stream is the
  contract, so it never silently skips a record. A single corrupt byte in a
  long-lived pipe therefore halts the sink; recovery is operator intervention.
  Setting `--dead-letter-file PATH` (or the top-level `dead_letter_file` config
  key / `EVM_TOOLS_DEAD_LETTER_FILE`) opts into a **dead-letter quarantine**: each
  poison line is appended to that file as a JSONL entry
  (`{quarantined_at, sink, error, record_base64}`, the original bytes preserved
  losslessly via base64), counted in `<sink>_records_quarantined_total`, and the
  sink continues. Nothing is dropped — the file *is* the record of it — so if the
  quarantine write itself fails the sink still halts. The feature is shared across
  every sink (it lives in the `record.Reader` quarantine hook), and fail-fast
  remains the default when no dead-letter file is configured.
- **Pipe lifecycle.** A producer ignores `SIGPIPE`, so a dead downstream sink
  surfaces as a terminal `EPIPE` (clean non-zero exit, graceful flush) rather than
  a signal kill. A second `SIGINT`/`SIGTERM` during graceful shutdown force-exits,
  so a wedged shutdown never requires `SIGKILL`.
- **Postgres sink startup and unstorable rows.** `evm-sink-postgres` validates at
  startup — it connects, checks the target table/columns, and confirms the
  `ON CONFLICT (dedup_key)` UNIQUE index/PRIMARY KEY exists (creating the table
  when `create_table=true`) — and **fails fast** if the database is unreachable or
  the table is misconfigured. This is intentional: a supervisor restart retries,
  and failing at boot beats failing on the first record. At run time, a record
  PostgreSQL genuinely cannot store — e.g. a JSON string containing `U+0000`,
  which `jsonb` rejects — is a permanent error that stops the sink (consistent with
  the fail-fast-on-malformed-input posture above), rather than being silently
  dropped or mutated; quarantine such input upstream if it can occur.
- **AWS sink credentials & retries.** The AWS sinks take no credentials in config
  — the SDK default chain supplies them. Retry classification delegates to the AWS
  SDK's own retryer (throttling, request timeouts, retryable 5xx, and connection
  errors are retried with blocking backoff), so only a genuine client fault
  (access denied, a non-existent queue/topic, a bad request) fails fast.
- **Redis sink idempotency is TTL-bounded.** `evm-sink-redis` is at-least-once;
  with `dedup` on (the default) an atomic dedup-gated `XADD` makes it effectively
  exactly-once *in the stream*. The connection URL is a secret (`_cmd`/`${VAR}`
  only, never a flag) and is never logged. Two caveats: dedup markers expire after
  `dedup_ttl` (default: never), so a duplicate arriving after the TTL — e.g. an
  overlapping re-run hours later — can re-append; setting no TTL makes dedup exact
  but grows marker-key memory unboundedly, so size it against your retention. And a
  `WRONGTYPE`/auth error fails fast (the stream key must be a stream and the
  credentials must permit `XADD`); transient states (`LOADING`, `CLUSTERDOWN`,
  network/timeouts) retry with blocking backoff.

## Open Questions

These are unresolved and worth deciding before or during the build:

1. ~~**Webhook sink scope and shape.**~~ **Settled (S2).** `evm-sink-webhook` is
   a FORWARDER with OPTIONAL FILTERS: it POSTs every record by default, with an
   optional include/exclude by record type and name plus a single simple field
   condition (`eq`/`gt`/`lt` on one named data field) — deliberately not a full
   rule DSL. Delivery is at-least-once (confirm-before-advance, blocking retry on
   transient errors, fail-fast on a permanent HTTP 4xx). See `[webhook]` config
   and the S2 milestone in [docs/plan.md](plan.md).
2. ~~**Sink delivery semantics.**~~ **Settled (S1).** Delivery responsibility
   lives in the sinks: each sink is **at-least-once** and the producers stay
   best-effort, with no producer-side checkpointing/resume for now.
   `evm-sink-kafka` publishes with `RequiredAcks=all` and confirms every publish
   before advancing the stdin cursor (confirm-before-advance), retrying a
   transient broker/network failure with blocking exponential backoff plus full
   jitter so backpressure propagates up the pipe — it never drops a record.
   Duplicates on retry are acceptable; consumers dedup via the record's
   [DedupKey](#deduplication-and-resume-keys), and per-key ordering is preserved
   by partitioning on the record's `PartitionIdentity`. `evm-sink-kafka` also
   offers an opt-in `delivery_mode = "idempotent"` (KIP-98 idempotent producer)
   that suppresses the producer's own in-session retry duplicates; it is
   session-scoped (not cross-run exactly-once — a restart re-publishes), so
   consumer-side dedup on the DedupKey remains the durable guarantee, and it is
   not the default precisely because it would over-promise versus the cross-run
   dedup the Postgres/Redis sinks give. See the `[kafka]` config and the
   [evm-sink-kafka](#evm-sink-kafka) subsection plus the S1 milestone in
   [docs/plan.md](plan.md). The same at-least-once posture governs
   `evm-sink-webhook` (Open Question 1).
3. **Internal native transfers.** *Resolved.* Trace-RPC-based internal transfer
   detection shipped behind the opt-in `[stream.native_transfers].include_internal`,
   emitting an `internal_transfer` record per value-bearing sub-call. Because trace
   RPC is provider-dependent and unevenly exposed, the stream cascades through three
   backends (`debug_traceBlockByNumber` → parity `trace_block` → per-tx
   `debug_traceTransaction`) and is capability-aware: a node serving none
   self-disables internal detection for the run, a persistent per-block trace
   failure skips that block rather than wedging, and `check rpc` reports trace
   support + the selected backend up front. See [evm-stream](#evm-stream).
4. **Finality signaling.** *Resolved.* Near-head reorg detection landed in S7
   (the `reorg` marker + canonical re-scan), and the additive, best-effort
   `finalized` envelope field now lets a sink distinguish final from
   still-reorganizable records without waiting on the `reorg` marker or a
   confirmation lag. Per-record `removed` tombstones were deliberately *not*
   added: the range-based `reorg` marker is the chosen retraction mechanism, since
   the producer does not buffer the full set of emitted records needed to tombstone
   them individually. Awaiting finality (vs. detect-and-retract) remains a
   deliberate non-goal for the low-latency default.
