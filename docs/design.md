# EVM Tools

This repository is a suite of composable command-line tools for observing EVM
chains and moving that data into downstream systems. The tools follow a
Unix-pipeline philosophy: each binary does one job
well, reads its settings from a shared configuration namespace, and speaks a
single common data contract — newline-delimited JSON (JSONL) on standard
output.

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
- [Open Questions](#open-questions)

## Design Principles

These principles apply to every tool in the repo.

1. **JSONL on stdout is the contract.** Every long-running tool writes one
   complete JSON object per line to standard output, and each line is written
   atomically so records from concurrent workers never interleave. JSONL keeps
   the tools easy to pipe, tail, archive, inspect with `jq`, and forward to
   another process.
2. **Stdout is for data; stderr is for humans.** Logs, warnings, progress
   messages, and diagnostics go to standard error so stdout stays
   machine-readable.
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
7. **Fail fast and clearly.** Misconfiguration — especially missing or invalid
   mTLS material — is reported with an explicit error before work starts, not
   midway through a run.

## Tool Suite

All tools live in this one repository as separate binaries under `cmd/`,
sharing the internal packages described in [Implementation](#implementation).
A single binary release and a single shared config file cover the whole suite.

| Tool | Role | Status |
| --- | --- | --- |
| `evm-stream` | Producer — live contract events and native ETH transfers | Building first |
| `evm-balance` | Producer — native/ERC-20/ERC-721 balance and contract-state polling | Building first |
| `evm-sink-kafka` | Sink — publish JSONL records to Kafka topics | Built (S1) |
| `evm-sink-webhook` | Sink — forward records over HTTP with optional filters | Built (S2) |

The pipeline shape is always the same: a producer writes JSONL to stdout, and a
sink reads it from stdin.

```sh
# Stream contract events straight into Kafka.
evm-stream run -c ~/.config/evm-tools/my-chain.toml \
  | evm-sink-kafka --topic evm.events

# Poll balances and forward changes to an alerting webhook.
evm-balance run -c ~/.config/evm-tools/my-chain.toml \
  | evm-sink-webhook --url https://hooks.internal.example.com/evm

# Or just inspect it locally.
evm-stream run -c ~/.config/evm-tools/my-chain.toml | jq
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

Each line of stdout is a single JSON object with a common **envelope** plus a
type-specific **`data`** payload.

### Envelope fields

| Field | Type | Notes |
| --- | --- | --- |
| `schema_version` | integer | Contract version. Starts at `1`. Bumped only on a breaking change to the envelope or an existing type's payload. |
| `type` | string | Record type, e.g. `event`, `native_transfer`, `balance_sample`. Selects the `data` shape. |
| `tool` | string | Producing tool, e.g. `evm-stream`. |
| `name` | string | Name of the configured entry (stream contract, balance, token, ownership, or contract-state check) that produced the record. |
| `chain` | string | Configured chain name, e.g. `my-chain`. |
| `chain_id` | integer | Resolved EVM chain ID (guaranteed within the JSON safe-integer range). |
| `block_number` | integer | Source block number. |
| `block_hash` | string | Source block hash, for provenance; not part of the dedup key. |
| `tx_hash` | string | Transaction hash when the record is transaction-backed. |
| `log_index` | integer | Log index within the block for `event` records. |
| `timestamp` | string | Block timestamp (RFC 3339) when available. |
| `emitted_at` | string | Wall-clock time the producer emitted the record (RFC 3339). Useful for latency and ordering at sinks. |
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
- **Sampled records** (`*_sample`): `chain_id` + `type` + `name` +
  `block_number`. Under interval cadence a single block can be sampled more than
  once, so `emitted_at` disambiguates; a sink that wants one row per block per
  entry can ignore it.
- **Change records** (`*_change`): `chain_id` + `type` + `name` +
  `block_number` (the block at which the new value was first observed).

Head records are best-effort and non-final: `evm-stream` follows the chain head
and may emit records that a later reorg removes (reorg handling is deferred —
see [Build Milestones](#build-milestones)). Schema version 1 carries no finality
or `removed` flag; sinks must treat head records as non-final. An optional
`finalized`/`removed` field may be added later as an additive change without a
version bump.

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
(per-contract watchers, per-account pollers) sharing one stdout — without
serialization, writes larger than the OS pipe-atomic size would interleave and
corrupt the stream. The writer flushes after every line so a downstream `jq`,
`tail`, or sink sees each record promptly and `emitted_at` reflects the true
emit time, including the low-volume case (e.g. `evm-balance` on a 1-minute
interval).

Producers are lossless. When a downstream sink is slow or stalled the OS pipe
fills and the stdout write blocks; that backpressure propagates upstream and
throttles RPC reads rather than dropping records or buffering without bound. A
blocked writer is observable: `evm_stream_emit_blocked_seconds` reports how long
the current or last write has been blocked, and `/readyz` flips to not-ready
once a write has been blocked beyond a threshold, so a wedged producer is
distinguishable from one that is merely lagging.

## evm-stream

`evm-stream` is a long-running CLI for live EVM activity monitoring. It reads a
configuration file, watches configured chain activity, and writes each observed
record to stdout as JSONL.

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
per-block value-transfer firehose, which is high-volume on busy chains. Internal
ETH transfers caused by contract calls require trace RPC support, so
`include_internal` is optional, provider-dependent, and out of the first
milestone.

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
```

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

Default config locations:

- `~/.config/evm-tools/` for a user-level workstation config.
- `/etc/evm-tools/` for a host-level or container config.

Every command also accepts `-c`/`--config` so scripts and deployments can point
at an explicit file.

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
include_internal = false
# Optional allowlist; without it, every value-bearing tx is emitted.
# from = ["0x..."]
# to = ["0x..."]

[balance]
interval = "1m"
# Or sample on a block cadence instead of a time interval (set exactly one):
# every_blocks = 50

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
`[webhook]` for `evm-sink-webhook` — and ignore the producer-only `[rpc]`,
`[stream]`, and `[balance]` sections. Both source their secrets (the Kafka SASL
password, the webhook auth-header value) through the same
[value interpolation](#value-interpolation) / `_cmd` machinery as the producers,
so nothing secret lands in the file.

```toml
# evm-sink-kafka — publish each stdin record to Kafka, at-least-once.
[kafka]
brokers = ["broker-1.internal:9093", "broker-2.internal:9093"]
topic = "evm.events"                      # default topic; --topic overrides
# Optional per-record-type topic routing; unmapped types use `topic`.
# topic_by_type = { native_transfer = "evm.transfers", balance_change = "evm.balances" }
partition_key = "identity"                # identity (default) | dedup | none
required_acks = "all"                     # only "all" — the at-least-once contract
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
```

### evm-sink-kafka

`evm-sink-kafka` reads the suite's JSONL contract on stdin and publishes each
record to Kafka with **at-least-once** delivery (see
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

## RPC Transport Security

RPC access requires mTLS. Every CLI in this repo uses an RPC transport that can
present a client certificate, use the matching client private key, and trust an
optional custom CA certificate bundle. This is shared transport configuration
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
[rpc]
url = "https://rpc.internal.example.com:8545"
client_cert = "/path/to/client.crt"
client_key = "/path/to/client.key"
ca_cert = "/path/to/ca.crt"
server_name = "rpc.internal.example.com"
```

Flags:

- `--rpc-url`: full EVM RPC endpoint, including port when needed.
- `--rpc-client-cert`: path to the mTLS client certificate.
- `--rpc-client-key`: path to the mTLS client private key.
- `--rpc-ca-cert`: path to a custom CA certificate bundle.
- `--rpc-server-name`: optional TLS server name override.

The names stay explicit instead of bare `--cert`/`--key`, because these apply to
the outbound RPC client connection and may not be the only TLS options the tools
eventually support.

The tools fail fast with a clear error when the configured RPC URL uses HTTPS
and the required mTLS files are missing, unreadable, mismatched, or invalid.
Plain HTTP is allowed for local development, but production EVM RPC
access is treated as HTTPS plus mTLS.

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
`/readyz` returns `200` only when the RPC endpoint is reachable, the stdout
writer is not blocked beyond its threshold, and `evm_stream_lag_blocks` is
within a configured bound; otherwise it returns `503` — so a producer wedged on
a stalled sink or far behind the head reads as not-ready. The one-shot
`check rpc` command remains useful before the stream starts and as an exec-style
probe.

## Metrics

Every CLI can expose a Prometheus HTTP endpoint for operators who want to scrape
runtime health and monitored EVM state. Metrics are disabled unless configured
or explicitly enabled by a flag.

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
configured entry. In the initial milestones config is loaded once at startup, so
the watched set is fixed for the process lifetime and changes take effect only
on restart. Once config reload lands (currently deferred), account and contract
metrics are removed or reset when a reload removes or disables the corresponding
entry.

`9000` is the default bind port for `evm-stream` and `9001` for `evm-balance`
when both run on the same host; operators can choose any bind address per CLI.

### Style and labels

Metrics follow the operational style of the parallel `blockchain-exporter`
project: Prometheus-compatible metrics, stable names, low-cardinality labels,
and enough chain/RPC/runtime visibility to debug from metrics alone. Metric
types follow Prometheus convention: counters end in `_total`, gauges carry no
suffix, and durations are histograms suffixed `_seconds`; the reserved
`_count`/`_sum`/`_bucket` suffixes are used only by histograms and summaries.

Address and name labels (`*_address`, `*_name`) are attached only to metrics
keyed by a configured entry, so cardinality is bounded by config size.
Per-transaction or per-counterparty identifiers — `tx_hash`, `log_index`, and
the `from`/`to` of an observed transfer — are never labels. The shared label
vocabulary stays close to `blockchain-exporter`:

- `blockchain`: configured chain name, such as `my-chain`.
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

- `blockchain_chain_head_block_number`: latest block number reported by RPC.
- `blockchain_chain_finalized_block_number`: finalized block number when the RPC
  endpoint supports it.
- `blockchain_chain_head_block_timestamp_seconds`: timestamp of the latest
  observed head block.
- `blockchain_chain_time_since_last_block_seconds`: wall-clock age of the latest
  head block.

Stream progress:

- `evm_stream_last_processed_block_number`: highest block processed.
- `evm_stream_last_emitted_block_number`: highest block that produced at least
  one emitted record.
- `evm_stream_lag_blocks`: difference between RPC head and last processed block.
- `evm_stream_emit_blocked_seconds`: time the current or last stdout write has
  been blocked by downstream backpressure.
- `evm_stream_records_emitted_total`: total JSONL records emitted.
- `evm_stream_event_records_emitted_total`: contract event records emitted.
- `evm_stream_contract_event_records_emitted_total`: contract event records by
  configured contract and event name.
- `evm_stream_native_transfer_records_emitted_total`: native transfer records
  emitted.
- `evm_stream_reorgs_detected_total`: detected chain reorganizations.
- `evm_stream_reconnects_total`: RPC reconnects after transport errors.

RPC and loop metrics:

- `blockchain_rpc_call_duration_seconds`: RPC call duration by chain, chain ID,
  operation.
- `blockchain_rpc_error_total`: RPC errors by chain, chain ID, operation, error
  type.
- `evm_stream_loop_duration_seconds`: duration of each poll loop.
- `evm_stream_consecutive_failures`: current consecutive failure count.
- `evm_stream_backoff_duration_seconds`: retry backoff duration after failures.

Log query metrics (for chunked `eth_getLogs` backfill/replay):

- `blockchain_log_chunks_created_total`: log query chunks created.
- `blockchain_log_chunk_blocks`: histogram of blocks covered per chunk.
- `blockchain_log_chunk_duration_seconds`: duration of each log chunk query.

### Balance metrics

`evm-balance` reuses the shared chain and RPC metrics, plus exporter-aligned
gauges emitted from the configured `[balance]` sections:

- `blockchain_account_balance_wei`.
- `blockchain_account_balance_eth`.
- `blockchain_account_token_balance_raw`.
- `blockchain_account_token_balance`.
- `blockchain_contract_balance_wei`.
- `blockchain_contract_balance_eth`.
- `blockchain_contract_token_total_supply`.
- `blockchain_contract_transfer_count`: transfers observed in the configured
  window, by contract.

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
    evm-sink-kafka/        # roadmap sink
    evm-sink-webhook/      # roadmap sink
  internal/
    config/                # shared loading, precedence, interpolation, per-tool decoding
    rpc/                   # mTLS RPC transport + client
    record/                # versioned JSONL envelope types + synchronized encoder (the contract)
    metrics/               # Prometheus registry + HTTP server
    chain/                 # chain metadata + block helpers
    buildinfo/             # version/commit/date stamped via -ldflags
    stream/                # evm-stream core logic
    balance/               # evm-balance core logic
  docs/
  .github/workflows/
```

The shared packages are the foundation that must land before the tools. The
`record` package is the single source of truth for the JSONL contract — both
producers and any sink that parses records depend on it.

### Logging

Human-readable diagnostics use the standard library `log/slog` on stderr. A
`--log-level` flag (`debug`/`info`/`warn`/`error`, default `info`) controls
verbosity and `--log-format` selects `text` (default) or `json`. Logging is
configured once in an internal package so every binary behaves identically. Per
Principle 5, metrics — not logs — are the primary operational surface.

#### Logging in containers

The stdout/stderr split (Principle 2) is exactly what a container runtime
expects, so it satisfies the 12-factor "logs as a stream" model without ever
putting logs on stdout. **stdout carries the JSONL data stream; stderr carries
the `log/slog` diagnostics.** Docker and Kubernetes capture *both* streams, so
`docker logs` and `kubectl logs` surface the stderr diagnostics for free — the
operator sees the human-readable log stream while the data contract on stdout
stays uncorrupted. Putting logs on stdout to "make them show up in
`kubectl logs`" would break the JSONL contract for any consumer of that stream;
it is unnecessary because the runtime already captures stderr. Set
`--log-format json` (or the `[log].format` key) when those diagnostics feed a
log aggregator such as Loki, Elasticsearch, or Cloud Logging, so each line
parses as structured JSON.

How you wire stdout depends on whether the container runs a producer alone or a
producer-to-sink pipeline:

- **Pipeline (producer → sink).** Run the producer's stdout into a sink — either
  both processes in one container connected by a shell pipe, or two containers
  with the producer's stdout redirected into the sink's stdin (e.g. a sidecar).
  The JSONL never reaches the container log stream; only the two processes'
  stderr diagnostics do.
- **Standalone producer.** stdout *is* the data. Redirect it to the next stage
  (a file, a named pipe, a sink container's stdin) rather than letting it land in
  the container log stream as undifferentiated lines. **Never merge stderr into
  stdout** — do not use `2>&1` or a shell redirect that folds the two together,
  because that interleaves human log lines into the JSONL and corrupts the data
  contract. Keep the streams separate and let the runtime collect stderr on its
  own.

One container caveat affects secret resolution: a distroless or `scratch` base
image has no shell, so config `_cmd` keys (which run via `sh -c`) fail with a
clear "shell not found" error there. In those images, source secrets through
environment-variable interpolation (`${VAR}`) or mounted secret files instead of
`_cmd`. A base image that ships a shell (for example `alpine`) keeps `_cmd`
working; see the suite `Dockerfile`, which uses such a base for that reason.

### Lifecycle and shutdown

The `run` commands derive a root context from `signal.NotifyContext` for
SIGINT/SIGTERM. On signal the producer stops accepting new work, finishes or
skips the in-flight line so a partial JSONL line is never emitted, flushes
stdout, and shuts down the metrics/health HTTP server cleanly. A bounded grace
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
client, JSON stdout output, and metrics server.

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
ownership checks, config reload, reorg handling, checkpointing, richer event
decoding, and the downstream sinks (`evm-sink-kafka`, `evm-sink-webhook`).

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
(`anvil`/`geth --dev`) live behind a build tag and run in a separate CI job.

## Release and Distribution

Tagged releases build cross-platform archives for at least:

- macOS arm64.
- macOS amd64.
- Linux arm64.
- Linux amd64.

Artifacts include the binaries, checksums, and the files installers need.
**GoReleaser** fits well: it builds the binaries, generates and signs the
checksums file (cosign), publishes GitHub releases, and updates the shared
Daxchain Homebrew tap from one workflow.

Homebrew publishes a single `evm-tools` cask to `daxchain-io/homebrew-tap` that
bundles all four binaries, so one command installs the whole suite:

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
four binaries by default into the chosen directory; set `EVM_TOOLS_BIN` to a
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
GoReleaser build target plus a tap formula — no new workflow. Container images
can be added as an additional GoReleaser target (`dockers`) if the
container-deployment path needs them.

## Naming Conventions

- `evm-stream` — the primary behavior is long-running live monitoring. It keeps
  the Unix-style stdout workflow while being clear about its purpose.
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
   by partitioning on the record's `PartitionIdentity`. See the `[kafka]` config
   and the [evm-sink-kafka](#evm-sink-kafka) configuration subsection plus the S1
   milestone in [docs/plan.md](plan.md). The same at-least-once posture governs
   `evm-sink-webhook` (Open Question 1).
3. **Internal native transfers.** Confirm when trace-RPC-based internal transfer
   detection (`include_internal`) is in scope, given it is provider-dependent
   and deferred from the first milestone.
4. **Finality signaling.** Once reorg handling lands, decide whether to add the
   additive `finalized`/`removed` envelope field (and `evm_stream` reorg
   re-emission) so sinks can distinguish final from reorganizable records.
