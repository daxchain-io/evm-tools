# EVM Tools

Composable command-line tools for observing EVM chains and moving that data into
downstream systems. Each tool does one job and speaks one data contract —
newline-delimited JSON (JSONL). Producers emit records over a Unix socket; sinks
dial in and deliver them. stdout carries logs, not records.

```text
producer  →  JSONL over a Unix socket  →  sink
```

A **producer** reads a chain over RPC, serves Prometheus metrics, and emits
records over its `--output` socket. A **sink** dials that socket with `--input`,
reads the records, and delivers them somewhere, at-least-once. Any producer
connects to any sink.

## The tools

### Producers — watch a chain, emit JSONL

| Tool | What it does |
| --- | --- |
| `evm-stream` | Live activity: decoded contract events and native ETH transfers. |
| `evm-balance` | Polls balances and contract state (native / ERC-20 / ERC-721). |

### Sinks — read JSONL, deliver it

| Tool | Delivers to | Notes |
| --- | --- | --- |
| `evm-sink-kafka` | Kafka topics | at-least-once or opt-in idempotent producer; SASL/TLS |
| `evm-sink-webhook` | an HTTP endpoint | at-least-once; optional filters |
| `evm-sink-file` | a rotating local file | gzip + retention |
| `evm-sink-aws-sqs` | an AWS SQS queue | FIFO-aware; SDK-chain credentials |
| `evm-sink-aws-sns` | an AWS SNS topic | FIFO-aware |
| `evm-sink-postgres` | a PostgreSQL table | idempotent (`ON CONFLICT`) |
| `evm-sink-redis` | a Redis Stream (`XADD`) | idempotent via `dedup_key` |
| `evm-sink-stdout` | stdout | the composability hatch — `… \| jq`, piping; logs to stderr |

All ten live in this repository, share one config file, and speak the same JSONL
contract.

## Install

One command installs the whole suite (all ten CLIs):

```sh
# Homebrew (macOS / Linux)
brew install --cask daxchain-io/tap/evm-tools

# Or without Homebrew — detects OS/arch, verifies a signed checksum, installs all ten:
curl -fsSL https://github.com/daxchain-io/evm-tools/releases/latest/download/install.sh | sh
```

To install just one CLI via the script, set `EVM_TOOLS_BIN` (e.g.
`EVM_TOOLS_BIN=evm-stream`). The installer verifies the release's cosign-signed
checksums before installing — see
[docs/design.md](docs/design.md#release-and-distribution) for the trust model.

## Quick start

1. Drop a config at `~/.evm-tools/config.toml` (it's auto-discovered, so no
   `-c` flag is needed):

   ```toml
   chain = "ethereum"

   [rpc]
   url = "https://my-rpc.example/v2/KEY"

   # evm-stream: which contract events to watch.
   [[stream.contracts]]
   name = "usdc"
   address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
   events = ["Transfer"]        # resolved from the built-in ERC-20 ABI

   # evm-sink-file: where to write them.
   [file]
   path = "/var/log/evm-tools/events.jsonl"
   ```

2. Connect a producer to a sink over a socket — or inspect the stream with
   `evm-sink-stdout`:

   ```sh
   # Records into the file sink. --output socket listens on the well-known socket;
   # the sink's --input defaults to it, so they auto-pair with no paths to type.
   evm-stream run --output socket &
   evm-sink-file run

   # Or just look: evm-sink-stdout prints records to stdout for jq.
   evm-stream run --output socket &
   evm-sink-stdout run | jq
   ```

**No config file?** `evm-stream` can run entirely from flags — point it at an RPC
endpoint and say what to watch. Keep the endpoint (it usually carries an API key)
out of your shell history by exporting it first, then referencing `$RPC_URL`. For
example, to stream live Tether (USDT) transfers on Ethereum mainnet:

```sh
export RPC_URL="https://eth-mainnet.example/v2/<your-key>"   # the secret lives here, not in the command

# The producer listens on the default socket; evm-sink-stdout prints records for jq.
evm-stream run \
  --rpc-url "${RPC_URL}" \
  --chain ethereum \
  --contract 0xdAC17F958D2ee523a2206206994597C13D831ec7 \
  --events Transfer --output socket &
evm-sink-stdout run | jq
```

Each line is one decoded `Transfer` — `from`, `to`, and `value` in the token's
base units (USDT has 6 decimals). `--contract` is repeatable and `--events`
defaults to `Transfer`, so for a plain token you can drop `--events` entirely;
swap in `--native-transfers` to follow native ETH instead (pipe each into
`evm-sink-stdout run | jq`, as above, to view):

```sh
# Shorter (Transfer is the default), and the native-ETH variant:
evm-stream run --rpc-url "${RPC_URL}" \
  --chain ethereum --contract 0xdAC17F958D2ee523a2206206994597C13D831ec7 --output socket

evm-stream run --rpc-url "${RPC_URL}" --native-transfers --output socket

# Also follow ETH moved *inside* transactions (router/multisig payouts, withdrawals,
# selfdestruct sweeps) — needs a trace-capable endpoint:
evm-stream run --rpc-url "${RPC_URL}" --native-transfers --include-internal --output socket
```

`--native-transfers` emits top-level ETH transfers. `--include-internal` adds
**internal transfers** — ETH moved during a transaction's execution, which emits no
log and is otherwise invisible — via trace RPC. It is opt-in and
**provider-dependent**, so the stream cascades through three trace backends
(`debug_traceBlockByNumber` → parity `trace_block` → `debug_traceTransaction`) and
uses whichever the node serves; on an endpoint that serves none it self-disables
for the run (top-level transfers and logs keep flowing) rather than failing, and
`evm-stream check rpc` reports `trace_supported` + the chosen `trace_backend` so
you know up front. Internal transfers arrive as a distinct `internal_transfer`
record carrying a `trace_address` call path. See the
[design doc](docs/design.md#evm-stream) for the gating and capability details.

The shell expands `${RPC_URL}` before `evm-stream` sees it — use double quotes (or
no quotes), not single quotes. (`evm-tools`' own `${VAR}` interpolation applies to
config-**file** values; a value passed as a flag is taken verbatim.)

`--chain` sets the record/metric label — omit it and it's derived from the
resolved chain id (e.g. `ethereum`, `base`), since the chain id always comes from
RPC; event names resolve against the built-in ERC-20/721/1155 ABIs, and flags
merge on top of a config file when both are present. By default the stream starts
at the chain head (new blocks only); add `--from-block <number>` to backfill from
a specific height and `--poll-interval <dur>` to tune the head-poll cadence — so
backfilling needs no config file either:

```sh
# Backfill USDT transfers from block 19,000,000, polling every second
# (view with `evm-sink-stdout run | jq`, as above):
evm-stream run --rpc-url "${RPC_URL}" --chain ethereum \
  --contract 0xdAC17F958D2ee523a2206206994597C13D831ec7 \
  --from-block 19000000 --poll-interval 1s --output socket
```

`evm-balance` is config-free too: name targets with `--native <address>` and
`--erc20 <token>:<holder>` (both repeatable) and set the cadence with `--interval`
or `--every-blocks`:

```sh
# Sample one address's native + USDT balance every 30s, no config file
# (view with `evm-sink-stdout run | jq`, as above):
evm-balance run --rpc-url "${RPC_URL}" --chain ethereum --interval 30s \
  --native 0xADDR \
  --erc20 0xdAC17F958D2ee523a2206206994597C13D831ec7:0xADDR --output socket
```

> **stdout carries logs; records travel over the socket.** Logs split by level
> (`debug`/`info`/`warn` → stdout, `error` → stderr); the JSONL record stream goes
> over the `--output`/`--input` Unix socket, never stdout — so there is nothing on
> stdout to corrupt.

## Pipelines

The shape is always producer → sink, connected by a Unix socket. The producer
opts into emitting with `--output socket` (it listens on a well-known per-host
path); a sink's `--input` **defaults** to that same socket, so they auto-pair with
no paths to type (either order — the sink retries until the producer listens):

```sh
# Events straight into Kafka.
evm-stream run --output socket &
evm-sink-kafka run

# Balance changes to an alerting webhook.
evm-balance run --output socket &
evm-sink-webhook run
```

Every command also takes `-c`/`--config` to point at an explicit config file
instead of the auto-discovered one. Output/input are also settable via
`[output]`/`[input]` in config or `EVM_TOOLS_OUTPUT`/`EVM_TOOLS_INPUT` (e.g.
`socket`, or a specific `unix:/path`).

### A fuller example — explicit, separate sockets

Run more than one pipeline on a host (the default socket is single-pipeline) and
give each an explicit `unix:` path. Producer and sink are **independent
processes** connecting directly:

```sh
# Start each on its own (any order — the sink retries until the producer listens):
evm-stream run --rpc-url "${RPC_URL}" --chain ethereum \
  --contract 0xdAC17F958D2ee523a2206206994597C13D831ec7 \
  --output unix:/run/evm/usdt.sock

evm-sink-file run --input unix:/run/evm/usdt.sock --path /var/log/evm-tools/usdt.jsonl
```

- **Same JSONL contract** travels over the socket; only the carrier changes. Also
  settable via `[output]`/`[input]` in config or `EVM_TOOLS_OUTPUT`/`EVM_TOOLS_INPUT`.
- **Lossless backpressure** is preserved (a slow sink throttles the producer).
- **Startup order doesn't matter**: by default the producer waits for a consumer
  before emitting (`--block-until-consumer`, on by default), so a sink that starts
  a little later loses nothing. Pass `--block-until-consumer=false` for
  fire-and-forget (drop when no consumer is connected).
- **Fan-out**: multiple sinks can connect to one producer's socket and each
  receives every record (the slowest gates the pace).
- **Resilient**: a `unix:` sink keeps running across producer restarts — on
  disconnect it waits and reconnects rather than exiting on EOF the way a closed
  pipe does. Stop it with Ctrl-C.
- **Owner-only**: the socket is created mode `0600` in a `0700` directory, so
  only the producer's user can connect — no port, no TLS for a local hand-off.
  (Linux gates connect on the socket's mode; macOS on directory traversal.)

What a socket **doesn't** do is replay: a sink that connects late or reconnects
after downtime gets the live tail, not history. When you need durable,
replay-from-the-beginning fan-out, use a broker sink (`evm-sink-kafka` /
`evm-sink-redis`) and read from its log.

**On Windows**, use a named pipe instead of `unix:` — same flags, `pipe:` scheme:

```text
evm-stream  run … --output pipe:evm-events        # → \\.\pipe\evm-events
evm-sink-file run … --input pipe:evm-events
```

The pipe's ACL is the access control — full access to the launching user (plus
SYSTEM and Administrators), the Windows analogue of the `0600` socket. An empty
`--output` (exporter-only) and stdin (`--input`, for replaying a JSONL file) work
on every platform.

### Poison records — dead-letter quarantine

By default a sink is **fail-fast**: a line it can't parse (bad JSON, unsupported
`schema_version`, trailing data) is a hard error and the sink exits non-zero — the
stream is the contract, so it never silently skips a record. To keep a long-lived
sink running past the occasional corrupt line, give it a **dead-letter file**:

```sh
evm-stream run --output socket &
evm-sink-kafka run --dead-letter-file /var/log/evm-tools/dead-letter.jsonl
```

Each poison line is appended there as one JSONL entry
(`{quarantined_at, sink, error, record_base64}` — the original bytes preserved
losslessly via base64), counted in `<sink>_records_quarantined_total`, and the
sink carries on. Nothing is dropped (the file *is* the record of it), so a failed
quarantine write still halts the sink. Recover a line with
`jq -r .record_base64 dead-letter.jsonl | base64 -d`. Also settable via the
top-level `dead_letter_file` config key or `EVM_TOOLS_DEAD_LETTER_FILE`; omit it
and fail-fast stays the default.

## Configuration

One shared config file serves every tool. The shared `[rpc]` / `[metrics]` /
`[log]` settings sit at the top level; each tool then reads its own section and
ignores the others:

| Tool | Config section |
| --- | --- |
| `evm-stream` | `[stream]` (+ `[[stream.contracts]]`) |
| `evm-balance` | `[balance]` (+ `[[balance.*]]` targets) |
| `evm-sink-kafka` | `[kafka]` |
| `evm-sink-webhook` | `[webhook]` |
| `evm-sink-file` | `[file]` |
| `evm-sink-aws-sqs` / `-sns` | `[aws_sqs]` / `[aws_sns]` |
| `evm-sink-postgres` | `[postgres]` |
| `evm-sink-redis` | `[redis]` |
| `evm-sink-stdout` | *(none — shared `[metrics]`/`[log]`/`[input]` only)* |

Producers take a few extra knobs: `[stream].checkpoint_file` (a durable resume
cursor — restart resumes gap-free instead of jumping to the head), `reorg_depth`,
and `head_staleness_threshold`; `[balance]` has `max_concurrency` / `target_timeout`.
Sending a tool **`SIGHUP`** re-reads the config and live-applies `log.level` /
`log.format` (e.g. bump to `debug` during an incident without a restart). On a
**producer** it also hot-reloads the watched set — `evm-stream`'s contracts and
`evm-balance`'s targets — applying adds/removes at the next poll and dropping the
metric series of removed entries (added entries are watched from the current point
forward, not backfilled). Connection-level and structural changes (RPC, chain,
cadence, sink destinations) still need a restart; with `checkpoint_file` set, a
producer restart is gap-free.

Without `-c`, the file is auto-discovered by checking these directories in order
(first match wins): `~/.evm-tools/`, then `~/.config/evm-tools/` (the OS
user-config dir), then `/etc/evm-tools/`. In each, `config.toml` is the primary
name and the legacy `evm-tools.toml` is still accepted as a fallback.

**Secrets** — the Kafka SASL password, the webhook auth value, the Postgres DSN,
the Redis URL — are sourced through `${VAR}` interpolation or a `_cmd` key, so
they never live in the config file or the logs.

The complete, commented options for every tool — each sink's full settings,
TLS/SASL, filters, and the producer tuning knobs (`reorg_depth`,
`head_staleness_threshold`, balance `max_concurrency` / `target_timeout`) — are
documented in [docs/design.md](docs/design.md#configuration).

<details>
<summary>Per-sink configuration reference (click to expand)</summary>

```toml
# evm-sink-kafka
[kafka]
brokers = ["broker:9093"]
topic = "evm.events"
required_acks = "all"             # only "all" — the at-least-once contract
delivery_mode = "at-least-once"   # at-least-once (default) | idempotent (KIP-98 producer, in-session dedup)
readiness_probe_interval = "15s"  # active broker probe keeps /readyz live while idle; "0" disables

[kafka.sasl]
mechanism = "scram-sha-512"       # plain | scram-sha-256 | scram-sha-512
username = "evm-tools"
password_cmd = "vault read -field=password secret/evm-tools/kafka"

[kafka.tls]
enabled = true                    # SASL requires TLS

# evm-sink-webhook
[webhook]
url = "https://hooks.internal.example.com/evm"
health_url = "https://hooks.internal.example.com/healthz"  # optional: active GET probe for /readyz
readiness_probe_interval = "15s"                           # probe cadence when health_url is set

[webhook.auth]
header = "Authorization"
value_cmd = "printf 'Bearer %s' \"$(vault read -field=token secret/evm-tools/webhook)\""

# Optional filters: forward all by default, narrow with these.
[webhook.filters]
include_types = ["balance_change", "native_transfer"]

[webhook.filters.field]           # a single simple condition: eq | gt | lt
field = "balance"
op = "gt"
value = "1000"

# evm-sink-file — append each record to a rotating local file.
[file]
path = "/var/log/evm-tools/events.jsonl"
max_size_mb = 100                 # rotate at this size; 0 disables size rotation
rotation_interval = "24h"         # also rotate at this age; "off" disables
max_backups = 7                   # keep this many rotated segments; 0 keeps all
compress = true                   # gzip rotated segments (.jsonl.gz)
fsync = false                     # fsync each line (durability vs throughput)

[file.filters]                    # type/name allow/deny lists (no field condition)
include_types = ["event", "native_transfer"]

# evm-sink-aws-sqs — send each record to SQS (credentials from the AWS default
# chain: env, shared config, IRSA/web identity, or instance role — never here).
[aws_sqs]
queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/evm-events"
region = "us-east-1"              # optional; SDK resolves from env if unset
# A .fifo queue_url auto-enables MessageGroupId/MessageDeduplicationId.

# evm-sink-aws-sns — publish each record to an SNS topic.
[aws_sns]
topic_arn = "arn:aws:sns:us-east-1:123456789012:evm-events"

# evm-sink-postgres — idempotent insert (ON CONFLICT (dedup_key) DO NOTHING).
[postgres]
dsn_cmd = "vault read -field=dsn secret/evm-tools/postgres"  # secret; never in the file
table = "evm_records"
create_table = true               # CREATE TABLE IF NOT EXISTS on startup

# evm-sink-redis — append to a Redis Stream (XADD), idempotent via dedup_key.
[redis]
url_cmd = "vault read -field=url secret/evm-tools/redis"  # secret (may carry a password); never in the file
stream = "evm.events"             # destination stream key
max_len = 1000000                 # approximate MAXLEN cap; 0 keeps all
dedup = true                      # dedup-gated append (effectively once-in-stream)
dedup_ttl = "24h"                 # marker lifetime; "0"/"off" = never expire
```

</details>

## Container image

A multi-stage `Dockerfile` builds an `alpine`-based image with all ten binaries.
The base ships a shell on purpose so config `_cmd` keys keep working; a
distroless/scratch base has no shell, so use `${VAR}` interpolation or mounted
secrets there instead.

```sh
docker build -t evm-tools .
docker run --rm evm-tools evm-stream version
```

See [docs/design.md](docs/design.md) for the full product and implementation
notes.
