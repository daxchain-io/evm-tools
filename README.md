# EVM Tools

Composable command-line tools for observing EVM chains and moving that data into
downstream systems. Each tool does one job and speaks one data contract —
newline-delimited JSON (JSONL) on stdout — so they pipe together cleanly:

```text
producer  →  JSONL on stdout  →  sink
```

A **producer** reads a chain over RPC and emits records. A **sink** reads those
records on stdin and delivers them somewhere, at-least-once. Any producer pipes
into any sink.

## The tools

### Producers — watch a chain, emit JSONL

| Tool | What it does |
| --- | --- |
| `evm-stream` | Live activity: decoded contract events and native ETH transfers. |
| `evm-balance` | Polls balances and contract state (native / ERC-20 / ERC-721). |

### Sinks — read JSONL, deliver it

| Tool | Delivers to | Notes |
| --- | --- | --- |
| `evm-sink-kafka` | Kafka topics | at-least-once; optional SASL/TLS |
| `evm-sink-webhook` | an HTTP endpoint | at-least-once; optional filters |
| `evm-sink-file` | a rotating local file | gzip + retention |
| `evm-sink-aws-sqs` | an AWS SQS queue | FIFO-aware; SDK-chain credentials |
| `evm-sink-aws-sns` | an AWS SNS topic | FIFO-aware |
| `evm-sink-postgres` | a PostgreSQL table | idempotent (`ON CONFLICT`) |
| `evm-sink-redis` | a Redis Stream (`XADD`) | idempotent via `dedup_key` |

All nine live in this repository, share one config file, and speak the same JSONL
contract.

## Install

One command installs the whole suite (all nine CLIs):

```sh
# Homebrew (macOS / Linux)
brew install --cask daxchain-io/tap/evm-tools

# Or without Homebrew — detects OS/arch, verifies a signed checksum, installs all nine:
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

2. Pipe a producer into a sink — or just inspect the stream:

   ```sh
   evm-stream run | evm-sink-file run
   evm-stream run | jq
   ```

**No config file?** `evm-stream` can run entirely from flags — point it at an RPC
endpoint and say what to watch. Keep the endpoint (it usually carries an API key)
out of your shell history by exporting it first, then referencing `$RPC_URL`. For
example, to stream live Tether (USDT) transfers on Ethereum mainnet:

```sh
export RPC_URL="https://eth-mainnet.example/v2/<your-key>"   # the secret lives here, not in the command

evm-stream run \
  --rpc-url "${RPC_URL}" \
  --chain ethereum \
  --contract 0xdAC17F958D2ee523a2206206994597C13D831ec7 \
  --events Transfer | jq
```

Each line is one decoded `Transfer` — `from`, `to`, and `value` in the token's
base units (USDT has 6 decimals). `--contract` is repeatable and `--events`
defaults to `Transfer`, so for a plain token you can drop `--events` entirely;
swap in `--native-transfers` to follow native ETH instead:

```sh
# Shorter (Transfer is the default), and the native-ETH variant:
evm-stream run --rpc-url "${RPC_URL}" \
  --chain ethereum --contract 0xdAC17F958D2ee523a2206206994597C13D831ec7 | jq

evm-stream run --rpc-url "${RPC_URL}" --native-transfers | jq
```

The shell expands `${RPC_URL}` before `evm-stream` sees it — use double quotes (or
no quotes), not single quotes. (`evm-tools`' own `${VAR}` interpolation applies to
config-**file** values; a value passed as a flag is taken verbatim.)

`--chain` sets the record/metric label (the chain id is always resolved from
RPC), event names resolve against the built-in ERC-20/721/1155 ABIs, and flags
merge on top of a config file when both are present. By default the stream starts
at the chain head (new blocks only); add `--from-block <number>` to backfill from
a specific height and `--poll-interval <dur>` to tune the head-poll cadence — so
backfilling needs no config file either:

```sh
# Backfill USDT transfers from block 19,000,000, polling every second:
evm-stream run --rpc-url "${RPC_URL}" --chain ethereum \
  --contract 0xdAC17F958D2ee523a2206206994597C13D831ec7 \
  --from-block 19000000 --poll-interval 1s | jq
```

> **stdout is data, stderr is diagnostics — never merge them.** `2>&1` would
> corrupt the JSONL. Keep the producer's stdout flowing straight into the sink.

## Pipelines

The shape is always producer → sink. A few combinations:

```sh
# Events straight into Kafka.
evm-stream run | evm-sink-kafka run

# Balance changes to an alerting webhook.
evm-balance run | evm-sink-webhook run

# Override a destination on the command line (no config edit).
evm-stream run | evm-sink-kafka --brokers broker:9093 --topic evm.events
evm-stream run | evm-sink-file --path /var/log/evm-tools/events.jsonl
```

Every command also takes `-c`/`--config` to point at an explicit config file
instead of the auto-discovered one.

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

Producers take a few extra knobs: `[stream].checkpoint_file` (a durable resume
cursor — restart resumes gap-free instead of jumping to the head), `reorg_depth`,
and `head_staleness_threshold`; `[balance]` has `max_concurrency` / `target_timeout`.
Sending a tool **`SIGHUP`** re-reads the config and live-applies `log.level` /
`log.format` (e.g. bump to `debug` during an incident without a restart); other
changes need a restart.

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

A multi-stage `Dockerfile` builds an `alpine`-based image with all nine binaries.
The base ships a shell on purpose so config `_cmd` keys keep working; a
distroless/scratch base has no shell, so use `${VAR}` interpolation or mounted
secrets there instead.

```sh
docker build -t evm-tools .
docker run --rm evm-tools evm-stream version
```

See [docs/design.md](docs/design.md) for the full product and implementation
notes.
