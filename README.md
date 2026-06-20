# EVM Tools

A suite of composable command-line tools for observing EVM chains and moving
that data into downstream systems. Each tool does one job and speaks a single
common data contract — newline-delimited JSON on standard output — so they pipe
together cleanly.

- `evm-stream` — live EVM activity monitoring (contract events and native ETH
  transfers) as JSONL.
- `evm-balance` — balance and contract-state polling as JSONL.
- `evm-sink-kafka` — publish each record to Kafka topics, at-least-once.
- `evm-sink-webhook` — forward each record over HTTP, at-least-once, with optional
  filters.

All four live in this repository and share one configuration namespace.

## Install

One command installs the whole suite (all four CLIs):

```sh
# Homebrew (macOS / Linux)
brew install daxchain-io/tap/evm-tools

# Or, without Homebrew — detects OS/arch, verifies a signed checksum, installs all four:
curl -fsSL https://github.com/daxchain-io/evm-tools/releases/latest/download/install.sh | sh
```

To install a single CLI via the script, set `EVM_TOOLS_BIN` (e.g.
`EVM_TOOLS_BIN=evm-stream`). The installer verifies the release's cosign-signed
checksums before installing — see
[docs/design.md](docs/design.md#release-and-distribution) for the trust model.

## Pipelines

The shape is always the same: a producer writes JSONL to stdout, and a sink
reads it from stdin.

```sh
# Stream contract events straight into Kafka.
evm-stream run -c ~/.config/evm-tools/my-chain.toml \
  | evm-sink-kafka run -c ~/.config/evm-tools/my-chain.toml

# Poll balances and forward changes to an alerting webhook.
evm-balance run -c ~/.config/evm-tools/my-chain.toml \
  | evm-sink-webhook run -c ~/.config/evm-tools/my-chain.toml

# Override the destination on the command line.
evm-stream run -c ~/.config/evm-tools/my-chain.toml \
  | evm-sink-kafka --brokers broker:9093 --topic evm.events

evm-balance run -c ~/.config/evm-tools/my-chain.toml \
  | evm-sink-webhook --url https://hooks.internal.example.com/evm

# Or just inspect the stream locally.
evm-stream run -c ~/.config/evm-tools/my-chain.toml | jq
```

stdout is the data stream and stderr is human-readable diagnostics, so the two
never mix — keep the producer's stdout flowing into the sink (or a file) and do
not merge stderr into it (`2>&1` would corrupt the JSONL).

## Configuration

Every tool reads one shared `evm-tools` config file. Producers read the shared
`[rpc]`/`[metrics]`/`[log]` settings plus their `[stream]`/`[balance]` section;
sinks read the shared `[metrics]`/`[log]` settings plus their `[kafka]` or
`[webhook]` section, and ignore the producer-only sections.

```toml
# evm-sink-kafka
[kafka]
brokers = ["broker:9093"]
topic = "evm.events"
required_acks = "all"          # only "all" — the at-least-once contract
readiness_probe_interval = "15s"  # active broker probe keeps /readyz live while idle; "0" disables

[kafka.sasl]
mechanism = "scram-sha-512"    # plain | scram-sha-256 | scram-sha-512
username = "evm-tools"
password_cmd = "vault read -field=password secret/evm-tools/kafka"

[kafka.tls]
enabled = true                 # SASL requires TLS

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

[webhook.filters.field]        # a single simple condition: eq | gt | lt
field = "balance"
op = "gt"
value = "1000"
```

Secrets (the Kafka SASL password, the webhook auth value) are sourced through
env interpolation (`${VAR}`) or a `_cmd` key, so they never live in the file or
the logs.

## Container image

A multi-stage `Dockerfile` builds an `alpine`-based image with all four binaries.
The base ships a shell on purpose so config `_cmd` keys keep working; a
distroless/scratch base has no shell, so use `${VAR}` interpolation or mounted
secrets there instead.

```sh
docker build -t evm-tools .
docker run --rm evm-tools evm-stream version
```

See [docs/design.md](docs/design.md) for the full product and implementation
notes.
