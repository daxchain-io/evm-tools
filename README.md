# Codex Chain EVM Tools

A suite of composable command-line tools for observing Codex Chain (and other
EVM chains) and moving that data into downstream systems. Each tool does one
job and speaks a single common data contract — newline-delimited JSON on
standard output — so they pipe together cleanly.

The first two tools are producers:

- `evm-stream`: live EVM activity monitoring (contract events and native ETH
  transfers) as newline-delimited JSON.
- `evm-balance`: balance and contract-state polling as newline-delimited JSON.

Downstream sink tools (e.g. `evm-sink-kafka`, `evm-sink-webhook`) consume that
stream and deliver it somewhere useful. All tools live in this repository and
share one configuration namespace.

See [docs/design.md](docs/design.md) for the current product and implementation
notes.
