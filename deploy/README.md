# Deploying observability for evm-tools

Ready-to-use Prometheus + Grafana assets for the suite. Every tool serves
`/metrics`, `/healthz`, and `/readyz` on its own port (defaults `:9000`–`:9008`);
enable the endpoint with `--metrics` or `[<tool>.metrics].enabled = true`.

| File | What it is |
| --- | --- |
| [`prometheus/prometheus.yml`](prometheus/prometheus.yml) | Example Prometheus config: one scrape job covering all ten tools, each target tagged with a `tool` label. |
| [`prometheus/rules.yml`](prometheus/rules.yml) | Recording rules + alerting rules (referenced by `prometheus.yml`). |
| [`grafana/evm-tools-dashboard.json`](grafana/evm-tools-dashboard.json) | Grafana dashboard (overview + producers + sinks). Import it and pick your Prometheus datasource. |

## Quick start

```sh
# Validate before shipping (CI does this too):
promtool check config deploy/prometheus/prometheus.yml
promtool check rules  deploy/prometheus/rules.yml

# Run Prometheus against the example config:
prometheus --config.file=deploy/prometheus/prometheus.yml
```

In Grafana: **Dashboards → New → Import**, upload `evm-tools-dashboard.json`, and
select your Prometheus datasource for the `Prometheus` variable.

The example uses `localhost:9000`–`:9008` static targets. In Kubernetes, drop the
static targets and scrape via pod annotations or a `ServiceMonitor`; keep a `tool`
label on each target so the dashboard and alerts group correctly.

### One logical endpoint per pod

The suite runs as separate processes — a `producer | sink` pipeline — so each tool
serves its own `/metrics` port; there is no single in-process endpoint to merge
them. Consolidation belongs at the scrape layer, not the binary:

- **A pod runs the whole pipeline.** Give each container/port its own scrape entry
  (or a `PodMonitor` with one `port` per container) and a shared `pod`/`instance`
  label plus the per-container `tool` label. Prometheus then treats the pod as one
  logical source — the dashboard and rules here already group by `tool` and
  `blockchain`, so a pod's producer and sink line up without any code change.
- **Avoid a merge sidecar.** A proxy that fans in the local `:900x` endpoints and
  re-serves one `/metrics` is possible but adds a process to run and keep in sync
  with the ports; the scrape-layer approach above achieves the same view with no
  extra moving parts. (This is why evm-tools keeps per-process endpoints rather
  than a built-in aggregator.)

## Metric groups

- **Common** (every tool): `go_*`, `process_*`, and `<tool>_build_info`
  (`version`/`commit`/`go_version`). Const labels `blockchain` and `chain_id` are
  on every series.
- **Chain / RPC** (producers): `evm_chain_head_block_number`,
  `evm_chain_finalized_block_number`,
  `evm_chain_time_since_last_block_seconds`,
  `evm_rpc_call_duration_seconds`, `evm_rpc_errors_total`.
- **evm-stream**: `evm_stream_lag_blocks`, `evm_stream_records_emitted_total`,
  `evm_stream_reorgs_detected_total`, `evm_stream_emit_blocked_seconds`, …
- **evm-balance**: `evm_balance_lag_blocks`,
  `evm_balance_records_emitted_total`, `evm_balance_*` gauges, …
- **Sinks**: `<sink>_records_consumed_total` plus a delivery counter
  (`…_records_published_total` / `_forwarded_total` / `_written_total` /
  `_records_delivered_total`), `…_records_failed_total`, a `…_blocked` gauge, and
  `…_consecutive_failures`.

## Runbook

Readiness (`/readyz`) is an HTTP check, not a scraped metric — alerts here are
metric-based. To page on `/readyz` directly, add the Prometheus blackbox exporter.

| Alert | Means | First response |
| --- | --- | --- |
| **EvmToolsTargetDown** | A tool's `/metrics` is unreachable for 2m (process down or crashed). | Check the pod/process and its stderr logs; a permanent error (bad config, unstorable record, dead downstream) exits non-zero by design — fix and restart. |
| **EvmChainHeadStale** | The head block's age has crossed the threshold (default 5m) — the chain or RPC endpoint stopped advancing. Age is computed live (`time() - head_block_timestamp`) so it still fires during an RPC outage. | Check the RPC endpoint (node synced? not load-balanced to a lagging peer?); mirrors the `head_staleness_threshold` `/readyz` check. |
| **EvmRpcErrorsElevated** | Sustained RPC errors by `operation`/`error_type`. | Inspect the provider (rate limits, auth, outage); check `evm_rpc_call_duration_seconds` for latency and the tool's backoff gauges. |
| **EvmStreamLagHigh** | evm-stream is >5000 blocks behind head for 10m. | Expected during a deep backfill; otherwise check RPC throughput and whether emission is blocked (next alert). |
| **EvmProducerEmitBlocked** | A producer's stdout has been blocked >30s — the downstream sink is applying backpressure. | Lossless by design (nothing dropped), but throughput is stalled: look at the *sink* it pipes into (see the next alert). |
| **EvmSinkDeliveryBlocked** | A sink has been retrying a failing destination for 5m. | Check the destination (broker/endpoint/DB/Redis/AWS) and the sink's `…_records_failed_total` by `error_type`; at-least-once means it's retrying, not dropping. |

Thresholds in `rules.yml` are conservative starting points — tune them to your
chain's block time and SLOs (e.g. lower `EvmChainHeadStale` for sub-second chains,
raise `EvmStreamLagHigh` if you intentionally replay history).
