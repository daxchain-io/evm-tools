# evm-tools Helm charts

Two charts, one per producer — each deploys the **cloud-native sidecar pattern**:
the producer and a sink container in one pod, connected by a Unix socket over a
shared `emptyDir`. Records travel over that socket; stdout/stderr carry only logs
(collected by the platform); `/metrics` is scraped per container.

| Chart | Deploys |
| --- | --- |
| [`evm-stream`](evm-stream) | `evm-stream` — live contract events + native transfers |
| [`evm-balance`](evm-balance) | `evm-balance` — native/ERC-20/ERC-721 balance + contract-state sampling |

They share the same shape, values, and guardrails; only the producer config
differs (`stream:` vs `balance:`).

## Install

The charts default to the published image `ghcr.io/daxchain-io/images/evm-tools` (built
and pushed by the release), so no local build is needed. For a custom image,
`docker build -f images/evm-tools/Dockerfile -t <ref> .`, push it, and add
`--set image.repository=<ref> --set image.tag=<tag>`.

```sh
# Provide the RPC endpoint. Preferred — an existing Secret:
kubectl create secret generic evm-rpc --from-literal=RPC_URL="$RPC_URL"
helm install eth-events charts/evm-stream --set rpc.existingSecret=evm-rpc

# Or, for a quick test, hand the chart the URL (it creates the Secret):
helm install eth-events charts/evm-stream --set rpc.url="$RPC_URL"

# Watch records flow (the default sink is evm-sink-stdout):
kubectl logs deploy/eth-events -c sink -f
```

`helm install eth-balances charts/evm-balance …` is identical.

## Key values

| Value | Default | Notes |
| --- | --- | --- |
| `image.repository` / `image.tag` | `evm-tools` / chart appVersion | the suite image |
| `rpc.existingSecret` / `rpc.url` | — | one is required; `existingSecret` preferred (the URL holds an API key) |
| `chain` | `ethereum` | record/metric label |
| `stream.*` / `balance.*` | example targets | the producer config (rendered into `evm-tools.toml`) |
| `sink.enabled` | `true` | `false` → producer runs **exporter-only** (just `/metrics`) |
| `sink.tool` | `evm-sink-stdout` | swap for `evm-sink-kafka` / `-postgres` / … + `sink.config` + `sink.extraArgs` |
| `metrics.enabled` / `metrics.port` | `true` / `9000` (`9001` balance) | producer `/metrics`+`/healthz`+`/readyz`; sink on `sink.metricsPort` (`9009`) |

## Guardrails built in

- **`replicaCount` must be 1** — a producer is a singleton per chain; a second
  active replica would double-emit. Run a separate release per chain.
- **One of `rpc.existingSecret` / `rpc.url` is required** — the chart fails to
  render otherwise.
- **Same UID** for both containers (`10001`) so the producer's `0600` socket is
  reachable by the sink; `--block-until-consumer` keeps it lossless and lets a
  dead sink flip `/readyz` not-ready (the kubelet restarts the pod).

For a no-Helm equivalent, see [`../deploy/kubernetes/evm-tools.yaml`](../deploy/kubernetes/evm-tools.yaml).
