# evm-tools on Kubernetes

[`evm-tools.yaml`](evm-tools.yaml) is the canonical cloud-native deployment: a
producer and a sink as **sidecar containers in one pod**, connected by a
Unix-domain socket over a shared `emptyDir`.

```text
┌───────────────────────── Pod ─────────────────────────┐
│  evm-stream            (emptyDir: /run/evm)      sink  │
│  --output unix:/run/evm/records.sock ──socket──> --input│
│  logs → stdout/stderr            logs → stdout/stderr  │
│  /metrics :9000                        /metrics :9009  │
└───────────────────────────────────────────────────────┘
        platform collects logs (kubectl logs) · Prometheus scrapes /metrics
```

Why it looks like this:

- **Records over the socket, never stdout.** stdout/stderr are logs, collected by
  the platform; the JSONL stream flows over the `emptyDir` socket between
  containers. Nothing pollutes the log stream.
- **Same UID.** Both containers run as UID `10001` (the image's user) so the
  producer's `0600` socket is reachable by the sink.
- **Lossless, self-healing.** `--block-until-consumer` (on by default) makes the
  producer wait for the sink before emitting; if the sink dies, the producer's
  emit blocks and `/readyz` flips not-ready, so the kubelet restarts the pod.
- **One producer per chain.** `replicas: 1` — a producer is a singleton (scaling
  it would double-emit). Run a second pod for a second chain.

## Deploy

```sh
# 1. The manifest pulls the published image ghcr.io/daxchain-io/evm-tools:2.1.0
#    by default — nothing to build. For a custom image, docker build -f
#    images/evm-tools/Dockerfile -t <ref> ., push it, and update the image: refs
#    in evm-tools.yaml (or for a local cluster: kind load / k3d image import).

# 2. Set your RPC endpoint in the Secret (it carries an API key — keep it here):
#    edit stringData.RPC_URL in evm-tools.yaml, or:
kubectl create secret generic evm-tools-rpc --from-literal=RPC_URL="https://…"

# 3. Apply and watch records flow:
kubectl apply -f deploy/kubernetes/evm-tools.yaml
kubectl rollout status deploy/evm-stream
kubectl logs deploy/evm-stream -c sink -f      # records (evm-sink-stdout)
kubectl logs deploy/evm-stream -c evm-stream -f # producer logs
```

## Adapt it

- **Production sink.** Swap the `sink` container's args for `evm-sink-kafka run`
  (or postgres/redis/webhook/file) plus its config; the socket wiring is
  unchanged. The records then leave the pod over the broker, not `kubectl logs`.
- **evm-balance.** Replace the producer with `evm-balance run` and a `[balance]`
  config for native/ERC-20/contract-state polling.
- **Scrape.** The pod carries `prometheus.io/scrape` annotations; the `Service`
  exposes both `:9000` (producer) and `:9009` (sink) for a `ServiceMonitor` or an
  annotation-based scrape. Metric names match the rest of the suite (`evm_*`).
