# evm-tools Helm charts

Two charts, one per producer — each deploys the **cloud-native sidecar pattern**:
the producer plus one or more sink containers in one pod, connected by a Unix
socket over a shared `emptyDir`. The socket **fans out**, so every sink receives
the full record stream. stdout/stderr carry only logs (collected by the platform);
each container serves its own `/metrics` (the producer on `metrics.port`, each sink
on its `metricsPort`), all exposed through the Service for a `ServiceMonitor`.

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

# Watch records flow (the default sink is evm-sink-stdout → container sink-stdout):
kubectl logs deploy/eth-events -c sink-stdout -f
```

`helm install eth-balances charts/evm-balance …` is identical.

## Key values

| Value | Default | Notes |
| --- | --- | --- |
| `image.repository` / `image.tag` | `ghcr.io/daxchain-io/images/evm-tools` / chart appVersion | the suite image |
| `rpc.existingSecret` / `rpc.url` | — | one is required; `existingSecret` preferred (the URL holds an API key) |
| `chain` | `ethereum` | record/metric label |
| `stream.*` / `balance.*` | example targets | the producer config (rendered into `evm-tools.toml`) |
| `sinks` | one `evm-sink-stdout` | list of sink sidecars (see **Sinks** below); empty list → producer runs **exporter-only** |
| `extraVolumes` | `[]` | pod volumes for sinks to mount (e.g. a file sink's output dir) |
| `metrics.enabled` / `metrics.port` | `true` / `9000` (`9001` balance) | producer `/metrics`+`/healthz`+`/readyz`; each sink on its own `metricsPort` (`9009`+) |

## Sinks

`sinks` is a list — each entry renders one sidecar container `sink-<name>` that
dials the shared socket. Because the socket fans out, every sink gets the full
stream, so you can run several at once — **including duplicates of the same tool**.
Each sink needs a unique `name` and `metricsPort`, and carries its **own config
and secrets**: `config`/`extraArgs` for settings (rendered to a per-sink
`sink-<name>.toml` when `config` is set), and `env`/`envFrom` for secrets (AWS
creds, a DB DSN, a Kafka password). Sinks consume records only — they don't use
RPC — so each is configured independently of the producer and of one another:

```yaml
extraVolumes:
  - name: archive
    persistentVolumeClaim: { claimName: evm-archive }
  - name: audit
    emptyDir: {}
sinks:
  - name: stdout                 # container sink-stdout
    tool: evm-sink-stdout
    metricsPort: 9009
  - name: archive                # a file sink → container sink-archive
    tool: evm-sink-file
    metricsPort: 9010
    config: |
      [file]
      path = "/data/records.jsonl"
      rotation_interval = "1h"
    volumeMounts:
      - { name: archive, mountPath: /data }
  - name: audit                  # a SECOND file sink → container sink-audit
    tool: evm-sink-file
    metricsPort: 9011
    config: |
      [file]
      path = "/audit/records.jsonl"
    volumeMounts:
      - { name: audit, mountPath: /audit }
  - name: sqs                    # an AWS sink with its OWN credentials
    tool: evm-sink-aws-sqs
    metricsPort: 9012
    extraArgs: ["--queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/evm"]
    env:                         # this sink's secrets — unrelated to RPC or other sinks
      - name: AWS_REGION
        value: us-east-1
      - name: AWS_ACCESS_KEY_ID
        valueFrom: { secretKeyRef: { name: aws-creds, key: access-key-id } }
      - name: AWS_SECRET_ACCESS_KEY
        valueFrom: { secretKeyRef: { name: aws-creds, key: secret-access-key } }
```

**Losslessness with multiple sinks:** the producer waits for the *first* consumer,
then keeps emitting while *any* sink is connected — so a sink that restarts while a
sibling stays up misses that gap. For durable per-sink delivery use a broker/DB
sink (Kafka/Postgres/Redis), or give that sink its own producer (release). With a
single sink the producer blocks when it drops, so nothing is lost.

## Guardrails built in

- **`replicaCount` must be 1** — a producer is a singleton per chain; a second
  active replica would double-emit. Run a separate release per chain.
- **One of `rpc.existingSecret` / `rpc.url` is required** — the chart fails to
  render otherwise.
- **Unique sink `name` and `metricsPort`** — render fails on duplicate names,
  duplicate ports, or a sink port that collides with the producer's `metrics.port`.
- **Same UID** for both producer and sinks (`10001`) so the producer's `0600`
  socket is reachable; `--block-until-consumer` keeps the first consumer lossless,
  and `/readyz` flips not-ready if every consumer drops (the producer is removed
  from Service endpoints until a sink reconnects).

For a no-Helm equivalent, see [`../deploy/kubernetes/evm-tools.yaml`](../deploy/kubernetes/evm-tools.yaml).
