# Multi-stage build for the evm-tools suite. The final image contains all four
# binaries (evm-stream, evm-balance, evm-sink-kafka, evm-sink-webhook) so one
# image serves every tool in a producer | sink pipeline.
#
# Base image choice: the runtime stage uses alpine — an image WITH a shell —
# on purpose. Config `_cmd` keys run via `sh -c` (see docs/design.md
# "Command execution (_cmd keys)"), so a distroless or scratch base would make
# every `_cmd` key fail with a "shell not found" error. If you swap this for a
# distroless/scratch base to shrink the image, drop `_cmd` and source secrets
# through environment-variable interpolation (${VAR}) or mounted secret files
# instead (see docs/design.md "Logging in containers" for the same caveat).
#
# Build:   docker build -t evm-tools .
# Run:     docker run --rm evm-tools evm-stream version
# Pipeline (stdout is data, stderr is diagnostics — never merge them):
#   docker run --rm -v "$PWD/codex-chain.toml:/etc/evm-tools/codex-chain.toml:ro" \
#     evm-tools sh -c 'evm-stream run -c /etc/evm-tools/codex-chain.toml \
#       | evm-sink-kafka run -c /etc/evm-tools/codex-chain.toml'

# --- build stage -------------------------------------------------------------
# Pin the builder to the toolchain the module targets (see go.mod).
FROM golang:1.25-alpine AS build

WORKDIR /src

# Cache module downloads on their own layer so a code-only change does not
# re-fetch dependencies.
COPY go.mod go.sum ./
RUN go mod download

# Build the static binaries. CGO stays disabled to match the release matrix
# (segmentio/kafka-go is pure Go), so the binaries are fully static and run on
# the minimal runtime image unchanged.
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown
ENV CGO_ENABLED=0
RUN set -eux; \
    ldflags="-s -w \
      -X github.com/daxchain-io/evm-tools/internal/buildinfo.Version=${VERSION} \
      -X github.com/daxchain-io/evm-tools/internal/buildinfo.Commit=${COMMIT} \
      -X github.com/daxchain-io/evm-tools/internal/buildinfo.Date=${DATE}"; \
    for tool in evm-stream evm-balance evm-sink-kafka evm-sink-webhook; do \
      go build -trimpath -ldflags "${ldflags}" -o "/out/${tool}" "./cmd/${tool}"; \
    done

# --- runtime stage -----------------------------------------------------------
# alpine keeps a shell (/bin/sh) so config `_cmd` keys work; see the comment at
# the top of this file. Pin the version so rebuilds are reproducible.
FROM alpine:3.21

# ca-certificates so outbound HTTPS (RPC, webhook endpoints, Kafka over TLS)
# can verify public roots; dumb-init reaps the process and forwards SIGINT/
# SIGTERM so the tools' graceful shutdown runs as PID 1.
# hadolint ignore=DL3018
RUN apk add --no-cache ca-certificates dumb-init \
    && adduser -D -H -u 10001 evmtools

COPY --from=build /out/evm-stream /out/evm-balance /out/evm-sink-kafka /out/evm-sink-webhook /usr/local/bin/

# Run as a non-root user; the tools need no privileges.
USER evmtools

# dumb-init as PID 1 so signals reach the tool and shutdown is graceful.
ENTRYPOINT ["dumb-init", "--"]
# Default to evm-stream; override the command to run another tool or a pipeline.
CMD ["evm-stream", "--help"]
