#!/usr/bin/env bash
# Bring up the integration stack (compose.yaml), run the build-tagged live tests
# against it, and tear it down. Extra args are passed through to `go test`
# (e.g. -run, -v). The tagged tests read EVM_TEST_* env vars, defaulting to the
# compose.yaml port mappings on localhost, so no extra wiring is needed.
set -euo pipefail

cd "$(dirname "$0")/.."

compose() { docker compose -f compose.yaml "$@"; }

cleanup() { compose down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "==> bringing up the integration stack"
compose up -d --wait

echo "==> running live tests (-tags integration)"
go test -tags integration ./... "$@"
