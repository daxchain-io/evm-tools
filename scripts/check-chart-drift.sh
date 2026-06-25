#!/usr/bin/env bash
#
# Guard against silent drift between the two Helm charts. evm-stream and
# evm-balance intentionally share every template except their producer config
# (configmap.yaml) and a few value defaults; the shared templates must stay
# identical modulo the chart-name token. This normalizes evm-stream's shared
# templates to the evm-balance name and diffs them — any difference fails so a
# one-sided edit to a shared template can't ship unnoticed.
set -euo pipefail

cd "$(dirname "$0")/.."

shared=(deployment.yaml service.yaml secret.yaml NOTES.txt _helpers.tpl)
status=0

for f in "${shared[@]}"; do
  a="charts/evm-stream/templates/$f"
  b="charts/evm-balance/templates/$f"
  if ! diff -u <(sed 's/evm-stream/evm-balance/g' "$a") "$b"; then
    echo ">>> DRIFT: charts/{evm-stream,evm-balance}/templates/$f differ beyond the chart-name token." >&2
    status=1
  fi
done

if [ "$status" -eq 0 ]; then
  echo "OK: shared chart templates are in lockstep (identical modulo the chart name)."
fi
exit "$status"
