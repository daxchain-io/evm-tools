#!/bin/sh
# Keyless cosign signing wrapper invoked by GoReleaser's `signs` step.
#
# Real tagged releases run in GitHub Actions with `id-token: write`, which
# exports ACTIONS_ID_TOKEN_REQUEST_URL/_TOKEN; cosign uses that OIDC identity to
# sign keylessly with no stored key. A local `goreleaser release --snapshot
# --clean` dry-run has no such identity, so unconditional keyless signing would
# block on the interactive Sigstore device flow and then fail offline.
#
# This wrapper signs only when a keyless OIDC identity is actually available
# (CI) or when an explicit COSIGN_PRIVATE_KEY fallback is configured. Otherwise
# it skips signing with a clear message so the snapshot stays offline-safe. The
# real release path is unchanged: in CI the identity is present, so it signs.
#
# Usage (from .goreleaser.yaml):
#   cmd: sh
#   args: ["scripts/cosign-sign.sh", "${artifact}", "${signature}", "${certificate}"]
set -eu

artifact="${1:?artifact path required}"
signature="${2:?signature path required}"
certificate="${3:?certificate path required}"

have_oidc=0
if [ -n "${ACTIONS_ID_TOKEN_REQUEST_URL:-}" ] && [ -n "${ACTIONS_ID_TOKEN_REQUEST_TOKEN:-}" ]; then
  have_oidc=1
fi
if [ -n "${SIGSTORE_ID_TOKEN:-}" ]; then
  have_oidc=1
fi

have_key=0
if [ -n "${COSIGN_PRIVATE_KEY:-}" ]; then
  have_key=1
fi

# GoReleaser registers the signature and certificate output paths as release
# artifacts (signs.output: true). When signing is skipped we still write empty
# placeholders so those registered paths exist on disk and GoReleaser never
# references a missing upload asset.
skip_with_placeholders() {
  : >"${signature}"
  : >"${certificate}"
}

# An operator can force-skip signing (mirrors `goreleaser ... --skip=sign`).
if [ "${COSIGN_SKIP:-0}" = "1" ]; then
  echo "cosign-sign: COSIGN_SKIP=1 set, skipping signature for ${artifact}" >&2
  skip_with_placeholders
  exit 0
fi

if [ "$have_oidc" -eq 0 ] && [ "$have_key" -eq 0 ]; then
  echo "cosign-sign: no OIDC identity (CI) or COSIGN_PRIVATE_KEY present;" >&2
  echo "cosign-sign: skipping signature for ${artifact} (snapshot/offline build)." >&2
  skip_with_placeholders
  exit 0
fi

if ! command -v cosign >/dev/null 2>&1; then
  echo "cosign-sign: error: cosign not found but signing was requested" >&2
  exit 1
fi

# install.sh verifies checksums.txt with `cosign verify-blob --signature <.sig>
# --certificate <.pem>`, i.e. the legacy DETACHED outputs. Pin that output
# format explicitly: --new-bundle-format=false makes sign-blob write the
# detached --output-signature/--output-certificate files regardless of the
# cosign default. cosign 2.x already defaults this off (so this is a no-op on
# the v2 line CI pins in .github/workflows/release.yml); cosign 3.x defaults it
# ON and would otherwise ignore --output-* and fail. Stating it keeps the
# wrapper honest if that pin ever moves. The flag exists since cosign 2.4.
# Adopting the bundle format here means switching install.sh to the bundle
# verify flow at the same time.

if [ "$have_key" -eq 1 ] && [ "$have_oidc" -eq 0 ]; then
  # Stored-key fallback when keyless OIDC is unavailable. No certificate is
  # produced in this mode, so write an empty placeholder to satisfy the
  # configured output path.
  echo "cosign-sign: signing ${artifact} with COSIGN_PRIVATE_KEY (key mode)" >&2
  cosign sign-blob \
    --key env://COSIGN_PRIVATE_KEY \
    --new-bundle-format=false \
    --output-signature="${signature}" \
    "${artifact}" \
    --yes
  : >"${certificate}"
  exit 0
fi

echo "cosign-sign: signing ${artifact} with keyless OIDC" >&2
cosign sign-blob \
  --new-bundle-format=false \
  --output-signature="${signature}" \
  --output-certificate="${certificate}" \
  "${artifact}" \
  --yes
