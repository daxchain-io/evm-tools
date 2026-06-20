#!/bin/sh
# Universal installer for the evm-tools suite.
#
#   curl -fsSL https://github.com/daxchain-io/evm-tools/releases/latest/download/install.sh | sh
#
# It detects the OS and CPU architecture, downloads the matching release
# archive, establishes trust in the checksums file by verifying its cosign
# signature against a pinned identity, then verifies the archive's SHA-256
# against that trusted checksums file and installs the requested binary.
# Downloads are HTTPS and the script fails closed.
#
# For high-assurance environments, download and inspect this script before
# running it instead of piping it straight into a shell.
#
# Environment overrides:
#   EVM_TOOLS_BIN      which CLIs to install: "all" (default) installs the whole
#                      suite from one archive, or name one — evm-stream |
#                      evm-balance | evm-sink-kafka | evm-sink-webhook
#   EVM_TOOLS_VERSION  version tag to install (default: latest)
#   EVM_TOOLS_INSTALL_DIR  install directory (default: /usr/local/bin)
#   EVM_TOOLS_BASE_URL     base "releases" URL (default the GitHub repo); set
#                          this to install from a mirror or to test against a
#                          local snapshot. "latest" resolution needs the real
#                          GitHub redirect, so pin EVM_TOOLS_VERSION when using
#                          a base URL that has no /latest redirect.
#   EVM_TOOLS_SKIP_SIGNATURE  set to 1 to skip cosign signature verification of
#                          the checksums file (NOT recommended; downgrades the
#                          trust model to a plain same-channel SHA-256 check).
set -eu

REPO="daxchain-io/evm-tools"
BIN="${EVM_TOOLS_BIN:-all}"
ALL_BINS="evm-stream evm-balance evm-sink-kafka evm-sink-webhook"
VERSION="${EVM_TOOLS_VERSION:-latest}"
INSTALL_DIR="${EVM_TOOLS_INSTALL_DIR:-/usr/local/bin}"
BASE_URL="${EVM_TOOLS_BASE_URL:-https://github.com/${REPO}/releases}"

# Pinned cosign identity for the keyless-signed checksums file. The release
# workflow (.github/workflows/release.yml) signs checksums.txt with GitHub OIDC,
# so the signing certificate's identity is the release workflow's ref under this
# repo and its issuer is the GitHub Actions OIDC provider. Pinning both here is
# the keyless equivalent of shipping a pinned public key: a compromised release
# channel cannot forge a checksums signature without also controlling this
# repo's GitHub Actions identity.
COSIGN_IDENTITY_REGEXP="${EVM_TOOLS_COSIGN_IDENTITY_REGEXP:-^https://github.com/${REPO}/\.github/workflows/.+@refs/tags/v}"
COSIGN_OIDC_ISSUER="${EVM_TOOLS_COSIGN_OIDC_ISSUER:-https://token.actions.githubusercontent.com}"

err() {
  echo "install.sh: error: $*" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || err "required command '$1' not found"
}

case "$BIN" in
  all | evm-stream | evm-balance | evm-sink-kafka | evm-sink-webhook) ;;
  *) err "unsupported EVM_TOOLS_BIN '$BIN' (want all, evm-stream, evm-balance, evm-sink-kafka, or evm-sink-webhook)" ;;
esac

# The release bundles every binary in one archive; pick which to install.
if [ "$BIN" = "all" ]; then
  bins="$ALL_BINS"
else
  bins="$BIN"
fi

need uname
need mktemp

# A downloader: prefer curl, fall back to wget.
download() {
  # $1 url, $2 dest
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$1" -o "$2"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "$2" "$1"
  else
    err "need curl or wget to download"
  fi
}

# Detect OS.
os="$(uname -s)"
case "$os" in
  Linux) os="linux" ;;
  Darwin) os="darwin" ;;
  *) err "unsupported OS '$os' (supported: Linux, Darwin)" ;;
esac

# Detect architecture and map to the release naming.
arch="$(uname -m)"
case "$arch" in
  x86_64 | amd64) arch="amd64" ;;
  arm64 | aarch64) arch="arm64" ;;
  *) err "unsupported architecture '$arch' (supported: amd64, arm64)" ;;
esac

base_url="$BASE_URL"
if [ "$VERSION" = "latest" ]; then
  dl="${base_url}/latest/download"
else
  dl="${base_url}/download/${VERSION}"
fi

# Strip a leading "v" for the archive name, which GoReleaser builds without it.
ver_no_v="${VERSION#v}"
if [ "$VERSION" = "latest" ]; then
  # The archive name embeds the concrete version even for the "latest" path,
  # so resolve the redirect to learn the tag.
  if command -v curl >/dev/null 2>&1; then
    resolved="$(curl -fsSLI -o /dev/null -w '%{url_effective}' "${base_url}/latest")"
    tag="${resolved##*/}"
    ver_no_v="${tag#v}"
    dl="${base_url}/download/${tag}"
  else
    err "resolving 'latest' needs curl; set EVM_TOOLS_VERSION explicitly"
  fi
fi

archive="evm-tools_${ver_no_v}_${os}_${arch}.tar.gz"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "Downloading ${archive} ..." >&2
download "${dl}/${archive}" "${tmp}/${archive}"
download "${dl}/checksums.txt" "${tmp}/checksums.txt"

# Establish trust in the checksums file independently of the artifact channel
# BEFORE trusting any hash it contains. The release pipeline signs checksums.txt
# keylessly with cosign (GitHub OIDC), publishing checksums.txt.sig and
# checksums.txt.pem. We verify that signature against the pinned identity/issuer
# above, so a compromised release host cannot swap the archive AND rewrite a
# matching checksum: forging the signature would also require this repo's GitHub
# Actions OIDC identity.
verify_checksums_signature() {
  download "${dl}/checksums.txt.sig" "${tmp}/checksums.txt.sig" ||
    err "could not download checksums.txt.sig; cannot verify signature (set EVM_TOOLS_SKIP_SIGNATURE=1 to bypass at your own risk)"
  download "${dl}/checksums.txt.pem" "${tmp}/checksums.txt.pem" ||
    err "could not download checksums.txt.pem; cannot verify signature (set EVM_TOOLS_SKIP_SIGNATURE=1 to bypass at your own risk)"

  echo "Verifying checksums.txt signature (cosign) ..." >&2
  cosign verify-blob \
    --certificate "${tmp}/checksums.txt.pem" \
    --signature "${tmp}/checksums.txt.sig" \
    --certificate-identity-regexp "$COSIGN_IDENTITY_REGEXP" \
    --certificate-oidc-issuer "$COSIGN_OIDC_ISSUER" \
    "${tmp}/checksums.txt" >&2 ||
    err "cosign signature verification of checksums.txt failed; refusing to install"
}

if [ "${EVM_TOOLS_SKIP_SIGNATURE:-0}" = "1" ]; then
  echo "install.sh: WARNING: EVM_TOOLS_SKIP_SIGNATURE=1 set; skipping cosign" >&2
  echo "install.sh: WARNING: signature verification. Checksum trust is NOT" >&2
  echo "install.sh: WARNING: independent of the download channel." >&2
elif command -v cosign >/dev/null 2>&1; then
  verify_checksums_signature
else
  err "cosign not found: checksums.txt signature cannot be verified. Install
cosign (https://docs.sigstore.dev/cosign/installation) so the signed checksums
file can be verified against the pinned release identity, or re-run with
EVM_TOOLS_SKIP_SIGNATURE=1 to accept an unauthenticated same-channel SHA-256
check at your own risk."
fi

# Verify the archive's SHA-256 against the (now trusted) checksums file.
echo "Verifying checksum ..." >&2
expected="$(grep " ${archive}\$" "${tmp}/checksums.txt" | awk '{print $1}')"
[ -n "$expected" ] || err "no checksum entry for ${archive}"

if command -v sha256sum >/dev/null 2>&1; then
  actual="$(sha256sum "${tmp}/${archive}" | awk '{print $1}')"
elif command -v shasum >/dev/null 2>&1; then
  actual="$(shasum -a 256 "${tmp}/${archive}" | awk '{print $1}')"
else
  err "need sha256sum or shasum to verify the download"
fi

[ "$expected" = "$actual" ] || err "checksum mismatch for ${archive}"

# Extract and install.
need tar
tar -xzf "${tmp}/${archive}" -C "${tmp}"

if [ ! -w "$INSTALL_DIR" ]; then
  err "install dir '$INSTALL_DIR' is not writable; re-run with sudo or set EVM_TOOLS_INSTALL_DIR"
fi

# shellcheck disable=SC2086 # intentional word-splitting of the space-separated bin list
for b in $bins; do
  [ -f "${tmp}/${b}" ] || err "binary '${b}' not found in ${archive}"
  install -m 0755 "${tmp}/${b}" "${INSTALL_DIR}/${b}"
  echo "Installed ${b} to ${INSTALL_DIR}/${b}" >&2
done
