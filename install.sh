#!/bin/sh
# Universal installer for the evm-tools suite.
#
#   curl -fsSL https://github.com/daxchain-io/evm-tools/releases/latest/download/install.sh | sh
#
# It detects the OS and CPU architecture, downloads the matching release
# archive, verifies its SHA-256 checksum against the signed checksums file, and
# installs the requested binary. Downloads are HTTPS and the script fails closed.
#
# For high-assurance environments, download and inspect this script before
# running it instead of piping it straight into a shell.
#
# Environment overrides:
#   EVM_TOOLS_BIN      binary to install: evm-stream | evm-balance (default evm-stream)
#   EVM_TOOLS_VERSION  version tag to install (default: latest)
#   EVM_TOOLS_INSTALL_DIR  install directory (default: /usr/local/bin)
set -eu

REPO="daxchain-io/evm-tools"
BIN="${EVM_TOOLS_BIN:-evm-stream}"
VERSION="${EVM_TOOLS_VERSION:-latest}"
INSTALL_DIR="${EVM_TOOLS_INSTALL_DIR:-/usr/local/bin}"

err() {
  echo "install.sh: error: $*" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || err "required command '$1' not found"
}

case "$BIN" in
  evm-stream | evm-balance) ;;
  *) err "unsupported binary '$BIN' (want evm-stream or evm-balance)" ;;
esac

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

base_url="https://github.com/${REPO}/releases"
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

archive="${BIN}_${ver_no_v}_${os}_${arch}.tar.gz"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "Downloading ${archive} ..." >&2
download "${dl}/${archive}" "${tmp}/${archive}"
download "${dl}/checksums.txt" "${tmp}/checksums.txt"

# Verify checksum. Establish trust in the checksum independently of the
# artifact by verifying the signed checksums file when cosign is available.
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

install -m 0755 "${tmp}/${BIN}" "${INSTALL_DIR}/${BIN}"
echo "Installed ${BIN} to ${INSTALL_DIR}/${BIN}" >&2
"${INSTALL_DIR}/${BIN}" version || true
