#!/bin/sh
# Smoke-test the GoReleaser-generated Homebrew cask against local snapshot
# archives. This lets CI run `brew install` before any tagged release exists.
set -eu

ROOT="${1:-$(pwd)}"
CASK_PATH="${EVM_TOOLS_CASK_PATH:-${ROOT}/dist/homebrew/Casks/evm-tools.rb}"
DIST_DIR="${EVM_TOOLS_DIST_DIR:-${ROOT}/dist}"
CASK_NAME="evm-tools"
TAP_NAME="${EVM_TOOLS_BREW_SMOKE_TAP:-local/evm-tools-smoke}"
BINS="evm-stream evm-balance evm-sink-kafka evm-sink-webhook"

err() {
  echo "brew-cask-smoke.sh: error: $*" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || err "required command '$1' not found"
}

need brew
need ruby

[ -f "$CASK_PATH" ] || err "cask not found: $CASK_PATH"
[ -d "$DIST_DIR" ] || err "dist dir not found: $DIST_DIR"

if brew list --cask "$CASK_NAME" >/dev/null 2>&1; then
  if [ "${EVM_TOOLS_BREW_SMOKE_FORCE:-0}" != "1" ]; then
    err "$CASK_NAME is already installed; set EVM_TOOLS_BREW_SMOKE_FORCE=1 to uninstall/reinstall for smoke testing"
  fi
  brew uninstall --cask --force "$CASK_NAME"
fi

tmp="$(mktemp -d)"
installed=0
tapped=0
cleanup() {
  if [ "$installed" = "1" ]; then
    brew uninstall --cask --force "$CASK_NAME" >/dev/null 2>&1 || true
  fi
  if [ "$tapped" = "1" ]; then
    brew untap "$TAP_NAME" >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT

smoke_cask="${tmp}/evm-tools.rb"
cp "$CASK_PATH" "$smoke_cask"

ruby - "$smoke_cask" "$DIST_DIR" <<'RUBY'
path, dist = ARGV
text = File.read(path)
version = text[/^\s*version\s+"([^"]+)"/, 1]
raise "could not determine cask version" unless version

%w[darwin linux].product(%w[amd64 arm64]).each do |os, arch|
  archive = File.expand_path("evm-tools_#{version}_#{os}_#{arch}.tar.gz", dist)
  raise "missing archive #{archive}" unless File.file?(archive)

  pattern = %r{https://github\.com/daxchain-io/evm-tools/releases/download/[^"]+/evm-tools_#\{version\}_#{os}_#{arch}\.tar\.gz}
  replacement = "file://#{archive}"
  text = text.gsub(pattern, replacement)
end

File.write(path, text)
RUBY

export HOMEBREW_NO_AUTO_UPDATE=1
export HOMEBREW_NO_INSTALL_CLEANUP=1
export HOMEBREW_NO_INSTALLED_DEPENDENTS_CHECK=1
export GIT_AUTHOR_NAME="${GIT_AUTHOR_NAME:-evm-tools CI}"
export GIT_AUTHOR_EMAIL="${GIT_AUTHOR_EMAIL:-ci@daxchain.io}"
export GIT_COMMITTER_NAME="${GIT_COMMITTER_NAME:-evm-tools CI}"
export GIT_COMMITTER_EMAIL="${GIT_COMMITTER_EMAIL:-ci@daxchain.io}"

brew untap "$TAP_NAME" >/dev/null 2>&1 || true
brew tap-new "$TAP_NAME" >/dev/null
tapped=1

tap_dir="$(brew --repo "$TAP_NAME")"
mkdir -p "${tap_dir}/Casks"
cp "$smoke_cask" "${tap_dir}/Casks/${CASK_NAME}.rb"

brew install --cask --force "${TAP_NAME}/${CASK_NAME}"
installed=1

for bin in $BINS; do
  command -v "$bin" >/dev/null 2>&1 || err "$bin was not linked by Homebrew"
  "$bin" version >/dev/null
done

echo "Homebrew cask smoke test passed for $CASK_NAME" >&2
