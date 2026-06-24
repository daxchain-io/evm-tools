package transport

import (
	"os"
	"path/filepath"
	"runtime"
)

// DefaultSpec returns the platform's well-known default transport spec. It is what
// the spec "socket" resolves to (on either --output or --input) and what a sink's
// --input defaults to when unset — so one producer (`--output socket`) and its
// sink(s) (default --input) auto-pair on this address with no path to type.
//
// On Linux/macOS it is a Unix socket under $XDG_RUNTIME_DIR (a per-user, 0700
// runtime dir) or, when that is unset, the per-user temp dir; the transport
// creates the socket 0600 inside a 0700 directory either way. On Windows it is a
// named pipe whose ACL is owner-only.
//
// The default is host-local and single-pipeline: one producer per host (or per
// Kubernetes pod, where it lives on a shared volume). To run more than one
// pipeline on the same host, give each an explicit "unix:/path" / "pipe:name".
func DefaultSpec() string {
	if runtime.GOOS == "windows" {
		return pipeScheme + "evm-tools-records"
	}
	dir := os.Getenv("XDG_RUNTIME_DIR")
	if dir == "" {
		dir = os.TempDir()
	}
	return unixScheme + filepath.Join(dir, "evm-tools", "records.sock")
}

// resolveSocketKeyword maps the convenience spec "socket" to DefaultSpec(); any
// other spec (including "", "unix:/path", "pipe:name", "-") is returned unchanged.
func resolveSocketKeyword(spec string) string {
	if spec == "socket" {
		return DefaultSpec()
	}
	return spec
}
