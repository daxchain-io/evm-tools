// Package keyperm provides a single shared check that warns when a private-key
// file is group- or world-readable, so every client-key surface (RPC mTLS, Kafka
// TLS, and any future one) enforces the same secret-handling posture rather than
// each re-implementing it.
package keyperm

import (
	"os"
	"runtime"
)

// WarnIfTooOpen calls warn(path, mode) when the file at path is group- or
// world-readable on a non-Windows host. It never reads the file contents — only
// the mode — and ignores a stat error (the caller's load step reports real read
// errors with the path). warn is typically backed by slog.Warn; a nil warn is a
// no-op.
func WarnIfTooOpen(path string, warn func(path string, mode os.FileMode)) {
	if warn == nil || runtime.GOOS == "windows" {
		return
	}
	info, err := os.Stat(path)
	if err != nil {
		return
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		warn(path, mode)
	}
}
