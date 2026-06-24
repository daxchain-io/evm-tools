package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"
)

// dialProbeTimeout bounds the liveness probe that distinguishes a stale socket
// file (left by an unclean exit) from one a peer is actively listening on.
const dialProbeTimeout = 200 * time.Millisecond

// listenUnix listens on a Unix-domain socket and returns a fan-out writer. A
// stale socket from an unclean exit is removed first; a live one is left alone so
// net.Listen reports the conflict. The socket is created mode 0600 inside a 0700
// directory so only the producer's user can connect (Linux gates connect on the
// file mode; macOS on directory traversal).
func listenUnix(ctx context.Context, path string, blockUntilConsumer bool) (io.WriteCloser, error) {
	if path == "" {
		return nil, errors.New("transport: empty unix socket path in --output")
	}
	// The OS bounds sun_path (~104 bytes on macOS, ~108 on Linux); a longer path
	// makes net.Listen fail with a cryptic "invalid argument". Catch it early with a
	// clear message — the default socket lives under $TMPDIR on macOS, which can be
	// long, so an operator may need a short explicit "unix:/tmp/x.sock".
	if len(path) > 103 {
		return nil, fmt.Errorf("transport: unix socket path is too long (%d bytes; OS limit ~104): %q — use a shorter --output unix:/path under /tmp or /run", len(path), path)
	}
	removeStaleSocket(path)
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0o700) // best-effort; a pre-existing dir keeps its mode
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("transport: listen %s: %w", path, err)
	}
	// Restrict the socket to its owner (a microsecond TOCTOU between Listen and
	// Chmod; the 0700 parent directory is the race-free control).
	if cerr := os.Chmod(path, 0o600); cerr != nil {
		_ = ln.Close() // unlinks the socket file
		return nil, fmt.Errorf("transport: secure socket %s: %w", path, cerr)
	}
	return newFanoutWriter(ctx, ln, blockUntilConsumer)
}

// dialUnix returns a reconnecting reader over a producer's Unix-domain socket.
func dialUnix(ctx context.Context, path string) (io.ReadCloser, error) {
	if path == "" {
		return nil, errors.New("transport: empty unix socket path in --input")
	}
	return newReconnectingReader(ctx, unixScheme+path, func(c context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(c, "unix", path)
	})
}

// removeStaleSocket removes path only if it is a socket that nothing answers — a
// leftover from an unclean exit. If a peer answers a probe dial, the socket is
// live and left untouched so net.Listen reports the conflict rather than
// hijacking another instance's address. A non-socket at path is also left alone
// so net.Listen surfaces a clear error.
func removeStaleSocket(path string) {
	info, err := os.Stat(path)
	if err != nil {
		return // nothing there
	}
	if info.Mode()&os.ModeSocket == 0 {
		return // not a socket; let net.Listen surface the conflict
	}
	if c, derr := net.DialTimeout("unix", path, dialProbeTimeout); derr == nil {
		_ = c.Close() // a live peer is listening; do not remove
		return
	}
	_ = os.Remove(path)
}
