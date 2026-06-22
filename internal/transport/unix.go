package transport

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// dialProbeTimeout bounds the liveness probe that distinguishes a stale socket
// file (left by an unclean exit) from one a peer is actively listening on.
const dialProbeTimeout = 200 * time.Millisecond

// listenUnix listens on a Unix-domain socket and blocks until the first consumer
// connects (block-until-consumer), returning a writer over that connection. A
// stale socket left by an unclean exit is removed first; a live one is left alone
// so net.Listen fails rather than stealing another instance's address.
func listenUnix(ctx context.Context, path string) (io.WriteCloser, error) {
	if path == "" {
		return nil, errors.New("transport: empty unix socket path in --output")
	}
	removeStaleSocket(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("transport: listen %s: %w", path, err)
	}
	// Closing the listener unblocks Accept, so a ctx cancel while we wait for the
	// first consumer returns promptly.
	stop := context.AfterFunc(ctx, func() { _ = ln.Close() })
	conn, err := ln.Accept()
	stop()
	if err != nil {
		_ = ln.Close()
		_ = os.Remove(path)
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		return nil, fmt.Errorf("transport: accept %s: %w", path, err)
	}
	return &unixWriter{conn: conn, ln: ln, path: path}, nil
}

// unixWriter writes records to the single connected consumer. A failed Write
// (consumer gone) surfaces to record.Writer.Emit exactly like a broken pipe, so
// the producer stops cleanly — matching stdout-pipe semantics for 1:1.
// (Fan-out to multiple consumers arrives in a later phase.)
type unixWriter struct {
	conn net.Conn
	ln   net.Listener
	path string
	once sync.Once
}

func (w *unixWriter) Write(p []byte) (int, error) { return w.conn.Write(p) }

// Close tears down the connection and listener and removes the socket file. It is
// idempotent.
func (w *unixWriter) Close() error {
	w.once.Do(func() {
		_ = w.conn.Close()
		_ = w.ln.Close() // a UnixListener unlinks the socket file on Close
		_ = os.Remove(w.path)
	})
	return nil
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

// dialUnix connects to a producer's Unix-domain socket, retrying with backoff
// until it succeeds or ctx is cancelled, and returns a reader that transparently
// reconnects on disconnect.
func dialUnix(ctx context.Context, path string) (io.ReadCloser, error) {
	if path == "" {
		return nil, errors.New("transport: empty unix socket path in --input")
	}
	r := &unixReader{ctx: ctx, path: path}
	if err := r.connect(); err != nil {
		return nil, err
	}
	// On ctx cancel, close the current connection to unblock a blocked Read.
	r.stopWatch = context.AfterFunc(ctx, func() {
		r.mu.Lock()
		r.closed = true
		if r.conn != nil {
			_ = r.conn.Close()
			r.conn = nil
		}
		r.mu.Unlock()
	})
	return r, nil
}

// unixReader reads a JSONL stream from a producer over a Unix-domain socket. It
// is line-oriented so a reconnect never splices half a record onto the next
// connection: on disconnect it discards any partial (newline-less) tail, redials
// with backoff, and resumes with the next complete line.
//
// Concurrency: only the ctx watcher touches conn concurrently with the reader
// goroutine, and both do so under mu. br/pending/attempt are owned by the reader
// goroutine. Like record.Reader, Read must be called from a single goroutine.
type unixReader struct {
	ctx  context.Context
	path string

	mu     sync.Mutex
	conn   net.Conn // guarded by mu (watcher closes on ctx-cancel; reader swaps on reconnect)
	closed bool     // guarded by mu

	br        *bufio.Reader // owned by the reader goroutine
	pending   []byte        // complete-line bytes not yet copied out
	attempt   int
	stopWatch func() bool
}

// Read serves complete lines from the current connection, reconnecting as needed.
func (r *unixReader) Read(p []byte) (int, error) {
	for {
		if len(r.pending) > 0 {
			n := copy(p, r.pending)
			r.pending = r.pending[n:]
			return n, nil
		}
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		if r.br == nil {
			if err := r.connect(); err != nil {
				return 0, err
			}
		}
		line, err := r.br.ReadBytes('\n')
		if err != nil {
			// Drop any partial (no trailing newline) line so the next connection's
			// stream is never spliced onto half a record, then reconnect.
			r.dropConn()
			if cerr := r.ctx.Err(); cerr != nil {
				return 0, cerr
			}
			continue
		}
		r.pending = line
	}
}

// connect dials the socket, retrying with backoff until it succeeds, ctx is
// cancelled, or the reader is closed (io.EOF).
func (r *unixReader) connect() error {
	for {
		r.mu.Lock()
		closed := r.closed
		r.mu.Unlock()
		if closed {
			return io.EOF
		}
		if err := r.ctx.Err(); err != nil {
			return err
		}
		c, err := (&net.Dialer{}).DialContext(r.ctx, "unix", r.path)
		if err == nil {
			r.mu.Lock()
			if r.closed {
				r.mu.Unlock()
				_ = c.Close()
				return io.EOF
			}
			r.conn = c
			r.mu.Unlock()
			r.br = bufio.NewReader(c)
			r.attempt = 0
			return nil
		}
		r.attempt++
		if !sleepCtx(r.ctx, backoffFor(r.attempt)) {
			return r.ctx.Err()
		}
	}
}

// dropConn closes the current connection (mu-guarded) and clears the reader's
// buffer so the next read reconnects. br is owned by the reader goroutine.
func (r *unixReader) dropConn() {
	r.mu.Lock()
	if r.conn != nil {
		_ = r.conn.Close()
		r.conn = nil
	}
	r.mu.Unlock()
	r.br = nil
}

// Close stops the ctx watcher and closes the current connection. Subsequent reads
// return io.EOF. It is safe to call once.
func (r *unixReader) Close() error {
	if r.stopWatch != nil {
		r.stopWatch()
	}
	r.mu.Lock()
	r.closed = true
	if r.conn != nil {
		_ = r.conn.Close()
		r.conn = nil
	}
	r.mu.Unlock()
	return nil
}
