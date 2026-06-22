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

// errWriterClosed is returned by a fan-out writer's Write after Close (with no
// ctx cancellation to report instead).
var errWriterClosed = errors.New("transport: writer closed")

// listenUnix listens on a Unix-domain socket and returns a fan-out writer that
// delivers each record to every connected consumer. With blockUntilConsumer it
// waits for the first consumer before returning (so a sink that starts shortly
// after the producer loses nothing) and blocks writes while no consumer is
// connected (lossless); without it, it returns immediately and drops a write that
// has no consumer (fire-and-forget). A stale socket from an unclean exit is
// removed first; a live one is left alone so net.Listen reports the conflict.
func listenUnix(ctx context.Context, path string, blockUntilConsumer bool) (io.WriteCloser, error) {
	if path == "" {
		return nil, errors.New("transport: empty unix socket path in --output")
	}
	removeStaleSocket(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("transport: listen %s: %w", path, err)
	}
	w := &unixFanoutWriter{
		ctx:        ctx,
		ln:         ln,
		path:       path,
		blockUntil: blockUntilConsumer,
		conns:      make(map[net.Conn]struct{}),
	}
	w.cond = sync.NewCond(&w.mu)
	// On ctx cancel, tear everything down and wake any blocked Write / waiter.
	w.stopWatch = context.AfterFunc(ctx, w.shutdown)
	go w.acceptLoop()
	if blockUntilConsumer {
		if err := w.waitForConns(1); err != nil {
			_ = w.Close()
			return nil, err
		}
	}
	return w, nil
}

// unixFanoutWriter delivers each record (one Write per line from record.Writer)
// to every connected consumer. A slow consumer applies backpressure to the
// producer (lockstep — lossless); a consumer whose write fails is dropped without
// affecting the others; a consumer that connects mid-stream receives the live
// tail. It is safe for concurrent use (record.Writer already serializes Write).
type unixFanoutWriter struct {
	ctx        context.Context
	ln         net.Listener
	path       string
	blockUntil bool

	mu        sync.Mutex
	cond      *sync.Cond
	conns     map[net.Conn]struct{}
	closed    bool
	stopWatch func() bool
}

// acceptLoop registers each new consumer until the listener is closed.
func (w *unixFanoutWriter) acceptLoop() {
	for {
		c, err := w.ln.Accept()
		if err != nil {
			return // listener closed
		}
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			_ = c.Close()
			return
		}
		w.conns[c] = struct{}{}
		w.cond.Broadcast() // wake waitForConns / a Write blocked on zero consumers
		w.mu.Unlock()
	}
}

// waitForConns blocks until at least n consumers are connected, or the writer is
// closed / ctx is cancelled.
func (w *unixFanoutWriter) waitForConns(n int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for len(w.conns) < n && !w.closed {
		w.cond.Wait()
	}
	if w.closed {
		if err := w.ctx.Err(); err != nil {
			return err
		}
		return errWriterClosed
	}
	return nil
}

// Write delivers p to every connected consumer.
func (w *unixFanoutWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	for len(w.conns) == 0 && w.blockUntil && !w.closed {
		w.cond.Wait()
	}
	if w.closed {
		w.mu.Unlock()
		if err := w.ctx.Err(); err != nil {
			return 0, err
		}
		return 0, errWriterClosed
	}
	snapshot := make([]net.Conn, 0, len(w.conns))
	for c := range w.conns {
		snapshot = append(snapshot, c)
	}
	w.mu.Unlock()

	// Write outside the lock so a slow consumer doesn't block accept or teardown;
	// it still blocks this Write, which is the lockstep backpressure to the producer.
	var dead []net.Conn
	for _, c := range snapshot {
		if _, err := c.Write(p); err != nil {
			dead = append(dead, c)
		}
	}
	if len(dead) > 0 {
		w.mu.Lock()
		for _, c := range dead {
			if _, ok := w.conns[c]; ok {
				delete(w.conns, c)
				_ = c.Close()
			}
		}
		w.mu.Unlock()
	}
	return len(p), nil
}

// shutdown closes the listener and every consumer, removes the socket file, and
// wakes any blocked Write/waiter. It is idempotent.
func (w *unixFanoutWriter) shutdown() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	_ = w.ln.Close() // a UnixListener unlinks the socket file on Close
	for c := range w.conns {
		_ = c.Close()
		delete(w.conns, c)
	}
	w.cond.Broadcast()
	w.mu.Unlock()
	_ = os.Remove(w.path)
}

// Close tears down the writer (idempotent).
func (w *unixFanoutWriter) Close() error {
	if w.stopWatch != nil {
		w.stopWatch()
	}
	w.shutdown()
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
