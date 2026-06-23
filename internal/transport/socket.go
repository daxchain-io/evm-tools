package transport

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"
)

// errWriterClosed is returned by a fan-out writer's Write after Close (with no
// ctx cancellation to report instead).
var errWriterClosed = errors.New("transport: writer closed")

// dialFunc establishes one connection to a producer — a Unix socket or a Windows
// named pipe — honoring ctx. It is the only transport-specific part of the
// reconnecting reader.
type dialFunc func(ctx context.Context) (net.Conn, error)

// newFanoutWriter wraps a listener in a fan-out writer shared by the unix and
// pipe backends: it starts the accept loop and, when blockUntilConsumer is set,
// waits for the first consumer before returning. The listener's Close is expected
// to release the underlying name (a UnixListener unlinks its socket file; a named
// pipe has no file).
func newFanoutWriter(ctx context.Context, ln net.Listener, blockUntilConsumer bool) (*fanoutWriter, error) {
	w := &fanoutWriter{
		ctx:        ctx,
		ln:         ln,
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

// fanoutWriter delivers each record (one Write per line from record.Writer) to
// every connected consumer. A slow consumer applies backpressure to the producer
// (lockstep — lossless); a consumer whose write fails is dropped without
// affecting the others; a consumer that connects mid-stream receives the live
// tail. It is transport-agnostic (any net.Listener) and safe for concurrent use
// (record.Writer already serializes Write).
type fanoutWriter struct {
	ctx        context.Context
	ln         net.Listener
	blockUntil bool

	mu        sync.Mutex
	cond      *sync.Cond
	conns     map[net.Conn]struct{}
	closed    bool
	stopWatch func() bool
}

// acceptLoop registers each new consumer until the listener is closed.
func (w *fanoutWriter) acceptLoop() {
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
func (w *fanoutWriter) waitForConns(n int) error {
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

// Write delivers p to every connected consumer. With blockUntil set it never
// reports success for a record that reached zero live consumers: if every target
// dies before receiving it, Write blocks for a new consumer and resends, so the
// producer's checkpoint never advances past an undelivered record (the lossless
// invariant). Without blockUntil a write with no live consumer is dropped
// (fire-and-forget).
func (w *fanoutWriter) Write(p []byte) (int, error) {
	for {
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

		// Write outside the lock so a slow consumer doesn't block accept or
		// teardown; it still blocks this Write — the lockstep backpressure to the
		// producer.
		var dead []net.Conn
		delivered := 0
		for _, c := range snapshot {
			if _, err := c.Write(p); err != nil {
				dead = append(dead, c)
			} else {
				delivered++
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
		// If block-until-consumer is set and the record reached no live consumer
		// (every target died), retry: block for a new consumer and resend rather
		// than dropping it. Reaching >=1 live consumer is success (a consumer that
		// died mid-record is dropped and gets the live tail on reconnect).
		if delivered == 0 && w.blockUntil {
			continue
		}
		return len(p), nil
	}
}

// shutdown closes the listener and every consumer and wakes any blocked
// Write/waiter. It is idempotent. The listener's Close releases the underlying
// name (unlinks a Unix socket file); do not also remove it here — that would race
// a successor instance that may already have recreated the name.
func (w *fanoutWriter) shutdown() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	_ = w.ln.Close()
	for c := range w.conns {
		_ = c.Close()
		delete(w.conns, c)
	}
	w.cond.Broadcast()
	w.mu.Unlock()
}

// Close tears down the writer (idempotent).
func (w *fanoutWriter) Close() error {
	if w.stopWatch != nil {
		w.stopWatch()
	}
	w.shutdown()
	return nil
}

// newReconnectingReader builds a reader that dials via dial, retrying with
// backoff until it succeeds, and transparently reconnects on disconnect.
func newReconnectingReader(ctx context.Context, dial dialFunc) (*reconnectingReader, error) {
	r := &reconnectingReader{ctx: ctx, dial: dial}
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

// reconnectingReader reads a JSONL stream from a producer over a socket or named
// pipe. It is line-oriented so a reconnect never splices half a record onto the
// next connection: on disconnect it discards any partial (newline-less) tail,
// redials with backoff, and resumes with the next complete line.
//
// Concurrency: only the ctx watcher touches conn concurrently with the reader
// goroutine, and both do so under mu. br/pending/attempt are owned by the reader
// goroutine. Like record.Reader, Read must be called from a single goroutine.
type reconnectingReader struct {
	ctx  context.Context
	dial dialFunc

	mu     sync.Mutex
	conn   net.Conn // guarded by mu (watcher closes on ctx-cancel; reader swaps on reconnect)
	closed bool     // guarded by mu

	br        *bufio.Reader // owned by the reader goroutine
	pending   []byte        // complete-line bytes not yet copied out
	attempt   int
	stopWatch func() bool
}

// Read serves complete lines from the current connection, reconnecting as needed.
func (r *reconnectingReader) Read(p []byte) (int, error) {
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

// connect dials via r.dial, retrying with backoff until it succeeds, ctx is
// cancelled, or the reader is closed (io.EOF).
func (r *reconnectingReader) connect() error {
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
		c, err := r.dial(r.ctx)
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
func (r *reconnectingReader) dropConn() {
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
func (r *reconnectingReader) Close() error {
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
