// Package transport carries the JSONL record stream between a producer and a
// sink over a pluggable carrier. The default carrier is stdout/stdin — the shell
// pipe the tools have always used. A "unix:/path" spec swaps in a Unix-domain
// socket so a producer and a sink can run as independent processes and connect
// directly, with no shell pipe between them.
//
// Transport supplies only the bytes' carrier: a producer still wraps the returned
// io.Writer in record.Writer and a sink still wraps the returned io.Reader in
// record.Reader, so the JSONL record contract (internal/record) is unchanged.
// stdout/stdin remain the default everywhere; the socket carrier is opt-in.
//
// Lifecycle, in brief:
//   - OpenWriter("unix:/p") listens and blocks until the first consumer connects
//     (block-until-consumer), then returns a writer over that connection. Close
//     tears down the connection, the listener, and the socket file.
//   - OpenReader("unix:/p") dials, retrying with backoff until a producer is
//     listening, and transparently reconnects on disconnect — never splicing a
//     partial record across a reconnect. Both honor ctx for prompt shutdown.
//
// Note: a Unix socket path is bounded by the OS (~104 bytes on macOS, ~108 on
// Linux); keep paths short (e.g. under /run or /tmp).
package transport

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"time"
)

// unixScheme prefixes a Unix-domain-socket address, e.g. "unix:/run/evm.sock".
const unixScheme = "unix:"

// WriterOptions tunes a "unix:" output. It is ignored for stdout.
type WriterOptions struct {
	// BlockUntilConsumer makes the producer wait for the first consumer before
	// emitting and block (lossless) while no consumer is connected. With it false
	// the producer never blocks and drops a write that has no consumer
	// (fire-and-forget broadcast).
	BlockUntilConsumer bool
}

// OpenWriter resolves a producer's --output spec to the io.WriteCloser a
// record.Writer wraps. "", "-", and "stdout" return std (the caller passes
// cmd.OutOrStdout() so cobra's redirection and the process's stdout are
// preserved; Close is a no-op so std is never closed). "unix:/path" listens on
// the socket and fans each record out to every connected consumer (see
// WriterOptions for the block-until-consumer behavior).
func OpenWriter(ctx context.Context, spec string, std io.Writer, opts WriterOptions) (io.WriteCloser, error) {
	switch {
	case isStdAlias(spec, "stdout"):
		return nopWriteCloser{std}, nil
	case strings.HasPrefix(spec, unixScheme):
		return listenUnix(ctx, strings.TrimPrefix(spec, unixScheme), opts.BlockUntilConsumer)
	default:
		return nil, fmt.Errorf("transport: unsupported --output %q (use %q, %q, or %q)", spec, "-", "stdout", "unix:/path")
	}
}

// OpenReader resolves a sink's --input spec to the io.ReadCloser a record.Reader
// reads. "", "-", and "stdin" return std (the caller passes cmd.InOrStdin() so
// cobra's redirection is preserved). "unix:/path" dials the socket, retrying with
// backoff until a producer is listening, and transparently reconnects on
// disconnect; ctx cancels the wait and unblocks a blocked read.
func OpenReader(ctx context.Context, spec string, std io.Reader) (io.ReadCloser, error) {
	switch {
	case isStdAlias(spec, "stdin"):
		return io.NopCloser(std), nil
	case strings.HasPrefix(spec, unixScheme):
		return dialUnix(ctx, strings.TrimPrefix(spec, unixScheme))
	default:
		return nil, fmt.Errorf("transport: unsupported --input %q (use %q, %q, or %q)", spec, "-", "stdin", "unix:/path")
	}
}

// isStdAlias reports whether spec selects the standard stream (stdin/stdout):
// the empty string, "-", or the stream's own name.
func isStdAlias(spec, name string) bool {
	return spec == "" || spec == "-" || spec == name
}

// nopWriteCloser adds a no-op Close to an io.Writer so os.Stdout is never closed
// by a caller that closes the transport.
type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

// Reconnect backoff, matching the sinks' convention: base*2^(n-1) capped at max,
// with jitter to spread reconnect storms.
const (
	backoffBase = 500 * time.Millisecond
	backoffMax  = 30 * time.Second
)

// backoffFor returns the delay before reconnect attempt n (1-based): base*2^(n-1)
// capped at backoffMax, with jitter in [d/2, d].
func backoffFor(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	d := backoffBase
	for i := 1; i < n && d < backoffMax; i++ {
		d *= 2
	}
	if d > backoffMax {
		d = backoffMax
	}
	return jitter(d)
}

// jitter applies jitter to a backoff duration: a uniform value in [d/2, d].
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	half := d / 2
	return half + time.Duration(rand.Int63n(int64(half)+1))
}

// sleepCtx sleeps for d unless ctx is cancelled first; it reports false if ctx
// was cancelled, so a retry loop can stop.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
