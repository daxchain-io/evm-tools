// Package transport carries the JSONL record stream between a producer and a
// sink over a pluggable carrier. stdout never carries records — it is reserved
// for logs (internal/logging). A producer's output is a "unix:/path" socket (or
// a Windows "pipe:name"); an empty output means no destination at all
// (exporter-only: the producer just serves /metrics and discards records). A
// sink's input is stdin (the default, useful for replaying a JSONL file) or a
// socket it dials.
//
// Transport supplies only the bytes' carrier: a producer still wraps the returned
// io.Writer in record.Writer and a sink still wraps the returned io.Reader in
// record.Reader, so the JSONL record contract (internal/record) is unchanged.
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
	"strings"
	"time"

	"github.com/daxchain-io/evm-tools/internal/backoff"
)

// Transport address schemes. unix: is a Unix-domain socket (Linux/macOS); pipe:
// is a Windows named pipe (e.g. "pipe:evm-events" or "pipe:\\.\pipe\evm-events").
const (
	unixScheme = "unix:"
	pipeScheme = "pipe:"
)

// WriterOptions tunes a "unix:"/"pipe:" output. It is ignored for an empty
// (exporter-only) output.
type WriterOptions struct {
	// BlockUntilConsumer makes the producer wait for the first consumer before
	// emitting and block (lossless) while no consumer is connected. With it false
	// the producer never blocks and drops a write that has no consumer
	// (fire-and-forget broadcast).
	BlockUntilConsumer bool
}

// OpenWriter resolves a producer's --output spec to the io.WriteCloser a
// record.Writer wraps. An empty spec means no record destination: the producer
// runs exporter-only and records are discarded (it still serves /metrics).
// "unix:/path" listens on the socket and fans each record out to every connected
// consumer (see WriterOptions for the block-until-consumer behavior); "pipe:name"
// is the Windows equivalent. stdout is not a record destination — it carries logs
// (internal/logging) — so "-"/"stdout" are rejected.
func OpenWriter(ctx context.Context, spec string, opts WriterOptions) (io.WriteCloser, error) {
	spec = resolveSocketKeyword(spec)
	switch {
	case spec == "":
		return nopWriteCloser{io.Discard}, nil
	case strings.HasPrefix(spec, unixScheme):
		return listenUnix(ctx, strings.TrimPrefix(spec, unixScheme), opts.BlockUntilConsumer)
	case strings.HasPrefix(spec, pipeScheme):
		return listenPipe(ctx, strings.TrimPrefix(spec, pipeScheme), opts.BlockUntilConsumer)
	case spec == "-" || spec == "stdout":
		return nil, fmt.Errorf("transport: stdout no longer carries records (it carries logs); use %q or %q", "unix:/path", "pipe:name")
	default:
		return nil, fmt.Errorf("transport: unsupported --output %q (use %q or %q)", spec, "unix:/path", "pipe:name")
	}
}

// OpenReader resolves a sink's --input spec to the io.ReadCloser a record.Reader
// reads. "", "-", and "stdin" return std (the caller passes cmd.InOrStdin() so
// cobra's redirection is preserved) — though the sink CLI resolves an unset input
// to the well-known socket *before* calling here, so in practice a sink reaches
// the std branch only for an explicit "-"/"stdin" (replay). "socket" resolves to
// DefaultSpec; "unix:/path" dials the socket, retrying with backoff until a
// producer is listening, and transparently reconnects on disconnect; ctx cancels
// the wait and unblocks a blocked read.
func OpenReader(ctx context.Context, spec string, std io.Reader) (io.ReadCloser, error) {
	spec = resolveSocketKeyword(spec)
	switch {
	case isStdAlias(spec, "stdin"):
		return io.NopCloser(std), nil
	case strings.HasPrefix(spec, unixScheme):
		return dialUnix(ctx, strings.TrimPrefix(spec, unixScheme))
	case strings.HasPrefix(spec, pipeScheme):
		return dialPipe(ctx, strings.TrimPrefix(spec, pipeScheme))
	default:
		return nil, fmt.Errorf("transport: unsupported --input %q (use %q/%q, %q, or %q)", spec, "-", "stdin", "unix:/path", "pipe:name")
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
	return backoff.Jitter(backoff.Duration(n, backoffBase, backoffMax))
}
