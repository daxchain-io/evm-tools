// Package stdoutsink holds the evm-sink-stdout core: read JSONL records from the
// input transport via the shared record contract and write each one's verbatim
// line to stdout. It is the one sink whose delivery target IS stdout — the
// composability hatch (`evm-sink-stdout --input unix:/run/x.sock | jq`) — so its
// own diagnostics go to stderr (see internal/logging.RouteAllToStderr), keeping
// the stdout record stream clean for the downstream consumer.
//
// Delivery is best-effort by nature: stdout is not durable. Each verbatim line is
// written and flushed before the input cursor advances, so a downstream that
// keeps up loses nothing; the only "failure" is the downstream closing the pipe
// (EPIPE), which ends the sink cleanly — there is nothing to retry to. Any other
// write error is terminal (fail fast, non-zero exit).
package stdoutsink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/daxchain-io/evm-tools/internal/record"
)

// LineWriter is the write surface the sink drives: one verbatim record line per
// call (the newline is added by the writer), flushed before returning so a
// downstream `jq`/tail sees each record promptly. The real writer (NewLineWriter)
// wraps cmd.OutOrStdout()/os.Stdout; tests substitute a fake.
type LineWriter interface {
	WriteLine(payload []byte) error
}

// Metrics is the subset of *metrics.SinkMetrics the sink reports to. A nil
// Metrics is tolerated via noopMetrics so tests need not wire one.
type Metrics interface {
	IncConsumed()
	IncDelivered(recordType string)
	IncFailed(errorType string)
	ObserveDeliver(time.Duration)
}

// Options configures a Sink.
type Options struct {
	Reader  *record.Reader
	Writer  LineWriter
	Metrics Metrics
	Logger  *slog.Logger

	// now is injectable for deterministic tests; defaults to time.Now.
	now func() time.Time
}

// Sink reads records and writes each one's verbatim line to stdout.
type Sink struct {
	opts Options
	log  *slog.Logger
	now  func() time.Time
}

// New builds a Sink from resolved options.
func New(opts Options) (*Sink, error) {
	if opts.Reader == nil {
		return nil, errors.New("stdoutsink: reader is required")
	}
	if opts.Writer == nil {
		return nil, errors.New("stdoutsink: writer is required")
	}
	if opts.Metrics == nil {
		opts.Metrics = noopMetrics{}
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.now == nil {
		opts.now = time.Now
	}
	return &Sink{opts: opts, log: opts.Logger, now: opts.now}, nil
}

// Run reads records from the input and writes each one's verbatim line to stdout,
// flushing before advancing. It returns nil on a clean EOF (the producer closed
// the input), a cancelled context, or the downstream closing stdout (EPIPE); a
// non-nil error only on an unparseable record, an unsupported schema_version, or
// a non-EPIPE write fault — all non-retryable, so failing fast is correct.
func (s *Sink) Run(ctx context.Context) (err error) {
	// Convert a panic into a terminal error so the caller's graceful shutdown
	// (metrics server stop) still runs and the process exits non-zero for a
	// supervisor restart, rather than crashing abruptly.
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("recovered from panic in stdout sink loop; stopping",
				"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			err = fmt.Errorf("stdout sink panic: %v", r)
		}
	}()
	for {
		if ctx.Err() != nil {
			return nil
		}
		env, payload, rerr := s.opts.Reader.NextCtx(ctx)
		if errors.Is(rerr, io.EOF) {
			s.log.Info("input closed; all records written")
			return nil
		}
		if rerr != nil {
			if ctx.Err() != nil {
				return nil // signal during a blocked read: clean stop
			}
			return fmt.Errorf("read record: %w", rerr)
		}
		s.opts.Metrics.IncConsumed()

		start := s.now()
		if werr := s.opts.Writer.WriteLine(payload); werr != nil {
			if errors.Is(werr, syscall.EPIPE) {
				// The downstream consumer (e.g. `| jq`) closed the pipe. There is
				// nothing to retry to; end cleanly, like a producer's closed stdout.
				s.log.Info("stdout closed by downstream; stopping")
				return nil
			}
			s.opts.Metrics.IncFailed("write")
			return fmt.Errorf("write to stdout: %w", werr)
		}
		s.opts.Metrics.ObserveDeliver(s.now().Sub(start))
		s.opts.Metrics.IncDelivered(string(env.Type))
	}
}

// lineWriter writes verbatim record lines to an underlying writer.
type lineWriter struct{ w io.Writer }

// NewLineWriter returns a LineWriter that writes verbatim record lines to w. The
// CLI passes cmd.OutOrStdout() (os.Stdout in production, a buffer under test).
func NewLineWriter(w io.Writer) LineWriter {
	return &lineWriter{w: w}
}

// WriteLine emits one record line as a SINGLE Write of payload+newline to the
// unbuffered underlying writer (os.Stdout). One Write keeps the line atomic at the
// syscall layer and avoids a bufio sticky-error: a failed write reports the error
// without having buffered (and then silently dropped) a partial line.
func (w *lineWriter) WriteLine(payload []byte) error {
	line := make([]byte, 0, len(payload)+1)
	line = append(line, payload...)
	line = append(line, '\n')
	_, err := w.w.Write(line)
	return err
}

// noopMetrics satisfies Metrics with no-ops.
type noopMetrics struct{}

func (noopMetrics) IncConsumed()                 {}
func (noopMetrics) IncDelivered(string)          {}
func (noopMetrics) IncFailed(string)             {}
func (noopMetrics) ObserveDeliver(time.Duration) {}
