// Package filesink holds the evm-sink-file core logic: read JSONL records from
// stdin via the shared record contract and append each one to a rotating local
// file with at-least-once durability.
//
// Scope: a RECORDER with OPTIONAL FILTERS and ROTATION. Each record's verbatim
// JSONL line is appended to the active file (line-atomic, optionally fsync'd),
// which rotates by size and/or age with optional gzip compression and retention
// (max_backups). Filters narrow which records are written by record type and
// name — NOT a rule DSL (use evm-sink-webhook for a field condition).
//
// Delivery semantics: AT-LEAST-ONCE. Every write (and fsync, when enabled) is
// confirmed before the stdin cursor advances, so a record is never dropped. A
// transient failure — a full disk (ENOSPC/EDQUOT) — is retried with blocking
// exponential backoff plus full jitter, so backpressure propagates up the pipe to
// the lossless producer rather than dropping records or buffering without bound.
// Any other write error is treated as permanent (a real filesystem/config fault):
// the sink fails fast (exits non-zero) rather than spinning forever, which still
// preserves losslessness — the unwritten record stays in the upstream pipe.
//
// The file write is behind the [FileWriter] interface so tests can exercise the
// full run path (including retry) with an in-memory fake; the real rotating
// writer lives in writer.go.
package filesink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"syscall"
	"time"

	"github.com/daxchain-io/evm-tools/internal/record"
)

// FileWriter is the durable-write surface the sink drives. The real
// *Writer (writer.go) implements it; tests substitute a fake.
type FileWriter interface {
	// Write appends one record line (a trailing newline is added by the writer)
	// and, when fsync is enabled, flushes it before returning.
	Write(line []byte) (int, error)
	// Sync flushes buffered data to stable storage.
	Sync() error
	// Size reports the active file's current size in bytes.
	Size() int64
	// Close flushes and closes the active file.
	Close() error
}

// Metrics is the subset of *metrics.File the sink reports to. A nil Metrics is
// tolerated via noopMetrics so tests need not wire one.
type Metrics interface {
	IncConsumed()
	IncFiltered()
	IncWritten(recordType string)
	IncFailed(et string)
	ObserveWrite(d time.Duration)
	IncRetry()
	SetCurrentSizeBytes(n int64)
	SetBackoffSeconds(d time.Duration)
	SetBlocked(blocked bool)
	SetConsecutiveFailures(n int)
}

// Healther is the readiness surface the loop updates. /readyz flips to not-ready
// while the sink has been blocked on a failing disk beyond its threshold.
type Healther interface {
	SetWriteBlocked(d time.Duration)
	SetWritable(v bool)
}

// Options configures a Sink.
type Options struct {
	Reader  *record.Reader
	Writer  FileWriter
	Filter  *Filter
	Metrics Metrics
	Health  Healther
	Logger  *slog.Logger

	// BackoffBase / BackoffMax bound the blocking exponential backoff on a
	// transient (disk-full) write failure. Zero values fall back to defaults.
	BackoffBase time.Duration
	BackoffMax  time.Duration

	// now and randInt are injectable for deterministic tests.
	now     func() time.Time
	randInt func(n int64) int64
}

// Sink reads records and appends each one at-least-once to a rotating file.
type Sink struct {
	opts Options
	log  *slog.Logger
	now  func() time.Time
}

// New builds a Sink from resolved options.
func New(opts Options) (*Sink, error) {
	if opts.Reader == nil {
		return nil, errors.New("filesink: reader is required")
	}
	if opts.Writer == nil {
		return nil, errors.New("filesink: writer is required")
	}
	if opts.Filter == nil {
		opts.Filter = NewFilter(FilterOptions{})
	}
	if opts.Metrics == nil {
		opts.Metrics = noopMetrics{}
	}
	if opts.Health == nil {
		opts.Health = noopHealth{}
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.now == nil {
		opts.now = time.Now
	}
	if opts.randInt == nil {
		opts.randInt = rand.Int63n
	}
	if opts.BackoffBase <= 0 {
		opts.BackoffBase = 500 * time.Millisecond
	}
	if opts.BackoffMax <= 0 {
		opts.BackoffMax = 30 * time.Second
	}
	return &Sink{opts: opts, log: opts.Logger, now: opts.now}, nil
}

// Run reads records from stdin and appends each one (that passes the filter),
// confirming the write before advancing to the next record. It returns nil on a
// clean EOF (the producer closed the pipe) or a cancelled context, and a non-nil
// error only on a permanent failure: a record this build cannot parse, an
// unsupported schema_version, or a non-recoverable write error. Those are
// non-retryable, so failing fast preserves losslessness rather than dropping the
// record.
func (s *Sink) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		env, err := s.opts.Reader.Next()
		if errors.Is(err, io.EOF) {
			s.log.Info("stdin closed; all records written")
			return nil
		}
		if err != nil {
			return fmt.Errorf("read record: %w", err)
		}
		s.opts.Metrics.IncConsumed()

		if !s.opts.Filter.Allow(env) {
			s.opts.Metrics.IncFiltered()
			continue
		}

		// Copy the raw bytes: Reader.Raw is valid only until the next Next, and a
		// retry loop holds it across backoff sleeps.
		payload := append([]byte(nil), s.opts.Reader.Raw()...)

		if err := s.writeWithRetry(ctx, payload); err != nil {
			return err
		}
		s.opts.Metrics.IncWritten(string(env.Type))
	}
}

// writeWithRetry appends payload, retrying transient (disk-full) failures with
// blocking exponential backoff plus full jitter until the write succeeds or ctx
// is cancelled. A permanent failure returns immediately. The write (and fsync,
// when enabled) is confirmed before this returns nil, so the caller advances the
// cursor only after the record is durably recorded.
func (s *Sink) writeWithRetry(ctx context.Context, payload []byte) error {
	attempt := 0
	blockedSince := time.Time{}
	for {
		if ctx.Err() != nil {
			return nil
		}
		start := s.now()
		_, err := s.opts.Writer.Write(payload)
		s.opts.Metrics.ObserveWrite(s.now().Sub(start))
		if err == nil {
			if attempt > 0 {
				s.log.Info("disk recovered", "after_failures", attempt)
				s.clearBlocked()
			}
			s.opts.Health.SetWritable(true)
			s.opts.Metrics.SetConsecutiveFailures(0)
			s.opts.Metrics.SetCurrentSizeBytes(s.opts.Writer.Size())
			return nil
		}

		class := Classify(err)
		s.opts.Metrics.IncFailed(string(class))
		if class == ClassPermanent {
			s.clearBlocked()
			return fmt.Errorf("permanent write failure: %w", err)
		}

		// Transient (disk full): back off and retry, blocking (lossless backpressure).
		attempt++
		if blockedSince.IsZero() {
			blockedSince = start
		}
		blocked := s.now().Sub(blockedSince)
		s.opts.Metrics.SetBlocked(true)
		s.opts.Metrics.SetConsecutiveFailures(attempt)
		s.opts.Metrics.IncRetry()
		s.opts.Health.SetWritable(false)
		s.opts.Health.SetWriteBlocked(blocked)

		backoff := s.backoffFor(attempt)
		s.opts.Metrics.SetBackoffSeconds(backoff)
		s.log.Warn("write failed; backing off and retrying",
			"error_type", string(class),
			"attempt", attempt,
			"backoff", backoff.String(),
			"blocked", blocked.String(),
		)
		if !sleepCtx(ctx, backoff) {
			return nil // ctx cancelled during backoff: clean stop.
		}
	}
}

// clearBlocked resets the blocked gauge/health after a recovery.
func (s *Sink) clearBlocked() {
	s.opts.Metrics.SetBlocked(false)
	s.opts.Metrics.SetBackoffSeconds(0)
	s.opts.Health.SetWriteBlocked(0)
}

// backoffFor computes base*2^(attempt-1), capped at BackoffMax, with full jitter
// (a uniform value in [d/2, d]).
func (s *Sink) backoffFor(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := s.opts.BackoffBase
	for i := 1; i < attempt && d < s.opts.BackoffMax; i++ {
		d *= 2
	}
	if d > s.opts.BackoffMax {
		d = s.opts.BackoffMax
	}
	return s.jitter(d)
}

func (s *Sink) jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	half := d / 2
	return half + time.Duration(s.opts.randInt(int64(half)+1))
}

// sleepCtx sleeps for d unless ctx is cancelled first; returns false on cancel.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// FailureClass categorizes a write error as transient (retry) or permanent
// (fail fast).
type FailureClass string

// Failure classes.
const (
	// ClassTransient is a retryable failure: a full disk (ENOSPC/EDQUOT). Blocking
	// and retrying lets the sink resume losslessly once space frees.
	ClassTransient FailureClass = "transient"
	// ClassPermanent is non-retryable: any other write error (a bad path, lost
	// mount, permission, or I/O fault). Retrying will not help, so the sink fails
	// fast rather than looping forever — the unwritten record stays in the pipe.
	ClassPermanent FailureClass = "permanent"
)

// Classify reduces a write error to its retry disposition. Only a full disk is
// transient; everything else is permanent. (This is the inverse default of the
// webhook sink, where remote failures are usually transient — a local write that
// fails for any reason other than "no space" signals a real, standing fault.)
func Classify(err error) FailureClass {
	if err == nil {
		return ClassTransient // not reached; callers check nil first.
	}
	if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) {
		return ClassTransient
	}
	var te *TransientError
	if errors.As(err, &te) {
		return ClassTransient
	}
	return ClassPermanent
}

// TransientError marks a write failure as retryable even when it is not a raw
// ENOSPC/EDQUOT (e.g. a fake writer in tests, or a wrapper that knows the
// condition is temporary). The real writer relies on the syscall checks instead.
type TransientError struct {
	Reason string
	Err    error
}

func (e *TransientError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("transient: %s: %v", e.Reason, e.Err)
	}
	return "transient: " + e.Reason
}

func (e *TransientError) Unwrap() error { return e.Err }

// noopMetrics satisfies Metrics with no-ops.
type noopMetrics struct{}

func (noopMetrics) IncConsumed()                    {}
func (noopMetrics) IncFiltered()                    {}
func (noopMetrics) IncWritten(string)               {}
func (noopMetrics) IncFailed(string)                {}
func (noopMetrics) ObserveWrite(time.Duration)      {}
func (noopMetrics) IncRetry()                       {}
func (noopMetrics) SetCurrentSizeBytes(int64)       {}
func (noopMetrics) SetBackoffSeconds(time.Duration) {}
func (noopMetrics) SetBlocked(bool)                 {}
func (noopMetrics) SetConsecutiveFailures(int)      {}

// noopHealth satisfies Healther with no-ops.
type noopHealth struct{}

func (noopHealth) SetWriteBlocked(time.Duration) {}
func (noopHealth) SetWritable(bool)              {}
