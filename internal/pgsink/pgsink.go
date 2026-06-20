// Package pgsink holds the evm-sink-postgres core logic: read JSONL records from
// stdin via the shared record contract and insert each one into a PostgreSQL
// table with at-least-once durability.
//
// Delivery semantics: AT-LEAST-ONCE that is effectively EXACTLY-ONCE in the
// table. Each row is inserted with ON CONFLICT (dedup_key) DO NOTHING keyed on the
// record's documented dedup identity ([record.Envelope.DedupKey]), so a duplicate
// from a retry (or a re-run over overlapping input) is a no-op rather than a
// duplicate row. The insert is confirmed before the stdin cursor advances, so a
// record is never dropped. A transient failure (connection loss, deadlock,
// serialization failure, insufficient resources) is retried with blocking
// exponential backoff plus full jitter — backpressure propagates up the pipe. A
// permanent failure (a schema/permission/data error) is non-retryable: the sink
// fails fast (exits non-zero) rather than spinning forever.
//
// The DB write is behind the [Inserter] interface so tests exercise the full run
// path with an in-memory fake; the real pgx-backed inserter lives in postgres.go.
package pgsink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"runtime/debug"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daxchain-io/evm-tools/internal/record"
)

// Inserter is the DB-write surface the loop depends on. The real pgx-backed
// implementation lives in postgres.go; tests substitute a fake. Insert must block
// until the row is committed (or the call fails) so the loop can
// confirm-before-advance (at-least-once).
type Inserter interface {
	// Insert writes one record (idempotently, ON CONFLICT DO NOTHING), returning
	// nil only after the row is committed.
	Insert(ctx context.Context, env record.Envelope, raw []byte) error
	// Reachable pings the database for the active readiness probe.
	Reachable(ctx context.Context) error
	// Target returns a redacted, log-safe DSN (host/port/db, never the password).
	Target() string
	Close() error
}

// Metrics is the subset of *metrics.SinkMetrics the loop reports to.
type Metrics interface {
	IncConsumed()
	IncDelivered(recordType string)
	IncFailed(et string)
	ObserveDeliver(d time.Duration)
	IncRetry()
	SetBackoffSeconds(d time.Duration)
	SetBlocked(blocked bool)
	SetConsecutiveFailures(n int)
}

// Healther is the readiness surface the loop updates.
type Healther interface {
	SetReachable(v bool)
	SetDeliverBlocked(d time.Duration)
}

// Options configures a Sink.
type Options struct {
	Reader   *record.Reader
	Inserter Inserter
	Metrics  Metrics
	Health   Healther
	Logger   *slog.Logger

	BackoffBase time.Duration
	BackoffMax  time.Duration

	ProbeInterval time.Duration
	ProbeTimeout  time.Duration

	now     func() time.Time
	randInt func(n int64) int64
}

// Sink reads records and inserts each one at-least-once into PostgreSQL.
type Sink struct {
	opts Options
	log  *slog.Logger
	now  func() time.Time
}

// New builds a Sink from resolved options.
func New(opts Options) (*Sink, error) {
	if opts.Reader == nil {
		return nil, errors.New("pgsink: reader is required")
	}
	if opts.Inserter == nil {
		return nil, errors.New("pgsink: inserter is required")
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
	if opts.ProbeInterval > 0 && opts.ProbeTimeout <= 0 {
		opts.ProbeTimeout = 10 * time.Second
		if opts.ProbeInterval < opts.ProbeTimeout {
			opts.ProbeTimeout = opts.ProbeInterval
		}
	}
	return &Sink{opts: opts, log: opts.Logger, now: opts.now}, nil
}

// Run reads records from stdin and inserts each one, confirming the commit before
// advancing. It returns nil on a clean EOF or cancelled context, and a non-nil
// error only on a permanent failure (an unparseable record or a non-retryable DB
// error).
func (s *Sink) Run(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("recovered from panic in postgres sink loop; stopping",
				"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			err = fmt.Errorf("postgres sink panic: %v", r)
		}
	}()

	if s.opts.ProbeInterval > 0 {
		pctx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() { defer close(done); s.probeLoop(pctx) }()
		defer func() {
			cancel()
			<-done
		}()
	}

	for {
		if ctx.Err() != nil {
			return nil
		}
		env, payload, rerr := s.opts.Reader.NextCtx(ctx)
		if errors.Is(rerr, io.EOF) {
			s.log.Info("stdin closed; all records inserted")
			return nil
		}
		if rerr != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read record: %w", rerr)
		}
		s.opts.Metrics.IncConsumed()

		inserted, ierr := s.insertWithRetry(ctx, env, payload)
		if ierr != nil {
			return ierr
		}
		if !inserted {
			return nil // ctx cancelled before commit: clean stop, don't count
		}
		s.opts.Metrics.IncDelivered(string(env.Type))
	}
}

// insertWithRetry inserts one record, retrying transient failures with blocking
// backoff until the commit succeeds or ctx is cancelled. Returns (true, nil) only
// after a confirmed commit, (false, nil) on a ctx-cancel path, and (false, err) on
// a permanent failure.
func (s *Sink) insertWithRetry(ctx context.Context, env record.Envelope, payload []byte) (bool, error) {
	attempt := 0
	blockedSince := time.Time{}
	for {
		if ctx.Err() != nil {
			return false, nil
		}
		start := s.now()
		ierr := s.opts.Inserter.Insert(ctx, env, payload)
		s.opts.Metrics.ObserveDeliver(s.now().Sub(start))
		if ierr == nil {
			if attempt > 0 {
				s.log.Info("database recovered", "after_failures", attempt)
				s.clearBlocked()
			}
			s.opts.Health.SetReachable(true)
			s.opts.Metrics.SetConsecutiveFailures(0)
			return true, nil
		}
		if ctx.Err() != nil {
			return false, nil
		}

		class := Classify(ierr)
		s.opts.Metrics.IncFailed(string(class))
		if class == ClassPermanent {
			s.clearBlocked()
			return false, fmt.Errorf("permanent insert failure into %s: %w", s.opts.Inserter.Target(), ierr)
		}

		attempt++
		if blockedSince.IsZero() {
			blockedSince = start
		}
		blocked := s.now().Sub(blockedSince)
		s.opts.Metrics.SetBlocked(true)
		s.opts.Metrics.SetConsecutiveFailures(attempt)
		s.opts.Metrics.IncRetry()
		s.opts.Health.SetReachable(false)
		s.opts.Health.SetDeliverBlocked(blocked)

		backoff := s.backoffFor(attempt)
		s.opts.Metrics.SetBackoffSeconds(backoff)
		s.log.Warn("insert failed; backing off and retrying",
			"error_type", string(class),
			"target", s.opts.Inserter.Target(),
			"attempt", attempt,
			"backoff", backoff.String(),
			"blocked", blocked.String(),
		)
		if !sleepCtx(ctx, backoff) {
			return false, nil
		}
	}
}

func (s *Sink) clearBlocked() {
	s.opts.Metrics.SetBlocked(false)
	s.opts.Metrics.SetBackoffSeconds(0)
	s.opts.Health.SetDeliverBlocked(0)
}

func (s *Sink) probeLoop(ctx context.Context) {
	s.probeOnce(ctx)
	t := time.NewTicker(s.opts.ProbeInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.probeOnce(ctx)
		}
	}
}

func (s *Sink) probeOnce(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	pctx := ctx
	if s.opts.ProbeTimeout > 0 {
		var cancel context.CancelFunc
		pctx, cancel = context.WithTimeout(ctx, s.opts.ProbeTimeout)
		defer cancel()
	}
	err := s.opts.Inserter.Reachable(pctx)
	s.opts.Health.SetReachable(err == nil)
	if err != nil && ctx.Err() == nil {
		s.log.Debug("database reachability probe failed", "error_type", string(Classify(err)))
	}
}

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

// FailureClass categorizes an insert error as transient (retry) or permanent
// (fail fast).
type FailureClass string

// Failure classes.
const (
	ClassTransient FailureClass = "transient"
	ClassPermanent FailureClass = "permanent"
)

// Classify reduces a PostgreSQL/pgx error to its retry disposition by SQLSTATE
// class: connection (08), insufficient resources (53), transaction rollback —
// serialization/deadlock (40), operator intervention (57), and lock-not-available
// (55) are transient; data (22), integrity (23), and syntax/access (42) errors are
// permanent. A non-PgError (a raw connection/network/timeout failure) defaults to
// transient so the never-drop posture holds.
func Classify(err error) FailureClass {
	if err == nil {
		return ClassTransient
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) && len(pg.Code) >= 2 {
		switch pg.Code[:2] {
		case "08", "53", "40", "57", "55":
			return ClassTransient
		default:
			return ClassPermanent
		}
	}
	return ClassTransient
}

// noopMetrics satisfies Metrics with no-ops.
type noopMetrics struct{}

func (noopMetrics) IncConsumed()                    {}
func (noopMetrics) IncDelivered(string)             {}
func (noopMetrics) IncFailed(string)                {}
func (noopMetrics) ObserveDeliver(time.Duration)    {}
func (noopMetrics) IncRetry()                       {}
func (noopMetrics) SetBackoffSeconds(time.Duration) {}
func (noopMetrics) SetBlocked(bool)                 {}
func (noopMetrics) SetConsecutiveFailures(int)      {}

// noopHealth satisfies Healther with no-ops.
type noopHealth struct{}

func (noopHealth) SetReachable(bool)               {}
func (noopHealth) SetDeliverBlocked(time.Duration) {}
