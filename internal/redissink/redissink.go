// Package redissink holds the evm-sink-redis core logic: read JSONL records from
// stdin via the shared record contract and append each one to a Redis Stream
// (XADD) with at-least-once durability.
//
// Delivery semantics: AT-LEAST-ONCE, and EFFECTIVELY EXACTLY-ONCE IN THE STREAM
// when dedup is enabled (the default). With dedup on, each append is gated by a
// per-record marker key derived from the record's documented dedup identity
// ([record.Envelope.DedupKey]) inside a single atomic Lua script, so a duplicate
// from a retry (or an overlapping re-run) is a no-op rather than a second stream
// entry. The append is confirmed before the stdin cursor advances, so a record is
// never dropped. A transient failure (connection loss, LOADING, CLUSTERDOWN, a
// network/timeout error) is retried with blocking exponential backoff plus full
// jitter — backpressure propagates up the pipe. A permanent failure (a WRONGTYPE
// stream key, an auth error) is non-retryable: the sink fails fast (exits
// non-zero) rather than spinning forever.
//
// The Redis write is behind the [Appender] interface so tests exercise the full
// run path with an in-memory fake; the real go-redis-backed appender lives in
// redis.go.
package redissink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"runtime/debug"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/daxchain-io/evm-tools/internal/record"
)

// Appender is the Redis-write surface the loop depends on. The real go-redis
// implementation lives in redis.go; tests substitute a fake. Append must block
// until Redis has acknowledged the write (or the call fails) so the loop can
// confirm-before-advance (at-least-once).
type Appender interface {
	// Append writes one record to the stream, idempotently when dedup is enabled,
	// returning nil only after Redis acknowledges. added reports whether a new
	// entry was created (false means it was deduplicated against a prior delivery).
	Append(ctx context.Context, env record.Envelope, raw []byte) (added bool, err error)
	// Reachable performs a PING for the active readiness probe; nil means reachable.
	Reachable(ctx context.Context) error
	// Target returns a redacted, log-safe description of the destination.
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
	Appender Appender
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

// Sink reads records and appends each one at-least-once to a Redis Stream.
type Sink struct {
	opts Options
	log  *slog.Logger
	now  func() time.Time
}

// New builds a Sink from resolved options.
func New(opts Options) (*Sink, error) {
	if opts.Reader == nil {
		return nil, errors.New("redissink: reader is required")
	}
	if opts.Appender == nil {
		return nil, errors.New("redissink: appender is required")
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

// Run reads records from stdin and appends each one, confirming the write before
// advancing. It returns nil on a clean EOF or cancelled context, and a non-nil
// error only on a permanent failure (an unparseable record or a non-retryable
// Redis error).
func (s *Sink) Run(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("recovered from panic in redis sink loop; stopping",
				"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			err = fmt.Errorf("redis sink panic: %v", r)
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
			s.log.Info("stdin closed; all records appended")
			return nil
		}
		if rerr != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read record: %w", rerr)
		}
		s.opts.Metrics.IncConsumed()

		appended, derr := s.appendWithRetry(ctx, env, payload)
		if derr != nil {
			return derr
		}
		if !appended {
			return nil // ctx cancelled before the write confirmed: clean stop
		}
		s.opts.Metrics.IncDelivered(string(env.Type))
	}
}

// appendWithRetry appends one record, retrying transient failures with blocking
// backoff until Redis confirms or ctx is cancelled. Returns (true, nil) only
// after a confirmed write, (false, nil) on a ctx-cancel path, and (false, err) on
// a permanent failure.
func (s *Sink) appendWithRetry(ctx context.Context, env record.Envelope, payload []byte) (bool, error) {
	attempt := 0
	blockedSince := time.Time{}
	for {
		if ctx.Err() != nil {
			return false, nil
		}
		start := s.now()
		added, aerr := s.opts.Appender.Append(ctx, env, payload)
		s.opts.Metrics.ObserveDeliver(s.now().Sub(start))
		if aerr == nil {
			if attempt > 0 {
				s.log.Info("redis recovered", "after_failures", attempt)
				s.clearBlocked()
			}
			if !added {
				s.log.Debug("record deduplicated; entry already present", "type", string(env.Type))
			}
			s.opts.Health.SetReachable(true)
			s.opts.Metrics.SetConsecutiveFailures(0)
			return true, nil
		}
		if ctx.Err() != nil {
			return false, nil
		}

		class := Classify(aerr)
		s.opts.Metrics.IncFailed(string(class))
		if class == ClassPermanent {
			s.clearBlocked()
			return false, fmt.Errorf("permanent append failure to %s: %w", s.opts.Appender.Target(), aerr)
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
		s.log.Warn("append failed; backing off and retrying",
			"error_type", string(class),
			"target", s.opts.Appender.Target(),
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
	err := s.opts.Appender.Reachable(pctx)
	s.opts.Health.SetReachable(err == nil)
	if err != nil && ctx.Err() == nil {
		s.log.Debug("redis reachability probe failed", "error_type", string(Classify(err)))
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

// FailureClass categorizes an append error as transient (retry) or permanent
// (fail fast).
type FailureClass string

// Failure classes.
const (
	ClassTransient FailureClass = "transient"
	ClassPermanent FailureClass = "permanent"
)

// permanentRedisErrors are server-error prefixes that retrying cannot fix: a
// wrong-type stream key, an auth/permission failure, or a CROSSSLOT error (the
// single-node client was pointed at a Redis Cluster node, where the stream key and
// the dedup marker key hash to different slots — a deployment misconfiguration, not
// a transient state). Everything else — including LOADING/CLUSTERDOWN/MASTERDOWN/
// TRYAGAIN/READONLY and any network/timeout error — is transient so the never-drop
// posture holds. evm-sink-redis targets a standalone/Sentinel Redis, not Cluster.
var permanentRedisErrors = []string{"WRONGTYPE", "NOAUTH", "WRONGPASS", "NOPERM", "CROSSSLOT"}

// Classify reduces a Redis append error to its retry disposition. A redis.Error
// (a server-side reply error) is permanent only for the known-unrecoverable
// prefixes above; all other errors, including connection/timeout failures and
// transient server states, are transient.
func Classify(err error) FailureClass {
	if err == nil {
		return ClassTransient // not reached; callers check nil first.
	}
	var rerr redis.Error
	if errors.As(err, &rerr) {
		msg := strings.ToUpper(rerr.Error())
		for _, p := range permanentRedisErrors {
			if strings.HasPrefix(msg, p) {
				return ClassPermanent
			}
		}
		return ClassTransient
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
