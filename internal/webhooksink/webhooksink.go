// Package webhooksink holds the evm-sink-webhook core logic: read JSONL records
// from stdin via the shared record contract and forward each one over HTTP to a
// configured URL with at-least-once delivery.
//
// Scope (see docs/design.md Open Question 1, settled for this build): a FORWARDER
// with OPTIONAL FILTERS — POST every record by default; optional include/exclude
// by record type and name plus a single simple field condition (eq/gt/lt on a
// named data field). It is NOT a rule DSL.
//
// Delivery semantics: AT-LEAST-ONCE. Every POST is confirmed (2xx) before the
// stdin cursor advances, so a record is never dropped. A transient failure
// (network, timeout, HTTP 5xx) is retried with blocking exponential backoff plus
// full jitter — backpressure propagates up the pipe to the lossless producer
// rather than buffering without bound. A permanent HTTP 4xx means retrying won't
// help: the sink fails fast (exits non-zero) rather than silently dropping the
// record (preserves losslessness). Duplicates on retry are acceptable; consumers
// dedup via the record's documented key ([record.Envelope.DedupKey]).
//
// The actual HTTP delivery is behind the [Poster] interface so default tests use
// net/http/httptest (no external endpoint); the real net/http poster lives in
// poster.go behind that interface.
package webhooksink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"runtime/debug"
	"time"

	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// Metrics is the subset of *metrics.Webhook the sink reports to. A nil Metrics is
// tolerated via noopMetrics so tests need not wire one.
type Metrics interface {
	IncConsumed()
	IncFiltered()
	IncForwarded(recordType string)
	IncFailed(et string)
	ObservePost(d time.Duration)
	IncRetry()
	SetBackoffSeconds(d time.Duration)
	SetBlocked(blocked bool)
	SetConsecutiveFailures(n int)
}

// Healther is the readiness surface the loop updates. /readyz flips to not-ready
// while the sink has been blocked on a failing endpoint beyond its threshold.
type Healther interface {
	SetPostBlocked(d time.Duration)
	SetEndpointReachable(v bool)
}

// Options configures a Sink.
type Options struct {
	Reader  *record.Reader
	Poster  Poster
	Filter  *Filter
	Metrics Metrics
	Health  Healther
	Logger  *slog.Logger

	// BackoffBase / BackoffMax bound the blocking exponential backoff on a
	// transient POST failure. Zero values fall back to built-in defaults.
	BackoffBase time.Duration
	BackoffMax  time.Duration

	// ProbeInterval, when > 0, enables an active endpoint-reachability probe (a
	// GET of the configured health URL) that refreshes readiness on this interval
	// even while no records are flowing. ProbeTimeout bounds a single probe; it is
	// defaulted from ProbeInterval when unset.
	ProbeInterval time.Duration
	ProbeTimeout  time.Duration

	// now and randInt are injectable for deterministic tests.
	now     func() time.Time
	randInt func(n int64) int64
}

// Sink reads records and forwards them at-least-once over HTTP.
type Sink struct {
	opts Options
	log  *slog.Logger
	now  func() time.Time
}

// New builds a Sink from resolved options.
func New(opts Options) (*Sink, error) {
	if opts.Reader == nil {
		return nil, errors.New("webhooksink: reader is required")
	}
	if opts.Poster == nil {
		return nil, errors.New("webhooksink: poster is required")
	}
	if opts.Filter == nil {
		// A nil filter means "forward everything"; build the permissive default
		// so the loop need not nil-check.
		f, err := NewFilter(FilterOptions{})
		if err != nil {
			return nil, err
		}
		opts.Filter = f
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

// Run reads records from stdin and forwards each one (that passes the filter),
// confirming the POST before advancing to the next record. It returns nil on a
// clean EOF (the producer closed the pipe) or a cancelled context, and a non-nil
// error only on a permanent failure: a record this build cannot parse, an
// unsupported schema_version, or a permanent HTTP 4xx. Those are non-retryable,
// so failing fast preserves losslessness rather than silently dropping the
// record.
func (s *Sink) Run(ctx context.Context) (err error) {
	// Convert a panic into a terminal error so the caller's graceful shutdown
	// (poster close, metrics server stop) still runs and the process exits
	// non-zero for a supervisor restart, rather than crashing abruptly.
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("recovered from panic in webhook sink loop; stopping",
				"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			err = fmt.Errorf("webhook sink panic: %v", r)
		}
	}()
	// Active readiness probe: keep /readyz reflecting endpoint reachability even
	// while no records flow. Cancelled and joined before Run returns.
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
		env, err := s.opts.Reader.Next()
		if errors.Is(err, io.EOF) {
			s.log.Info("stdin closed; all records forwarded")
			return nil
		}
		if err != nil {
			// A malformed line or unsupported schema is permanent: the stream is
			// the contract, so we fail fast rather than skip a record.
			return fmt.Errorf("read record: %w", err)
		}
		s.opts.Metrics.IncConsumed()

		if !s.opts.Filter.Allow(env) {
			// Filtered out: not an error, not forwarded. The cursor still advances
			// (a dropped-by-filter record is intentional, not a delivery loss).
			s.opts.Metrics.IncFiltered()
			continue
		}

		// Copy the raw bytes: Reader.Raw is valid only until the next Next, and a
		// retry loop holds it across backoff sleeps.
		payload := append([]byte(nil), s.opts.Reader.Raw()...)

		posted, err := s.postWithRetry(ctx, payload)
		if err != nil {
			return err
		}
		if !posted {
			// ctx cancelled mid-retry before the server confirmed the record:
			// a clean stop, not a delivery. Don't count it.
			return nil
		}
		s.opts.Metrics.IncForwarded(string(env.Type))
	}
}

// postWithRetry POSTs payload, retrying transient failures with blocking
// exponential backoff plus full jitter until the POST succeeds or ctx is
// cancelled. A permanent failure (HTTP 4xx) returns immediately. It reports
// posted=true only after the POST is confirmed (2xx), so the caller advances the
// cursor and counts the record only once the server has it; a cancellation
// before that returns posted=false, nil (a clean stop).
func (s *Sink) postWithRetry(ctx context.Context, payload []byte) (posted bool, err error) {
	attempt := 0
	blockedSince := time.Time{}
	for {
		if ctx.Err() != nil {
			return false, nil
		}
		start := s.now()
		perr := s.opts.Poster.Post(ctx, payload)
		s.opts.Metrics.ObservePost(s.now().Sub(start))
		if perr == nil {
			if attempt > 0 {
				s.log.Info("endpoint recovered", "after_failures", attempt)
				s.clearBlocked()
			}
			s.opts.Health.SetEndpointReachable(true)
			s.opts.Metrics.SetConsecutiveFailures(0)
			return true, nil
		}
		if ctx.Err() != nil {
			// Cancelled mid-POST: a clean shutdown, not a failure or a delivery.
			return false, nil
		}

		class := Classify(perr)
		s.opts.Metrics.IncFailed(string(class))
		if class == ClassPermanent {
			s.clearBlocked()
			return false, fmt.Errorf("permanent POST failure: %w", perr)
		}

		// Transient: back off and retry, blocking (lossless backpressure).
		attempt++
		if blockedSince.IsZero() {
			blockedSince = start
		}
		blocked := s.now().Sub(blockedSince)
		s.opts.Metrics.SetBlocked(true)
		s.opts.Metrics.SetConsecutiveFailures(attempt)
		s.opts.Metrics.IncRetry()
		s.opts.Health.SetEndpointReachable(false)
		s.opts.Health.SetPostBlocked(blocked)

		backoff := s.backoffFor(attempt)
		s.opts.Metrics.SetBackoffSeconds(backoff)
		s.log.Warn("POST failed; backing off and retrying",
			"error_type", string(class),
			"attempt", attempt,
			"backoff", backoff.String(),
			"blocked", blocked.String(),
		)
		if !sleepCtx(ctx, backoff) {
			return false, nil // ctx cancelled during backoff: clean stop, nothing posted.
		}
	}
}

// clearBlocked resets the blocked gauge/health after a recovery.
func (s *Sink) clearBlocked() {
	s.opts.Metrics.SetBlocked(false)
	s.opts.Metrics.SetBackoffSeconds(0)
	s.opts.Health.SetPostBlocked(0)
}

// probeLoop actively checks endpoint reachability on ProbeInterval and records
// the result in readiness, so /readyz reflects the endpoint even when no records
// are flowing. It probes once immediately so startup readiness is live without
// waiting for a tick, then on each interval until ctx is cancelled.
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

// probeOnce runs one bounded reachability check and updates readiness. It logs
// only a coarse error_type (never the URL, body, or any secret) on failure, and
// is safe to run concurrently with the post loop's own readiness updates — both
// go through the atomic Health setter.
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
	err := s.opts.Poster.Reachable(pctx)
	s.opts.Health.SetEndpointReachable(err == nil)
	if err != nil && ctx.Err() == nil {
		s.log.Debug("endpoint reachability probe failed", "error_type", string(Classify(err)))
	}
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

// FailureClass categorizes a POST error as transient (retry) or permanent
// (fail fast).
type FailureClass string

// Failure classes.
const (
	// ClassTransient is a retryable failure: network error, timeout, HTTP 5xx.
	ClassTransient FailureClass = "transient"
	// ClassPermanent is non-retryable: an HTTP 4xx (client error) — retrying will
	// not fix it, so the sink fails fast rather than looping forever.
	ClassPermanent FailureClass = "permanent"
)

// Classify reduces a POST error to its retry disposition. It defaults to
// transient (retry) so a never-drop posture is the safe default; only errors
// known to be permanent (an HTTP 4xx, wrapped in *PermanentError) fail fast.
func Classify(err error) FailureClass {
	if err == nil {
		return ClassTransient // not reached; callers check nil first.
	}
	var pe *PermanentError
	if errors.As(err, &pe) {
		return ClassPermanent
	}
	// A 5xx surfaces as *transientHTTPError; everything else (transport/timeout)
	// reuses the shared RPC-style classification, all of which is transient for a
	// sink (retry until the endpoint recovers).
	var te *transientHTTPError
	if errors.As(err, &te) {
		return ClassTransient
	}
	switch rpc.Classify(err) {
	case rpc.ErrorTimeout, rpc.ErrorConnection, rpc.ErrorUnknown, rpc.ErrorRPC, rpc.ErrorDecode, rpc.ErrorNone:
		return ClassTransient
	default:
		return ClassTransient
	}
}

// PermanentError marks a POST failure as non-retryable so the sink fails fast
// instead of looping forever. The real poster wraps an HTTP 4xx (and an
// unbuildable request) in this type.
type PermanentError struct {
	Reason string
	Err    error
}

func (e *PermanentError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("permanent: %s: %v", e.Reason, e.Err)
	}
	return "permanent: " + e.Reason
}

func (e *PermanentError) Unwrap() error { return e.Err }

// noopMetrics satisfies Metrics with no-ops.
type noopMetrics struct{}

func (noopMetrics) IncConsumed()                    {}
func (noopMetrics) IncFiltered()                    {}
func (noopMetrics) IncForwarded(string)             {}
func (noopMetrics) IncFailed(string)                {}
func (noopMetrics) ObservePost(time.Duration)       {}
func (noopMetrics) IncRetry()                       {}
func (noopMetrics) SetBackoffSeconds(time.Duration) {}
func (noopMetrics) SetBlocked(bool)                 {}
func (noopMetrics) SetConsecutiveFailures(int)      {}

// noopHealth satisfies Healther with no-ops.
type noopHealth struct{}

func (noopHealth) SetPostBlocked(time.Duration) {}
func (noopHealth) SetEndpointReachable(bool)    {}
