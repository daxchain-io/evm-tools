// Package kafkasink holds the evm-sink-kafka core logic: read JSONL records from
// stdin via the shared record contract and publish each one to Kafka with
// at-least-once delivery.
//
// Delivery semantics (see docs/design.md "Sink delivery semantics", settled for
// this build): AT-LEAST-ONCE. The publisher is configured with RequiredAcks=all
// and every publish is confirmed before the stdin cursor advances, so a record
// is never dropped. A transient failure (broker unavailable, network, timeout)
// is retried with blocking exponential backoff plus full jitter — backpressure
// propagates up the pipe to the lossless producer rather than buffering without
// bound. Duplicates on retry are acceptable; consumers dedup via the record's
// documented key ([record.Envelope.DedupKey]).
//
// The actual broker publish is behind the [Publisher] interface so default tests
// use an in-memory fake (no real broker); the real segmentio/kafka-go writer
// lives in writer.go behind that interface, and a real-broker test sits behind a
// build tag.
package kafkasink

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

// Message is one record ready to publish: the topic it routes to, the partition
// key (per-key ordering), and the verbatim JSONL payload bytes. Keeping it
// transport-neutral lets the fake and the real kafka-go writer share the loop.
type Message struct {
	Topic string
	Key   []byte
	Value []byte
}

// Publisher is the broker-publish surface the sink loop depends on. The real
// implementation wraps a segmentio/kafka-go *Writer (RequiredAcks=all);
// tests substitute a fake. Publish must block until the broker has acknowledged
// the message (or the call fails), so the loop can confirm-before-advance.
//
// A returned error is classified by [Classify] into transient (retry) vs.
// permanent (fail fast). Publish must respect ctx cancellation.
type Publisher interface {
	Publish(ctx context.Context, msg Message) error
	// Reachable performs a lightweight, read-only check that the broker cluster
	// is reachable (a metadata request), returning nil when it answered. The
	// active readiness probe uses it so /readyz reflects broker health even while
	// no records are flowing.
	Reachable(ctx context.Context) error
	Close() error
}

// PartitionKeyMode selects how the partition key is derived from a record.
type PartitionKeyMode string

// Partition-key strategies.
const (
	// PartitionIdentity keys on the record's dedup identity (default), so every
	// record sharing a logical identity lands on one partition and per-key
	// ordering holds.
	PartitionIdentity PartitionKeyMode = "identity"
	// PartitionDedup keys on the full dedup key (identity plus the sample
	// disambiguator). Useful when a downstream wants per-exact-record keying.
	PartitionDedup PartitionKeyMode = "dedup"
	// PartitionNone sends no key — round-robin partitioning, no ordering.
	PartitionNone PartitionKeyMode = "none"
)

// Router maps a record to its destination topic and partition key.
type Router struct {
	defaultTopic string
	byType       map[string]string
	keyMode      PartitionKeyMode
}

// NewRouter builds a Router. defaultTopic is required; byType overrides it per
// record type; keyMode selects the partition-key strategy (empty defaults to
// identity).
func NewRouter(defaultTopic string, byType map[string]string, keyMode PartitionKeyMode) (*Router, error) {
	if defaultTopic == "" && len(byType) == 0 {
		return nil, errors.New("kafkasink: a default topic or a per-type topic map is required")
	}
	if keyMode == "" {
		keyMode = PartitionIdentity
	}
	switch keyMode {
	case PartitionIdentity, PartitionDedup, PartitionNone:
	default:
		return nil, fmt.Errorf("kafkasink: unsupported partition_key %q (want identity|dedup|none)", keyMode)
	}
	return &Router{defaultTopic: defaultTopic, byType: byType, keyMode: keyMode}, nil
}

// Route returns the topic and partition key for a record. A record whose type
// has no per-type mapping uses the default topic; if there is neither a per-type
// topic nor a default, it returns an error (a record with nowhere to go must not
// be silently dropped).
func (r *Router) Route(env record.Envelope) (topic string, key []byte, err error) {
	topic = r.defaultTopic
	if t, ok := r.byType[string(env.Type)]; ok && t != "" {
		topic = t
	}
	if topic == "" {
		return "", nil, fmt.Errorf("kafkasink: no topic for record type %q (set a default topic or map it in topic_by_type)", env.Type)
	}
	switch r.keyMode {
	case PartitionNone:
		key = nil
	case PartitionDedup:
		key = []byte(env.DedupKey())
	default: // PartitionIdentity
		key = []byte(env.PartitionIdentity())
	}
	return topic, key, nil
}

// Metrics is the subset of *metrics.Kafka the sink reports to. A nil Metrics is
// tolerated via noopMetrics so tests need not wire one.
type Metrics interface {
	IncConsumed()
	IncPublished(topic string)
	IncFailed(et string)
	ObservePublish(d time.Duration)
	IncRetry()
	SetBackoffSeconds(d time.Duration)
	SetBlocked(blocked bool)
	SetConsecutiveFailures(n int)
}

// Healther is the readiness surface the loop updates. /readyz flips to not-ready
// while the sink has been blocked on a failing broker beyond its threshold.
type Healther interface {
	SetPublishBlocked(d time.Duration)
	SetBrokerReachable(v bool)
}

// Options configures a Sink.
type Options struct {
	Reader    *record.Reader
	Publisher Publisher
	Router    *Router
	Metrics   Metrics
	Health    Healther
	Logger    *slog.Logger

	// BackoffBase / BackoffMax bound the blocking exponential backoff on a
	// transient publish failure. Zero values fall back to built-in defaults.
	BackoffBase time.Duration
	BackoffMax  time.Duration

	// ProbeInterval, when > 0, enables an active broker-reachability probe that
	// refreshes readiness on this interval even while no records are flowing, so
	// /readyz reflects the broker (not just the last publish outcome). Zero
	// disables the probe. ProbeTimeout bounds a single probe; it is defaulted
	// from ProbeInterval when unset.
	ProbeInterval time.Duration
	ProbeTimeout  time.Duration

	// now and randSrc are injectable for deterministic tests.
	now     func() time.Time
	randInt func(n int64) int64
}

// Sink reads records and publishes them at-least-once.
type Sink struct {
	opts Options
	log  *slog.Logger
	now  func() time.Time
}

// New builds a Sink from resolved options.
func New(opts Options) (*Sink, error) {
	if opts.Reader == nil {
		return nil, errors.New("kafkasink: reader is required")
	}
	if opts.Publisher == nil {
		return nil, errors.New("kafkasink: publisher is required")
	}
	if opts.Router == nil {
		return nil, errors.New("kafkasink: router is required")
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

// Run reads records from stdin and publishes each one, confirming the publish
// before advancing to the next record. It returns nil on a clean EOF (the
// producer closed the pipe) or a cancelled context, and a non-nil error only on
// a permanent failure: a record this build cannot parse, an unsupported
// schema_version, or a permanent broker rejection (a 4xx-equivalent). Those are
// non-retryable, so failing fast preserves losslessness rather than silently
// dropping the record.
func (s *Sink) Run(ctx context.Context) (err error) {
	// Convert a panic into a terminal error so the caller's graceful shutdown
	// (publisher close, metrics server stop) still runs and the process exits
	// non-zero for a supervisor restart, rather than crashing abruptly.
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("recovered from panic in kafka sink loop; stopping",
				"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			err = fmt.Errorf("kafka sink panic: %v", r)
		}
	}()
	// Active readiness probe: keep /readyz reflecting broker reachability even
	// while no records flow (an idle pipe makes no publish attempts, so the
	// publish-path signal alone would go stale). Cancelled and joined before Run
	// returns.
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
		// NextCtx makes the blocking read cancellable so a signal stops an idle
		// sink promptly (rather than blocking until stdin closes); it returns a
		// private copy of the raw bytes, valid across the retry/backoff below.
		env, value, err := s.opts.Reader.NextCtx(ctx)
		if errors.Is(err, io.EOF) {
			s.log.Info("stdin closed; all records published")
			return nil
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil // signal during a blocked read: clean stop
			}
			// A malformed line or unsupported schema is permanent: the stream is
			// the contract, so we fail fast rather than skip a record.
			return fmt.Errorf("read record: %w", err)
		}
		s.opts.Metrics.IncConsumed()

		topic, key, rerr := s.opts.Router.Route(env)
		if rerr != nil {
			return rerr
		}

		msg := Message{Topic: topic, Key: key, Value: value}

		published, err := s.publishWithRetry(ctx, msg)
		if err != nil {
			return err
		}
		if !published {
			// ctx was cancelled before the broker confirmed: a clean stop, not a
			// confirmed publish. Don't count it (published_total is the
			// at-least-once delivery evidence) — mirrors the file/webhook sinks.
			return nil
		}
		s.opts.Metrics.IncPublished(topic)
	}
}

// publishWithRetry publishes msg, retrying transient failures with blocking
// exponential backoff plus full jitter until the publish succeeds or ctx is
// cancelled. A permanent failure returns immediately. The publish is confirmed
// (RequiredAcks=all on the real writer) before this returns (true, nil), so the
// caller advances the cursor and counts the record only after the broker has it.
// It returns (false, nil) on every ctx-cancel path (a clean shutdown that did NOT
// confirm a publish) so the caller does not over-count published_total.
func (s *Sink) publishWithRetry(ctx context.Context, msg Message) (bool, error) {
	attempt := 0
	blockedSince := time.Time{}
	for {
		if ctx.Err() != nil {
			return false, nil
		}
		start := s.now()
		err := s.opts.Publisher.Publish(ctx, msg)
		s.opts.Metrics.ObservePublish(s.now().Sub(start))
		if err == nil {
			if attempt > 0 {
				s.log.Info("broker recovered", "after_failures", attempt)
				s.clearBlocked()
			}
			s.opts.Health.SetBrokerReachable(true)
			s.opts.Metrics.SetConsecutiveFailures(0)
			return true, nil
		}
		if ctx.Err() != nil {
			// Cancelled mid-publish: a clean shutdown, not a failure.
			return false, nil
		}

		class := Classify(err)
		s.opts.Metrics.IncFailed(string(class))
		if class == ClassPermanent {
			s.clearBlocked()
			return false, fmt.Errorf("permanent publish failure to topic %q: %w", msg.Topic, err)
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
		s.opts.Health.SetBrokerReachable(false)
		s.opts.Health.SetPublishBlocked(blocked)

		backoff := s.backoffFor(attempt)
		s.opts.Metrics.SetBackoffSeconds(backoff)
		s.log.Warn("publish failed; backing off and retrying",
			"error_type", string(class),
			"topic", msg.Topic,
			"attempt", attempt,
			"backoff", backoff.String(),
			"blocked", blocked.String(),
		)
		if !sleepCtx(ctx, backoff) {
			return false, nil // ctx cancelled during backoff: clean stop.
		}
	}
}

// clearBlocked resets the blocked gauge/health after a recovery.
func (s *Sink) clearBlocked() {
	s.opts.Metrics.SetBlocked(false)
	s.opts.Metrics.SetBackoffSeconds(0)
	s.opts.Health.SetPublishBlocked(0)
}

// probeLoop actively checks broker reachability on ProbeInterval and records the
// result in readiness, so /readyz reflects the broker even when no records are
// flowing. It probes once immediately so startup readiness is live without
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
// only a coarse error_type (never the broker error text or any secret) on
// failure. It is safe to run concurrently with the publish loop's own readiness
// updates — both go through the atomic Health setter.
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
	err := s.opts.Publisher.Reachable(pctx)
	s.opts.Health.SetBrokerReachable(err == nil)
	if err != nil && ctx.Err() == nil {
		s.log.Debug("broker reachability probe failed", "error_type", string(Classify(err)))
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

// FailureClass categorizes a publish error as transient (retry) or permanent
// (fail fast).
type FailureClass string

// Failure classes.
const (
	// ClassTransient is a retryable failure: broker unavailable, network error,
	// timeout, leader election, etc.
	ClassTransient FailureClass = "transient"
	// ClassPermanent is non-retryable: a malformed/too-large message or an
	// authorization/topic error that retrying will not fix.
	ClassPermanent FailureClass = "permanent"
)

// Classify reduces a publish error to its retry disposition. It defaults to
// transient (retry) so a never-drop posture is the safe default; only errors
// known to be permanent fail fast. A PermanentError (wrapped by the real writer
// for unrecoverable broker rejections) forces ClassPermanent.
func Classify(err error) FailureClass {
	if err == nil {
		return ClassTransient // not reached; callers check nil first.
	}
	var pe *PermanentError
	if errors.As(err, &pe) {
		return ClassPermanent
	}
	// Reuse the shared RPC-style classification for transport/timeout shapes;
	// all of those are transient for a sink (retry until the broker recovers).
	switch rpc.Classify(err) {
	case rpc.ErrorTimeout, rpc.ErrorConnection, rpc.ErrorUnknown, rpc.ErrorRPC, rpc.ErrorDecode, rpc.ErrorNone:
		return ClassTransient
	default:
		return ClassTransient
	}
}

// PermanentError marks a publish failure as non-retryable so the sink fails fast
// instead of looping forever. The real writer wraps unrecoverable broker
// rejections (e.g. message too large, unknown topic with auto-create off,
// authorization failure) in this type.
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
func (noopMetrics) IncPublished(string)             {}
func (noopMetrics) IncFailed(string)                {}
func (noopMetrics) ObservePublish(time.Duration)    {}
func (noopMetrics) IncRetry()                       {}
func (noopMetrics) SetBackoffSeconds(time.Duration) {}
func (noopMetrics) SetBlocked(bool)                 {}
func (noopMetrics) SetConsecutiveFailures(int)      {}

// noopHealth satisfies Healther with no-ops.
type noopHealth struct{}

func (noopHealth) SetPublishBlocked(time.Duration) {}
func (noopHealth) SetBrokerReachable(bool)         {}
