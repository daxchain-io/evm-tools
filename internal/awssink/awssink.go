// Package awssink holds the shared core for the AWS sinks (evm-sink-aws-sqs and
// evm-sink-aws-sns): read JSONL records from stdin via the shared record contract
// and deliver each one to AWS with at-least-once semantics. SQS (SendMessage) and
// SNS (Publish) differ only in the underlying API call behind the [Publisher]
// interface, so the run loop, retry/backoff, FIFO key derivation, size guard, and
// metrics/health live here once.
//
// Delivery semantics: AT-LEAST-ONCE. Every send is confirmed (a MessageId is
// returned) before the stdin cursor advances, so a record is never dropped. A
// transient failure (throttling, a 5xx, a network/timeout error) is retried with
// blocking exponential backoff plus full jitter — backpressure propagates up the
// pipe rather than dropping or buffering without bound. A permanent failure (a 4xx
// such as access denied, a non-existent queue/topic, or an oversize message) is
// non-retryable: the sink fails fast (exits non-zero) rather than silently
// dropping the record. Duplicates on retry are acceptable; consumers dedup via the
// record's documented key ([record.Envelope.DedupKey]). On a FIFO queue/topic the
// dedup key is also sent as the MessageDeduplicationId, so AWS itself collapses
// in-window duplicates.
//
// Credentials are never read from evm-tools config: the real publishers use the
// AWS SDK default credential chain (environment, shared config, IRSA/web identity,
// or an instance role), so no secret material lands in the file.
package awssink

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"runtime/debug"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/smithy-go"

	"github.com/daxchain-io/evm-tools/internal/record"
)

// fifoID derives a fixed-length, FIFO-safe MessageGroupId/MessageDeduplicationId
// from a record key by hashing it: 64 hex chars, well within the 128-char limit
// and restricted alphabet, and deterministic so duplicates collapse and per-group
// ordering is preserved.
func fifoID(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// maxMessageBytes is the SQS/SNS maximum message size (256 KiB). A record larger
// than this cannot be delivered and is a permanent failure (fail fast, never
// drop) rather than a retry that can never succeed.
const maxMessageBytes = 262144

// Message is one record to deliver. GroupID/DedupID are set only for a FIFO
// queue/topic (empty otherwise): GroupID preserves per-key ordering and DedupID
// lets AWS collapse in-window duplicates.
type Message struct {
	Body    []byte
	GroupID string
	DedupID string
}

// Publisher is the AWS-delivery surface the loop depends on. The real SQS/SNS
// implementations live in sqs.go/sns.go; tests substitute a fake. Publish must
// block until AWS has acknowledged the message (or the call fails) so the loop can
// confirm-before-advance (at-least-once).
type Publisher interface {
	// Publish delivers one message, returning nil only after AWS acknowledges it.
	Publish(ctx context.Context, msg Message) error
	// Reachable performs a read-only check (e.g. GetQueueAttributes /
	// GetTopicAttributes) that the destination exists and is reachable, for the
	// active readiness probe. nil means reachable.
	Reachable(ctx context.Context) error
	// Target returns a redacted, log-safe description of the destination.
	Target() string
	Close() error
}

// Metrics is the subset of *metrics.SinkMetrics the loop reports to. A nil Metrics
// is tolerated via noopMetrics so tests need not wire one.
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
	Reader    *record.Reader
	Publisher Publisher
	Metrics   Metrics
	Health    Healther
	Logger    *slog.Logger

	// FIFO marks the destination as a FIFO queue/topic, so each message carries a
	// MessageGroupId (the record's partition identity) and MessageDeduplicationId
	// (its dedup key).
	FIFO bool

	// BackoffBase / BackoffMax bound the blocking exponential backoff on a
	// transient failure. Zero values fall back to built-in defaults.
	BackoffBase time.Duration
	BackoffMax  time.Duration

	// ProbeInterval, when > 0, enables an active reachability probe so /readyz
	// reflects destination reachability even while no records flow. ProbeTimeout
	// bounds one probe; defaulted from ProbeInterval when unset.
	ProbeInterval time.Duration
	ProbeTimeout  time.Duration

	// now and randInt are injectable for deterministic tests.
	now     func() time.Time
	randInt func(n int64) int64
}

// Sink reads records and delivers them at-least-once to AWS.
type Sink struct {
	opts Options
	log  *slog.Logger
	now  func() time.Time
}

// New builds a Sink from resolved options.
func New(opts Options) (*Sink, error) {
	if opts.Reader == nil {
		return nil, errors.New("awssink: reader is required")
	}
	if opts.Publisher == nil {
		return nil, errors.New("awssink: publisher is required")
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

// Run reads records from stdin and delivers each one, confirming the send before
// advancing. It returns nil on a clean EOF or cancelled context, and a non-nil
// error only on a permanent failure (an unparseable record, an oversize message,
// or a non-retryable AWS error) or a broken downstream.
func (s *Sink) Run(ctx context.Context) (err error) {
	// Convert a panic into a terminal error so the caller's graceful shutdown
	// (publisher close, metrics server stop) still runs and the process exits
	// non-zero for a supervisor restart, rather than crashing abruptly.
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("recovered from panic in aws sink loop; stopping",
				"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			err = fmt.Errorf("aws sink panic: %v", r)
		}
	}()

	// Active readiness probe: keep /readyz reflecting destination reachability even
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
		env, payload, rerr := s.opts.Reader.NextCtx(ctx)
		if errors.Is(rerr, io.EOF) {
			s.log.Info("stdin closed; all records delivered")
			return nil
		}
		if rerr != nil {
			if ctx.Err() != nil {
				return nil // signal during a blocked read: clean stop
			}
			return fmt.Errorf("read record: %w", rerr)
		}
		s.opts.Metrics.IncConsumed()

		if len(payload) > maxMessageBytes {
			// Oversize: retrying cannot help — fail fast rather than drop.
			return fmt.Errorf("record exceeds the %d-byte AWS message limit (%d bytes); cannot deliver", maxMessageBytes, len(payload))
		}

		msg := Message{Body: payload}
		if s.opts.FIFO {
			// SQS/SNS cap MessageGroupId/MessageDeduplicationId at 128 chars with a
			// restricted alphabet; a raw DedupKey (66-char tx hash + fields) can
			// exceed that. Hash to a fixed 64-char hex id: deterministic, so AWS
			// still dedups in-window (same key -> same id) and orders per group.
			msg.GroupID = fifoID(env.PartitionIdentity())
			msg.DedupID = fifoID(env.DedupKey())
		}

		delivered, derr := s.deliverWithRetry(ctx, msg)
		if derr != nil {
			return derr
		}
		if !delivered {
			return nil // ctx cancelled before AWS confirmed: clean stop, don't count
		}
		s.opts.Metrics.IncDelivered(string(env.Type))
	}
}

// deliverWithRetry delivers msg, retrying transient failures with blocking
// exponential backoff plus full jitter until AWS acknowledges or ctx is cancelled.
// A permanent failure returns immediately. It returns (true, nil) only after AWS
// confirms, and (false, nil) on every ctx-cancel path (a clean stop that did NOT
// deliver), so the caller does not over-count.
func (s *Sink) deliverWithRetry(ctx context.Context, msg Message) (bool, error) {
	attempt := 0
	blockedSince := time.Time{}
	for {
		if ctx.Err() != nil {
			return false, nil
		}
		start := s.now()
		perr := s.opts.Publisher.Publish(ctx, msg)
		s.opts.Metrics.ObserveDeliver(s.now().Sub(start))
		if perr == nil {
			if attempt > 0 {
				s.log.Info("destination recovered", "after_failures", attempt)
				s.clearBlocked()
			}
			s.opts.Health.SetReachable(true)
			s.opts.Metrics.SetConsecutiveFailures(0)
			return true, nil
		}
		if ctx.Err() != nil {
			return false, nil // cancelled mid-publish: clean shutdown
		}

		class := Classify(perr)
		s.opts.Metrics.IncFailed(string(class))
		if class == ClassPermanent {
			s.clearBlocked()
			return false, fmt.Errorf("permanent delivery failure to %s: %w", s.opts.Publisher.Target(), perr)
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
		s.log.Warn("delivery failed; backing off and retrying",
			"error_type", string(class),
			"target", s.opts.Publisher.Target(),
			"attempt", attempt,
			"backoff", backoff.String(),
			"blocked", blocked.String(),
		)
		if !sleepCtx(ctx, backoff) {
			return false, nil // ctx cancelled during backoff: clean stop
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
	err := s.opts.Publisher.Reachable(pctx)
	s.opts.Health.SetReachable(err == nil)
	if err != nil && ctx.Err() == nil {
		s.log.Debug("destination reachability probe failed", "error_type", string(Classify(err)))
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

// FailureClass categorizes a delivery error as transient (retry) or permanent
// (fail fast).
type FailureClass string

// Failure classes.
const (
	// ClassTransient is retryable: throttling, a 5xx server fault, or a
	// network/timeout error.
	ClassTransient FailureClass = "transient"
	// ClassPermanent is non-retryable: a 4xx client fault (access denied, a
	// non-existent queue/topic, an invalid parameter) — retrying cannot help.
	ClassPermanent FailureClass = "permanent"
)

// awsRetryer is the SDK's standard error classifier. We reuse it so our transient
// set never drifts from the SDK's own — which already knows the full throttle code
// set, the retryable client codes (RequestTimeout / RequestTimeoutException), the
// retryable 5xx statuses, and connection/timeout errors. IsErrorRetryable is a
// pure classification call (it consumes no retry tokens) and is safe to reuse.
var awsRetryer = retry.NewStandard()

// Classify reduces an AWS delivery error to its retry disposition. It first
// delegates to the SDK classifier (throttling, request-timeout, retryable 5xx,
// and network/timeout errors are all transient). For anything the SDK does not
// deem retryable, a smithy client (4xx) fault is permanent (access denied, bad
// request, a non-existent queue/topic), a server fault is transient, and an
// unknown non-API error defaults to transient so the never-drop posture holds.
func Classify(err error) FailureClass {
	if err == nil {
		return ClassTransient // not reached; callers check nil first.
	}
	if awsRetryer.IsErrorRetryable(err) {
		return ClassTransient
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		if ae.ErrorFault() == smithy.FaultClient {
			return ClassPermanent
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
