package stream

import (
	"context"
	"math/rand"
	"time"

	"github.com/daxchain-io/evm-tools/internal/record"
)

// jitter applies full jitter to a backoff duration: a uniform random value in
// [d/2, d]. This spreads retries so a fleet does not thunder a recovering node.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	half := d / 2
	return half + time.Duration(rand.Int63n(int64(half)+1))
}

// sleepCtx sleeps for d unless ctx is cancelled first. It returns false when
// ctx was cancelled (the caller should stop), true on a full sleep.
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

// watchdogInterval is how often the in-flight write watchdog republishes the
// growing blocked duration. It must be well below the /readyz emit-blocked
// threshold so a genuinely wedged write trips readiness promptly rather than
// only after the (possibly never-returning) write completes.
const watchdogInterval = 250 * time.Millisecond

// blockTrackingEmitter wraps a record.Emitter and reports how long the current
// or last stdout write has been blocked by downstream backpressure. Because the
// underlying write blocks when the OS pipe fills, a write that never returns
// (a genuinely wedged sink) would never update the gauge if measured only on
// return — the very case the gauge exists to catch. So a concurrent watchdog,
// started before inner.Emit, periodically publishes now-start while the write is
// in flight, growing the gauge and tripping /readyz at the threshold; on return
// the watchdog is stopped and the final span is published.
//
// Emission stays lossless: this wrapper only measures and never drops or
// reorders records — the blocking write still propagates backpressure upstream.
type blockTrackingEmitter struct {
	inner   Emitter
	metrics Metrics
	health  Healther
	now     func() time.Time
	// interval is the watchdog republish cadence; 0 uses watchdogInterval.
	interval time.Duration
}

func newBlockTrackingEmitter(inner Emitter, m Metrics, h Healther, now func() time.Time) *blockTrackingEmitter {
	return &blockTrackingEmitter{inner: inner, metrics: m, health: h, now: now}
}

// Emit publishes the growing blocked duration of the underlying write. A
// watchdog goroutine started before the (potentially blocking) write ticks the
// gauge upward while the write is in flight, so a stuck write trips /readyz at
// the threshold even though inner.Emit has not yet returned. On completion the
// watchdog stops and the final measured span is published.
func (e *blockTrackingEmitter) Emit(env record.Envelope) error {
	interval := e.interval
	if interval <= 0 {
		interval = watchdogInterval
	}

	start := e.now()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				blocked := e.now().Sub(start)
				e.metrics.SetEmitBlockedSeconds(blocked.Seconds())
				e.health.SetEmitBlocked(blocked)
			}
		}
	}()

	err := e.inner.Emit(env)

	close(stop)
	<-done // ensure no watchdog tick lands after the final value below

	blocked := e.now().Sub(start)
	e.metrics.SetEmitBlockedSeconds(blocked.Seconds())
	e.health.SetEmitBlocked(blocked)
	return err
}

// noopMetrics satisfies Metrics with no-ops, so a Stream can run without a
// metrics registry (tests, metrics disabled).
type noopMetrics struct{}

func (noopMetrics) SetHead(uint64)                        {}
func (noopMetrics) SetHeadBlockTime(time.Time, time.Time) {}
func (noopMetrics) SetLastProcessedBlock(uint64)          {}
func (noopMetrics) SetLastEmittedBlock(uint64)            {}
func (noopMetrics) SetLagBlocks(uint64)                   {}
func (noopMetrics) SetEmitBlockedSeconds(float64)         {}
func (noopMetrics) IncEventRecord(string, string, string) {}
func (noopMetrics) IncSkippedLog()                        {}
func (noopMetrics) IncNativeTransferRecord()              {}
func (noopMetrics) IncReorgsDetected()                    {}
func (noopMetrics) IncReconnects()                        {}
func (noopMetrics) ObserveLoop(time.Duration)             {}
func (noopMetrics) SetConsecutiveFailures(int)            {}
func (noopMetrics) SetBackoffSeconds(time.Duration)       {}
func (noopMetrics) ObserveLogChunk(uint64, time.Duration) {}

// noopHealth satisfies Healther with no-ops.
type noopHealth struct{}

func (noopHealth) SetRPCReachable(bool)         {}
func (noopHealth) SetEmitBlocked(time.Duration) {}
func (noopHealth) SetLag(uint64)                {}
func (noopHealth) SetHeadBlockTime(time.Time)   {}
