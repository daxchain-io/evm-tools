package metrics

import "time"

// SetUp marks the sink process available (true) or not.
func (f *File) SetUp(up bool) { f.up.Set(b2f(up)) }

// SetWorkers records the active sink worker count.
func (f *File) SetWorkers(n int) { f.workers.Set(float64(n)) }

// IncConsumed counts one record read from stdin.
func (f *File) IncConsumed() { f.consumed.Inc() }

// IncQuarantined counts one poison record routed to the dead-letter file.
func (f *File) IncQuarantined() { f.quarantined.Inc() }

// IncFiltered counts one record dropped by the configured filters.
func (f *File) IncFiltered() { f.filtered.Inc() }

// IncWritten counts one record confirmed written of the given type.
func (f *File) IncWritten(recordType string) { f.written.WithLabelValues(recordType).Inc() }

// IncFailed counts one write failure of the given coarse error type.
func (f *File) IncFailed(errorType string) { f.failed.WithLabelValues(errorType).Inc() }

// IncRetry counts one write retry attempt after a transient failure.
func (f *File) IncRetry() { f.retries.Inc() }

// ObserveWrite records one record-write duration.
func (f *File) ObserveWrite(d time.Duration) { f.writeDuration.Observe(d.Seconds()) }

// SetBackoffSeconds records the current retry backoff.
func (f *File) SetBackoffSeconds(d time.Duration) { f.backoffSeconds.Set(d.Seconds()) }

// SetBlocked records whether the sink is currently blocked retrying a failing disk.
func (f *File) SetBlocked(blocked bool) { f.blocked.Set(b2f(blocked)) }

// SetConsecutiveFailures records the current consecutive write failure count.
func (f *File) SetConsecutiveFailures(n int) { f.consecutiveFail.Set(float64(n)) }

// IncRotation counts one active-file rotation. Wired to the writer's OnRotate hook.
func (f *File) IncRotation() { f.rotations.Inc() }

// SetCurrentSizeBytes records the active output file's current size.
func (f *File) SetCurrentSizeBytes(n int64) { f.currentSize.Set(float64(n)) }

// FileHealth adapts the shared *Health to the file sink's readiness surface. A
// sink has no chain lag, so it maps disk writability onto the RPC-reachable
// signal and write-blocked onto the emit-blocked signal — reusing the same
// /readyz logic and HTTP server as the producers. Build the backing Health with a
// write-blocked threshold and a zero lag threshold (lag disabled).
type FileHealth struct{ h *Health }

// NewFileHealth wraps a Health (built with the write-blocked threshold and a zero
// lag threshold) for the file sink. It starts live and writable so /readyz is
// ready until a write actually fails.
func NewFileHealth(h *Health) *FileHealth {
	h.SetRPCReachable(true)
	return &FileHealth{h: h}
}

// SetWriteBlocked records how long the current/last write has been blocked
// retrying a failing disk; beyond the Health threshold /readyz flips not-ready.
func (f *FileHealth) SetWriteBlocked(d time.Duration) { f.h.SetEmitBlocked(d) }

// SetWritable records the latest disk writability for /readyz.
func (f *FileHealth) SetWritable(v bool) { f.h.SetRPCReachable(v) }
