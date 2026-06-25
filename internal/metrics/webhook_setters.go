package metrics

import "time"

// SetUp marks the sink process available (true) or not.
func (w *Webhook) SetUp(up bool) { w.up.Set(b2f(up)) }

// SetWorkers records the active sink worker count.
func (w *Webhook) SetWorkers(n int) { w.workers.Set(float64(n)) }

// IncConsumed counts one record read from stdin.
func (w *Webhook) IncConsumed() { w.consumed.Inc() }

// IncQuarantined counts one poison record routed to the dead-letter file.
func (w *Webhook) IncQuarantined() { w.quarantined.Inc() }

// IncFiltered counts one record dropped by the configured filters.
func (w *Webhook) IncFiltered() { w.filtered.Inc() }

// IncForwarded counts one record confirmed forwarded (HTTP 2xx) of the given type.
func (w *Webhook) IncForwarded(recordType string) { w.forwarded.WithLabelValues(recordType).Inc() }

// IncFailed counts one POST failure of the given coarse error type.
func (w *Webhook) IncFailed(errorType string) { w.failed.WithLabelValues(errorType).Inc() }

// IncRetry counts one POST retry attempt after a transient failure.
func (w *Webhook) IncRetry() { w.retries.Inc() }

// ObservePost records one HTTP POST duration.
func (w *Webhook) ObservePost(d time.Duration) { w.postDuration.Observe(d.Seconds()) }

// SetBackoffSeconds records the current retry backoff.
func (w *Webhook) SetBackoffSeconds(d time.Duration) { w.backoffSeconds.Set(d.Seconds()) }

// SetBlocked records whether the sink is currently blocked retrying an endpoint.
func (w *Webhook) SetBlocked(blocked bool) { w.blocked.Set(b2f(blocked)) }

// SetConsecutiveFailures records the current consecutive POST failure count.
func (w *Webhook) SetConsecutiveFailures(n int) { w.consecutiveFail.Set(float64(n)) }

// WebhookHealth adapts the shared *Health to the sink's readiness surface. A
// sink has no chain lag, so it maps endpoint reachability onto the RPC-reachable
// signal and post-blocked onto the emit-blocked signal — reusing the same
// /readyz logic and HTTP server as the producers. Build the backing Health with
// a post-blocked threshold and a zero lag threshold (lag disabled).
type WebhookHealth struct{ h *Health }

// NewWebhookHealth wraps a Health (built with the post-blocked threshold and a
// zero lag threshold) for the webhook sink. It starts live and endpoint-reachable
// so /readyz is ready until a POST actually fails.
func NewWebhookHealth(h *Health) *WebhookHealth {
	h.SetRPCReachable(true)
	return &WebhookHealth{h: h}
}

// SetEmitBlocked records how long the current/last POST has been blocked retrying
// a failing endpoint; beyond the Health threshold /readyz flips not-ready.
func (w *WebhookHealth) SetEmitBlocked(d time.Duration) { w.h.SetEmitBlocked(d) }

// SetEndpointReachable records the latest endpoint reachability for /readyz.
func (w *WebhookHealth) SetEndpointReachable(v bool) { w.h.SetRPCReachable(v) }
