package metrics

import "time"

// SetUp marks the sink process available (true) or not.
func (k *Kafka) SetUp(up bool) { k.up.Set(b2f(up)) }

// SetWorkers records the active sink worker count.
func (k *Kafka) SetWorkers(n int) { k.workers.Set(float64(n)) }

// IncConsumed counts one record read from stdin.
func (k *Kafka) IncConsumed() { k.consumed.Inc() }

// IncQuarantined counts one poison record routed to the dead-letter file.
func (k *Kafka) IncQuarantined() { k.quarantined.Inc() }

// IncPublished counts one record confirmed published to the given topic.
func (k *Kafka) IncPublished(topic string) { k.published.WithLabelValues(topic).Inc() }

// IncFailed counts one publish failure of the given coarse error type.
func (k *Kafka) IncFailed(errorType string) { k.failed.WithLabelValues(errorType).Inc() }

// IncRetry counts one publish retry attempt after a transient failure.
func (k *Kafka) IncRetry() { k.retries.Inc() }

// ObservePublish records one broker publish duration.
func (k *Kafka) ObservePublish(d time.Duration) { k.publishDuration.Observe(d.Seconds()) }

// SetBackoffSeconds records the current retry backoff.
func (k *Kafka) SetBackoffSeconds(d time.Duration) { k.backoffSeconds.Set(d.Seconds()) }

// SetBlocked records whether the sink is currently blocked retrying a broker.
func (k *Kafka) SetBlocked(blocked bool) { k.blocked.Set(b2f(blocked)) }

// SetConsecutiveFailures records the current consecutive publish failure count.
func (k *Kafka) SetConsecutiveFailures(n int) { k.consecutiveFail.Set(float64(n)) }

// KafkaHealth adapts the shared *Health to the sink's readiness surface. A sink
// has no chain lag, so it maps broker reachability onto the RPC-reachable signal
// and publish-blocked onto the emit-blocked signal — reusing the same /readyz
// logic and HTTP server as the producers. Build the backing Health with a
// publish-blocked threshold and a zero lag threshold (lag disabled).
type KafkaHealth struct{ h *Health }

// NewKafkaHealth wraps a Health (built with the publish-blocked threshold and a
// zero lag threshold) for the kafka sink. It starts live and broker-reachable so
// /readyz is ready until a publish actually fails.
func NewKafkaHealth(h *Health) *KafkaHealth {
	h.SetRPCReachable(true)
	return &KafkaHealth{h: h}
}

// SetPublishBlocked records how long the current/last publish has been blocked
// retrying a failing broker; beyond the Health threshold /readyz flips not-ready.
func (k *KafkaHealth) SetPublishBlocked(d time.Duration) { k.h.SetEmitBlocked(d) }

// SetBrokerReachable records the latest broker reachability for /readyz.
func (k *KafkaHealth) SetBrokerReachable(v bool) { k.h.SetRPCReachable(v) }
