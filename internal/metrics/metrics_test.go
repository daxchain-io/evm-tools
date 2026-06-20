package metrics

import (
	"context"
	"io"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func newTestServer(t *testing.T, metricsEnabled bool, h *Health, reg *prometheus.Registry) *Server {
	t.Helper()
	srv, err := NewServer(ServerOptions{
		Addr:           "127.0.0.1:0",
		MetricsEnabled: metricsEnabled,
		MetricsPath:    "/metrics",
		Registry:       reg,
		Health:         h,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return srv
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func TestHealthzAlwaysServed(t *testing.T) {
	h := NewHealth(0, 0)
	srv := newTestServer(t, false, h, nil) // metrics disabled
	code, body := get(t, "http://"+srv.Addr()+"/healthz")
	if code != http.StatusOK {
		t.Errorf("/healthz = %d, body %s", code, body)
	}
	// /metrics is not served when disabled.
	code, _ = get(t, "http://"+srv.Addr()+"/metrics")
	if code != http.StatusNotFound {
		t.Errorf("/metrics should be 404 when disabled, got %d", code)
	}
}

func TestReadyzReflectsSignals(t *testing.T) {
	h := NewHealth(30*time.Second, 5000)
	srv := newTestServer(t, false, h, nil)
	base := "http://" + srv.Addr()

	// Not ready until RPC reachable.
	if code, _ := get(t, base+"/readyz"); code != http.StatusServiceUnavailable {
		t.Errorf("/readyz should be 503 before rpc reachable, got %d", code)
	}
	h.SetRPCReachable(true)
	if code, _ := get(t, base+"/readyz"); code != http.StatusOK {
		t.Errorf("/readyz should be 200 once reachable, got %d", code)
	}

	// Lag beyond threshold flips not-ready.
	h.SetLag(6000)
	if code, body := get(t, base+"/readyz"); code != http.StatusServiceUnavailable || !strings.Contains(body, "lag") {
		t.Errorf("/readyz should report lag, got %d %s", code, body)
	}
	h.SetLag(10)

	// Emit-blocked beyond threshold flips not-ready.
	h.SetEmitBlocked(time.Minute)
	if code, body := get(t, base+"/readyz"); code != http.StatusServiceUnavailable || !strings.Contains(body, "blocked") {
		t.Errorf("/readyz should report blocked, got %d %s", code, body)
	}
}

func TestMetricsEndpointServesStreamSet(t *testing.T) {
	s := NewStream("codex-chain", "4242")
	s.SetUp(true)
	s.SetConfiguredContracts(3)
	s.IncEventRecord("usdc", "0xabc", "Transfer")
	s.IncNativeTransferRecord()
	s.SetLagBlocks(12)
	// Record an RPC observation so the labelled histogram series materializes.
	s.RPCObserver()("eth_chainId", time.Millisecond, "")

	h := NewHealth(0, 0)
	srv := newTestServer(t, true, h, s.Registry())
	code, body := get(t, "http://"+srv.Addr()+"/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics = %d", code)
	}
	for _, want := range []string{
		`evm_stream_up{blockchain="codex-chain",chain_id="4242"} 1`,
		`evm_stream_configured_contracts{blockchain="codex-chain",chain_id="4242"} 3`,
		"evm_stream_records_emitted_total",
		"evm_stream_native_transfer_records_emitted_total",
		`evm_stream_contract_event_records_emitted_total{`,
		"blockchain_rpc_call_duration_seconds",
		"evm_stream_lag_blocks",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
	// Counters end in _total, gauges do not carry it.
	if strings.Contains(body, "evm_stream_lag_blocks_total") {
		t.Error("lag gauge should not have _total suffix")
	}
}

func TestMetricsEndpointServesBalanceSet(t *testing.T) {
	b := NewBalance("codex-chain", "4242")
	b.SetUp(true)
	b.SetConfiguredNative(2)
	b.SetConfiguredContracts(1)
	b.SetAccountBalanceWei("treasury", "0xabc", big.NewInt(4_200_000))
	b.SetAccountBalanceEth("treasury", "0xabc", 4.2)
	b.SetAccountTokenBalanceRaw("usdc", "0xabc", "usdc", "0xtok", big.NewInt(1_000_000))
	b.SetContractTokenTotalSupply("usdc", "0xtok", 50_000_000)
	b.SetContractTransferCount("usdc", "0xtok", 3)
	b.IncSampleRecord()
	b.IncChangeRecord()
	b.RPCObserver()("eth_call", time.Millisecond, "")

	h := NewHealth(0, 0)
	srv := newTestServer(t, true, h, b.Registry())
	code, body := get(t, "http://"+srv.Addr()+"/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics = %d", code)
	}
	for _, want := range []string{
		`evm_balance_up{blockchain="codex-chain",chain_id="4242"} 1`,
		`evm_balance_configured_native_accounts{blockchain="codex-chain",chain_id="4242"} 2`,
		"evm_balance_records_emitted_total",
		"evm_balance_sample_records_emitted_total",
		"evm_balance_change_records_emitted_total",
		`blockchain_account_balance_wei{`,
		`blockchain_account_token_balance_raw{`,
		`blockchain_contract_token_total_supply{`,
		`blockchain_contract_transfer_count{`,
		"blockchain_rpc_call_duration_seconds",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
	// Gauges must not carry a _total suffix.
	if strings.Contains(body, "blockchain_contract_transfer_count_total") {
		t.Error("transfer count gauge should not have _total suffix")
	}
}

func TestMetricsEndpointServesKafkaSet(t *testing.T) {
	k := NewKafka("codex-chain", "")
	k.SetUp(true)
	k.SetWorkers(1)
	k.IncConsumed()
	k.IncPublished("evm.events")
	k.IncFailed("transient")
	k.IncRetry()
	k.ObservePublish(time.Millisecond)
	k.SetBackoffSeconds(time.Second)
	k.SetBlocked(true)
	k.SetConsecutiveFailures(2)

	h := NewHealth(0, 0)
	srv := newTestServer(t, true, h, k.Registry())
	code, body := get(t, "http://"+srv.Addr()+"/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics = %d", code)
	}
	for _, want := range []string{
		`evm_sink_kafka_up{blockchain="codex-chain",chain_id="unknown"} 1`,
		"evm_sink_kafka_records_consumed_total",
		`evm_sink_kafka_records_published_total{`,
		`topic="evm.events"`,
		`evm_sink_kafka_records_failed_total{`,
		`error_type="transient"`,
		"evm_sink_kafka_publish_retries_total",
		"evm_sink_kafka_publish_duration_seconds",
		"evm_sink_kafka_backoff_duration_seconds",
		"evm_sink_kafka_publish_blocked",
		"evm_sink_kafka_consecutive_failures",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
	// Gauges must not carry a _total suffix.
	if strings.Contains(body, "evm_sink_kafka_publish_blocked_total") {
		t.Error("blocked gauge should not have _total suffix")
	}
}

func TestMetricsEndpointServesWebhookSet(t *testing.T) {
	w := NewWebhook("codex-chain", "")
	w.SetUp(true)
	w.SetWorkers(1)
	w.IncConsumed()
	w.IncFiltered()
	w.IncForwarded("event")
	w.IncFailed("transient")
	w.IncRetry()
	w.ObservePost(time.Millisecond)
	w.SetBackoffSeconds(time.Second)
	w.SetBlocked(true)
	w.SetConsecutiveFailures(2)

	h := NewHealth(0, 0)
	srv := newTestServer(t, true, h, w.Registry())
	code, body := get(t, "http://"+srv.Addr()+"/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics = %d", code)
	}
	for _, want := range []string{
		`evm_sink_webhook_up{blockchain="codex-chain",chain_id="unknown"} 1`,
		"evm_sink_webhook_records_consumed_total",
		"evm_sink_webhook_records_filtered_total",
		`evm_sink_webhook_records_forwarded_total{`,
		`record_type="event"`,
		`evm_sink_webhook_records_failed_total{`,
		`error_type="transient"`,
		"evm_sink_webhook_post_retries_total",
		"evm_sink_webhook_post_duration_seconds",
		"evm_sink_webhook_backoff_duration_seconds",
		"evm_sink_webhook_post_blocked",
		"evm_sink_webhook_consecutive_failures",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
	// Gauges must not carry a _total suffix.
	if strings.Contains(body, "evm_sink_webhook_post_blocked_total") {
		t.Error("blocked gauge should not have _total suffix")
	}
}

// TestWebhookHealthReadiness verifies the sink health adapter maps endpoint
// reachability and post-blocked onto /readyz via the shared Health.
func TestWebhookHealthReadiness(t *testing.T) {
	base := NewHealth(30*time.Second, 0) // lag disabled for a sink.
	wh := NewWebhookHealth(base)
	srv := newTestServer(t, false, base, nil)
	url := "http://" + srv.Addr()

	// Ready by default (endpoint reachable, not blocked).
	if code, _ := get(t, url+"/readyz"); code != http.StatusOK {
		t.Errorf("/readyz should be 200 by default, got %d", code)
	}
	// Endpoint unreachable flips not-ready.
	wh.SetEndpointReachable(false)
	if code, _ := get(t, url+"/readyz"); code != http.StatusServiceUnavailable {
		t.Errorf("/readyz should be 503 when endpoint unreachable, got %d", code)
	}
	wh.SetEndpointReachable(true)
	// Post blocked beyond threshold flips not-ready.
	wh.SetPostBlocked(time.Minute)
	if code, body := get(t, url+"/readyz"); code != http.StatusServiceUnavailable || !strings.Contains(body, "blocked") {
		t.Errorf("/readyz should report blocked, got %d %s", code, body)
	}
}

// TestKafkaHealthReadiness verifies the sink health adapter maps broker
// reachability and publish-blocked onto /readyz via the shared Health.
func TestKafkaHealthReadiness(t *testing.T) {
	base := NewHealth(30*time.Second, 0) // lag disabled for a sink.
	kh := NewKafkaHealth(base)
	srv := newTestServer(t, false, base, nil)
	url := "http://" + srv.Addr()

	// Ready by default (broker reachable, not blocked).
	if code, _ := get(t, url+"/readyz"); code != http.StatusOK {
		t.Errorf("/readyz should be 200 by default, got %d", code)
	}
	// Broker unreachable flips not-ready.
	kh.SetBrokerReachable(false)
	if code, _ := get(t, url+"/readyz"); code != http.StatusServiceUnavailable {
		t.Errorf("/readyz should be 503 when broker unreachable, got %d", code)
	}
	kh.SetBrokerReachable(true)
	// Publish blocked beyond threshold flips not-ready.
	kh.SetPublishBlocked(time.Minute)
	if code, body := get(t, url+"/readyz"); code != http.StatusServiceUnavailable || !strings.Contains(body, "blocked") {
		t.Errorf("/readyz should report blocked, got %d %s", code, body)
	}
}
