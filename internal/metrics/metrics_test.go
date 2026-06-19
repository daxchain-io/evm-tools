package metrics

import (
	"context"
	"io"
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
