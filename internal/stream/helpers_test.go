package stream

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/metrics"
	"github.com/daxchain-io/evm-tools/internal/record"
)

// blockingEmitter blocks inside Emit until released, simulating a wedged
// downstream sink whose stdout write never returns.
type blockingEmitter struct {
	release chan struct{}
	entered chan struct{}
	once    atomic.Bool
}

func (e *blockingEmitter) Emit(record.Envelope) error {
	if e.once.CompareAndSwap(false, true) {
		close(e.entered)
	}
	<-e.release
	return nil
}

// TestEmitWedgeTripsReadyz holds the underlying write blocked past the
// emit-blocked threshold and asserts the in-progress-wedge path updates the
// gauge and flips /readyz to 503 *while still wedged* — the case the
// emit_blocked gauge exists to catch (design.md, "Output discipline and
// backpressure"). Without the concurrent watchdog the gauge would only update
// after the (never-returning) write completes, so /readyz would stay ready.
func TestEmitWedgeTripsReadyz(t *testing.T) {
	const threshold = 60 * time.Millisecond

	health := metrics.NewHealth(threshold, 0)
	health.SetRPCReachable(true) // isolate the emit-blocked dimension

	srv, err := metrics.NewServer(metrics.ServerOptions{
		Addr:           "127.0.0.1:0",
		MetricsEnabled: false,
		Health:         health,
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
	readyzURL := "http://" + srv.Addr() + "/readyz"

	// Ready before the write begins.
	if code, _ := getStatus(t, readyzURL); code != http.StatusOK {
		t.Fatalf("/readyz should be 200 before any blocked write, got %d", code)
	}

	be := &blockingEmitter{release: make(chan struct{}), entered: make(chan struct{})}
	bte := newBlockTrackingEmitter(be, metrics.NewStream("c", "1"), health, time.Now)
	bte.interval = 5 * time.Millisecond // tick fast so the wedge is observed quickly

	emitDone := make(chan struct{})
	go func() {
		defer close(emitDone)
		_ = bte.Emit(record.Envelope{Type: record.TypeEvent, Tool: record.ToolStream})
	}()
	<-be.entered // the write is now in flight and blocked

	// While the write is still blocked, the watchdog must push the gauge past the
	// threshold and /readyz must flip to 503 reporting a blocked stdout write.
	deadline := time.Now().Add(2 * time.Second)
	var lastCode int
	var lastBody string
	for time.Now().Before(deadline) {
		lastCode, lastBody = getStatus(t, readyzURL)
		if lastCode == http.StatusServiceUnavailable && strings.Contains(lastBody, "blocked") {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if lastCode != http.StatusServiceUnavailable || !strings.Contains(lastBody, "blocked") {
		t.Fatalf("/readyz did not flip to not-ready while write wedged: code=%d body=%q", lastCode, lastBody)
	}

	// Release the wedged write. The gauge reports the current-or-LAST write's
	// blocked span, so it correctly holds the long value (still not-ready) until
	// a subsequent quick write republishes a small span. A fast emit through a
	// non-blocking emitter then clears it and readiness recovers.
	close(be.release)
	<-emitDone

	fast := newBlockTrackingEmitter(passthroughEmitter{}, metrics.NewStream("c", "1"), health, time.Now)
	if err := fast.Emit(record.Envelope{Type: record.TypeEvent, Tool: record.ToolStream}); err != nil {
		t.Fatalf("fast emit: %v", err)
	}
	if code, body := getStatus(t, readyzURL); code != http.StatusOK {
		t.Fatalf("/readyz should recover after a fast write clears the gauge, got %d %q", code, body)
	}
}

// passthroughEmitter returns immediately, modeling an unblocked stdout writer.
type passthroughEmitter struct{}

func (passthroughEmitter) Emit(record.Envelope) error { return nil }

func getStatus(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}
