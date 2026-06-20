package webhooksink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/record"
)

// streamFrom encodes the given envelopes through the real record.Writer (the
// source of truth) so the sink reads exactly what a producer would emit.
func streamFrom(t *testing.T, envs ...record.Envelope) string {
	t.Helper()
	var buf bytes.Buffer
	w := record.NewWriter(&buf)
	for _, e := range envs {
		if err := w.Emit(e); err != nil {
			t.Fatalf("emit: %v", err)
		}
	}
	return buf.String()
}

func eventEnv(tx string, logIndex uint64) record.Envelope {
	li := logIndex
	return record.Envelope{
		Type: record.TypeEvent, Tool: record.ToolStream, Name: "usdc",
		Chain: "codex-chain", ChainID: 4242, BlockNumber: 100,
		TxHash: tx, LogIndex: &li,
		Data: record.EventData{Event: "Transfer", Signature: "Transfer(address,address,uint256)", Contract: "0xc", Params: map[string]string{"value": "1"}},
	}
}

func nativeSampleEnv(name string, block uint64, balance string) record.Envelope {
	return record.Envelope{
		Type: record.TypeBalanceSample, Tool: record.ToolBalance, Name: name,
		Chain: "codex-chain", ChainID: 4242, BlockNumber: block,
		Data: record.BalanceData{Kind: record.KindNative, Address: "0xa", BalanceWei: "1", Balance: balance},
	}
}

func newFilterOrFatal(t *testing.T, opts FilterOptions) *Filter {
	t.Helper()
	f, err := NewFilter(opts)
	if err != nil {
		t.Fatalf("NewFilter: %v", err)
	}
	return f
}

// captureServer is an httptest server that records every received body and can be
// programmed to fail a given number of requests with a status before succeeding.
type captureServer struct {
	srv  *httptest.Server
	mu   sync.Mutex
	got  [][]byte
	ct   []string
	hdr  []http.Header
	reqs int32

	// failFirst N requests return failStatus, then requests return 200.
	failFirst  int32
	failStatus int
	alwaysStat int // when non-zero, every request returns this status
}

func newCaptureServer(t *testing.T) *captureServer {
	t.Helper()
	cs := &captureServer{}
	cs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		atomic.AddInt32(&cs.reqs, 1)

		if cs.alwaysStat != 0 {
			w.WriteHeader(cs.alwaysStat)
			return
		}
		if atomic.AddInt32(&cs.failFirst, -1) >= 0 {
			w.WriteHeader(cs.failStatus)
			return
		}
		cs.mu.Lock()
		cs.got = append(cs.got, body)
		cs.ct = append(cs.ct, r.Header.Get("Content-Type"))
		cs.hdr = append(cs.hdr, r.Header.Clone())
		cs.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(cs.srv.Close)
	return cs
}

func (cs *captureServer) count() int {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return len(cs.got)
}

func (cs *captureServer) bodies() [][]byte {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return append([][]byte(nil), cs.got...)
}

func newHTTPSink(t *testing.T, in string, cfg PosterConfig, filter *Filter, m Metrics) *Sink {
	t.Helper()
	poster, err := NewHTTPPoster(cfg)
	if err != nil {
		t.Fatalf("NewHTTPPoster: %v", err)
	}
	sink, err := New(Options{
		Reader:      record.NewReader(strings.NewReader(in)),
		Poster:      poster,
		Filter:      filter,
		Metrics:     m,
		BackoffBase: time.Millisecond,
		BackoffMax:  2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return sink
}

// TestSinkForwardsEveryRecord verifies the happy path: every JSONL line on stdin
// is POSTed once, byte-for-byte, as application/json.
func TestSinkForwardsEveryRecord(t *testing.T) {
	cs := newCaptureServer(t)
	in := streamFrom(t, eventEnv("0x1", 0), eventEnv("0x2", 1), nativeSampleEnv("treasury", 100, "0"))
	sink := newHTTPSink(t, in, PosterConfig{URL: cs.srv.URL}, nil, nil)
	if err := sink.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if cs.count() != 3 {
		t.Fatalf("server received %d requests, want 3", cs.count())
	}
	wantLines := strings.Split(strings.TrimRight(in, "\n"), "\n")
	for i, body := range cs.bodies() {
		if string(body) != wantLines[i] {
			t.Errorf("request %d body mismatch\n got: %s\nwant: %s", i, body, wantLines[i])
		}
		if cs.ct[i] != "application/json" {
			t.Errorf("request %d content-type = %q, want application/json", i, cs.ct[i])
		}
	}
}

// TestSinkRetriesTransient5xx verifies an HTTP 5xx is retried (blocking) and the
// record is eventually forwarded — never dropped.
func TestSinkRetriesTransient5xx(t *testing.T) {
	cs := newCaptureServer(t)
	cs.failFirst = 3
	cs.failStatus = http.StatusServiceUnavailable

	mm := &countingMetrics{}
	in := streamFrom(t, eventEnv("0x1", 0))
	sink := newHTTPSink(t, in, PosterConfig{URL: cs.srv.URL}, nil, mm)
	if err := sink.Run(context.Background()); err != nil {
		t.Fatalf("Run should succeed after retries, got: %v", err)
	}
	if cs.count() != 1 {
		t.Fatalf("expected 1 forwarded record after retries, got %d", cs.count())
	}
	if atomic.LoadInt32(&cs.reqs) != 4 { // 3 failures + 1 success
		t.Errorf("expected 4 requests, got %d", atomic.LoadInt32(&cs.reqs))
	}
	if mm.retries == 0 {
		t.Errorf("expected retry metric to increment")
	}
	if mm.failed["transient"] != 3 {
		t.Errorf("expected 3 transient failures recorded, got %d", mm.failed["transient"])
	}
}

// TestSinkFailsFastOn4xx verifies a permanent HTTP 4xx stops the sink with a
// non-nil error (exit non-zero) rather than retrying — a silent drop would
// violate losslessness; an infinite retry would wedge the pipeline.
func TestSinkFailsFastOn4xx(t *testing.T) {
	cs := newCaptureServer(t)
	cs.alwaysStat = http.StatusBadRequest

	in := streamFrom(t, eventEnv("0x1", 0))
	sink := newHTTPSink(t, in, PosterConfig{URL: cs.srv.URL}, nil, nil)
	err := sink.Run(context.Background())
	if err == nil {
		t.Fatal("expected a permanent-failure error on HTTP 4xx")
	}
	if !strings.Contains(err.Error(), "permanent POST failure") {
		t.Errorf("error should name the permanent failure, got: %v", err)
	}
	if got := atomic.LoadInt32(&cs.reqs); got != 1 {
		t.Errorf("4xx should not retry, got %d requests", got)
	}
}

// TestSinkFailsFastOnMalformedLine verifies a malformed JSONL line stops the sink
// (the stream is the contract; a sink must not skip a record it cannot parse).
func TestSinkFailsFastOnMalformedLine(t *testing.T) {
	cs := newCaptureServer(t)
	in := streamFrom(t, eventEnv("0x1", 0)) + "{not json\n"
	sink := newHTTPSink(t, in, PosterConfig{URL: cs.srv.URL}, nil, nil)
	err := sink.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "read record") {
		t.Fatalf("expected a read-record error, got: %v", err)
	}
	if cs.count() != 1 {
		t.Errorf("expected the valid record to forward before the malformed line, got %d", cs.count())
	}
}

// TestSinkSendsAuthHeader verifies the optional auth header is attached to every
// request.
func TestSinkSendsAuthHeader(t *testing.T) {
	cs := newCaptureServer(t)
	in := streamFrom(t, eventEnv("0x1", 0))
	cfg := PosterConfig{
		URL:        cs.srv.URL,
		AuthHeader: "Authorization",
		AuthValue:  "Bearer s3cr3t",
		Headers:    map[string]string{"X-Source": "evm-tools"},
	}
	sink := newHTTPSink(t, in, cfg, nil, nil)
	if err := sink.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if len(cs.hdr) != 1 {
		t.Fatalf("expected 1 captured request, got %d", len(cs.hdr))
	}
	if got := cs.hdr[0].Get("Authorization"); got != "Bearer s3cr3t" {
		t.Errorf("Authorization = %q, want Bearer s3cr3t", got)
	}
	if got := cs.hdr[0].Get("X-Source"); got != "evm-tools" {
		t.Errorf("X-Source = %q, want evm-tools", got)
	}
}

// TestSinkConfirmBeforeAdvance verifies the cursor advances only after a POST is
// confirmed: with a poster that blocks until released, the second record is not
// sent until the first completes.
func TestSinkConfirmBeforeAdvance(t *testing.T) {
	in := streamFrom(t, eventEnv("0x1", 0), eventEnv("0x2", 1))
	release := make(chan struct{})
	bp := &blockingPoster{release: release}
	sink, _ := New(Options{
		Reader: record.NewReader(strings.NewReader(in)),
		Poster: bp,
	})

	done := make(chan error, 1)
	go func() { done <- sink.Run(context.Background()) }()

	waitFor(t, func() bool { return bp.inflight() == 1 })
	if bp.completed() != 0 {
		t.Fatalf("no POST should be completed while the first is blocked")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if bp.completed() != 2 {
		t.Errorf("expected 2 completed POSTs, got %d", bp.completed())
	}
}

// blockingPoster blocks every Post until release is closed.
type blockingPoster struct {
	release chan struct{}
	mu      sync.Mutex
	started int
	done    int
}

func (b *blockingPoster) Post(ctx context.Context, _ []byte) error {
	b.mu.Lock()
	b.started++
	b.mu.Unlock()
	select {
	case <-b.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	b.mu.Lock()
	b.done++
	b.mu.Unlock()
	return nil
}
func (b *blockingPoster) Close() error { return nil }
func (b *blockingPoster) inflight() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.started - b.done
}
func (b *blockingPoster) completed() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.done
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

// countingMetrics records the metric calls relevant to the retry/filter tests.
type countingMetrics struct {
	consumed  int
	filtered  int
	forwarded int
	failed    map[string]int
	retries   int
}

func (m *countingMetrics) IncConsumed()        { m.consumed++ }
func (m *countingMetrics) IncFiltered()        { m.filtered++ }
func (m *countingMetrics) IncForwarded(string) { m.forwarded++ }
func (m *countingMetrics) IncFailed(et string) {
	if m.failed == nil {
		m.failed = map[string]int{}
	}
	m.failed[et]++
}
func (m *countingMetrics) ObservePost(time.Duration)       {}
func (m *countingMetrics) IncRetry()                       { m.retries++ }
func (m *countingMetrics) SetBackoffSeconds(time.Duration) {}
func (m *countingMetrics) SetBlocked(bool)                 {}
func (m *countingMetrics) SetConsecutiveFailures(int)      {}

// TestClassify confirms the transient/permanent split.
func TestClassify(t *testing.T) {
	if Classify(errors.New("dial tcp: i/o timeout")) != ClassTransient {
		t.Errorf("network error should be transient")
	}
	if Classify(&transientHTTPError{status: 503}) != ClassTransient {
		t.Errorf("5xx should be transient")
	}
	if Classify(&PermanentError{Reason: "HTTP 400"}) != ClassPermanent {
		t.Errorf("PermanentError should be permanent")
	}
	if Classify(fmt.Errorf("wrapped: %w", &PermanentError{Reason: "x"})) != ClassPermanent {
		t.Errorf("wrapped PermanentError should be permanent")
	}
}

// TestNewHTTPPosterRejectsBadURL verifies construction fails fast on a missing or
// non-http(s) URL and an unsupported method.
func TestNewHTTPPosterRejectsBadURL(t *testing.T) {
	if _, err := NewHTTPPoster(PosterConfig{URL: ""}); err == nil {
		t.Error("expected an error for an empty URL")
	}
	if _, err := NewHTTPPoster(PosterConfig{URL: "ftp://example.com"}); err == nil {
		t.Error("expected an error for a non-http(s) URL")
	}
	if _, err := NewHTTPPoster(PosterConfig{URL: "https://x", Method: "DELETE"}); err == nil {
		t.Error("expected an error for an unsupported method")
	}
}

// TestRedactURL verifies query strings and userinfo are stripped for logging.
func TestRedactURL(t *testing.T) {
	cases := map[string]string{
		"https://h.example.com/evm?token=abc":     "https://h.example.com/evm",
		"https://user:pass@h.example.com/evm":     "https://h.example.com/evm",
		"https://user:pass@h.example.com/e?t=sec": "https://h.example.com/e",
		"http://plain.example.com/path":           "http://plain.example.com/path",
	}
	for in, want := range cases {
		if got := RedactURL(in); got != want {
			t.Errorf("RedactURL(%q) = %q, want %q", in, got, want)
		}
	}
}
