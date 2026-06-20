package kafkasink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/record"
)

// fakePublisher is an in-memory Publisher used by the default (offline) tests so
// no real broker is needed. It can be programmed to fail a given number of times
// (transiently or permanently) before succeeding.
type fakePublisher struct {
	mu        sync.Mutex
	published []Message
	closed    bool

	// failFirst N publishes return failErr, then publishes succeed.
	failFirst int
	failErr   error
	// calls counts every Publish invocation (including failures).
	calls int
}

func (f *fakePublisher) Publish(_ context.Context, msg Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failFirst > 0 {
		f.failFirst--
		return f.failErr
	}
	// Copy the value: the loop reuses buffers across retries in some paths.
	v := append([]byte(nil), msg.Value...)
	f.published = append(f.published, Message{Topic: msg.Topic, Key: append([]byte(nil), msg.Key...), Value: v})
	return nil
}

func (f *fakePublisher) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakePublisher) snapshot() []Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Message(nil), f.published...)
}

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

func sampleEnv(name string, block uint64) record.Envelope {
	return record.Envelope{
		Type: record.TypeBalanceSample, Tool: record.ToolBalance, Name: name,
		Chain: "codex-chain", ChainID: 4242, BlockNumber: block,
		Data: record.BalanceData{Kind: record.KindNative, Address: "0xa", BalanceWei: "1", Balance: "0"},
	}
}

func newRouterOrFatal(t *testing.T, topic string, byType map[string]string, mode PartitionKeyMode) *Router {
	t.Helper()
	r, err := NewRouter(topic, byType, mode)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return r
}

// TestSinkPublishesEveryRecord verifies the happy path: every JSONL line on
// stdin is published once, byte-for-byte, to the default topic.
func TestSinkPublishesEveryRecord(t *testing.T) {
	in := streamFrom(t, eventEnv("0x1", 0), eventEnv("0x2", 1), sampleEnv("treasury", 100))
	pub := &fakePublisher{}
	sink, err := New(Options{
		Reader:    record.NewReader(strings.NewReader(in)),
		Publisher: pub,
		Router:    newRouterOrFatal(t, "evm.events", nil, PartitionIdentity),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := sink.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := pub.snapshot()
	if len(got) != 3 {
		t.Fatalf("published %d messages, want 3", len(got))
	}
	// Payloads must be the verbatim JSONL lines (minus newline).
	wantLines := strings.Split(strings.TrimRight(in, "\n"), "\n")
	for i, m := range got {
		if m.Topic != "evm.events" {
			t.Errorf("message %d topic = %q, want evm.events", i, m.Topic)
		}
		if string(m.Value) != wantLines[i] {
			t.Errorf("message %d value mismatch\n got: %s\nwant: %s", i, m.Value, wantLines[i])
		}
		if len(m.Key) == 0 {
			t.Errorf("message %d missing partition key", i)
		}
	}
}

// TestSinkTopicRouting verifies per-record-type topic overrides and the default
// fallback.
func TestSinkTopicRouting(t *testing.T) {
	in := streamFrom(t, eventEnv("0x1", 0), sampleEnv("treasury", 100))
	pub := &fakePublisher{}
	router := newRouterOrFatal(t, "evm.default", map[string]string{
		"event": "evm.events",
	}, PartitionIdentity)
	sink, _ := New(Options{Reader: record.NewReader(strings.NewReader(in)), Publisher: pub, Router: router})
	if err := sink.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := pub.snapshot()
	if got[0].Topic != "evm.events" {
		t.Errorf("event routed to %q, want evm.events", got[0].Topic)
	}
	if got[1].Topic != "evm.default" {
		t.Errorf("balance_sample routed to %q, want evm.default", got[1].Topic)
	}
}

// TestSinkPartitionKeyOrdering verifies the identity strategy keys two samples of
// the same entry to the same partition key (per-key ordering), while the dedup
// strategy keys them distinctly, and none drops the key entirely under "none".
func TestSinkPartitionKeyOrdering(t *testing.T) {
	envs := []record.Envelope{sampleEnv("treasury", 100), sampleEnv("treasury", 101)}

	t.Run("identity", func(t *testing.T) {
		pub := &fakePublisher{}
		sink, _ := New(Options{
			Reader:    record.NewReader(strings.NewReader(streamFrom(t, envs...))),
			Publisher: pub,
			Router:    newRouterOrFatal(t, "t", nil, PartitionIdentity),
		})
		_ = sink.Run(context.Background())
		got := pub.snapshot()
		if !bytes.Equal(got[0].Key, got[1].Key) {
			t.Errorf("identity keys should match for the same entry: %q vs %q", got[0].Key, got[1].Key)
		}
	})

	t.Run("dedup", func(t *testing.T) {
		pub := &fakePublisher{}
		sink, _ := New(Options{
			Reader:    record.NewReader(strings.NewReader(streamFrom(t, envs...))),
			Publisher: pub,
			Router:    newRouterOrFatal(t, "t", nil, PartitionDedup),
		})
		_ = sink.Run(context.Background())
		got := pub.snapshot()
		if bytes.Equal(got[0].Key, got[1].Key) {
			t.Errorf("dedup keys should differ for distinct samples")
		}
	})

	t.Run("none", func(t *testing.T) {
		pub := &fakePublisher{}
		sink, _ := New(Options{
			Reader:    record.NewReader(strings.NewReader(streamFrom(t, envs...))),
			Publisher: pub,
			Router:    newRouterOrFatal(t, "t", nil, PartitionNone),
		})
		_ = sink.Run(context.Background())
		got := pub.snapshot()
		if len(got[0].Key) != 0 {
			t.Errorf("none strategy should emit no key, got %q", got[0].Key)
		}
	})
}

// TestSinkRetriesTransient verifies a transient failure is retried (blocking) and
// the record is eventually published exactly to its topic — never dropped.
func TestSinkRetriesTransient(t *testing.T) {
	in := streamFrom(t, eventEnv("0x1", 0))
	pub := &fakePublisher{failFirst: 3, failErr: errors.New("dial tcp: connection refused")}
	mm := &countingMetrics{}
	sink, _ := New(Options{
		Reader:      record.NewReader(strings.NewReader(in)),
		Publisher:   pub,
		Router:      newRouterOrFatal(t, "evm.events", nil, PartitionIdentity),
		Metrics:     mm,
		BackoffBase: time.Millisecond, // keep the test fast.
		BackoffMax:  2 * time.Millisecond,
	})
	if err := sink.Run(context.Background()); err != nil {
		t.Fatalf("Run should succeed after retries, got: %v", err)
	}
	if got := pub.snapshot(); len(got) != 1 {
		t.Fatalf("expected 1 published message after retries, got %d", len(got))
	}
	if pub.calls != 4 { // 3 failures + 1 success
		t.Errorf("expected 4 publish attempts, got %d", pub.calls)
	}
	if mm.retries == 0 {
		t.Errorf("expected retry metric to increment")
	}
	if mm.failed["transient"] != 3 {
		t.Errorf("expected 3 transient failures recorded, got %d", mm.failed["transient"])
	}
}

// TestSinkFailsFastOnPermanent verifies a permanent broker rejection stops the
// sink with a non-nil error (exit non-zero) rather than retrying forever — a
// stuck retry would silently wedge the lossless pipeline.
func TestSinkFailsFastOnPermanent(t *testing.T) {
	in := streamFrom(t, eventEnv("0x1", 0))
	pub := &fakePublisher{failFirst: 100, failErr: &PermanentError{Reason: "message too large"}}
	sink, _ := New(Options{
		Reader:      record.NewReader(strings.NewReader(in)),
		Publisher:   pub,
		Router:      newRouterOrFatal(t, "evm.events", nil, PartitionIdentity),
		BackoffBase: time.Millisecond,
	})
	err := sink.Run(context.Background())
	if err == nil {
		t.Fatal("expected a permanent-failure error")
	}
	if !strings.Contains(err.Error(), "permanent publish failure") {
		t.Errorf("error should name the permanent failure, got: %v", err)
	}
	if pub.calls != 1 {
		t.Errorf("permanent failure should not retry, got %d attempts", pub.calls)
	}
}

// TestSinkFailsFastOnMalformedLine verifies a malformed JSONL line stops the sink
// (the stream is the contract; a sink must not skip a record it cannot parse).
func TestSinkFailsFastOnMalformedLine(t *testing.T) {
	in := streamFrom(t, eventEnv("0x1", 0)) + "{not json\n"
	pub := &fakePublisher{}
	sink, _ := New(Options{
		Reader:    record.NewReader(strings.NewReader(in)),
		Publisher: pub,
		Router:    newRouterOrFatal(t, "evm.events", nil, PartitionIdentity),
	})
	err := sink.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "read record") {
		t.Fatalf("expected a read-record error, got: %v", err)
	}
	// The first (valid) record was still published before the bad line.
	if len(pub.snapshot()) != 1 {
		t.Errorf("expected the valid record to publish before the malformed line")
	}
}

// TestSinkConfirmBeforeAdvance verifies the cursor advances only after a publish
// is confirmed: with a publisher that blocks until released, the second record is
// not consumed until the first is acked.
func TestSinkConfirmBeforeAdvance(t *testing.T) {
	in := streamFrom(t, eventEnv("0x1", 0), eventEnv("0x2", 1))
	release := make(chan struct{})
	bp := &blockingPublisher{release: release}
	sink, _ := New(Options{
		Reader:    record.NewReader(strings.NewReader(in)),
		Publisher: bp,
		Router:    newRouterOrFatal(t, "evm.events", nil, PartitionIdentity),
	})

	done := make(chan error, 1)
	go func() { done <- sink.Run(context.Background()) }()

	// Wait for the first publish to be in flight.
	waitFor(t, func() bool { return bp.inflight() == 1 })
	if bp.completed() != 0 {
		t.Fatalf("no publish should be completed while the first is blocked")
	}
	// Release both publishes.
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if bp.completed() != 2 {
		t.Errorf("expected 2 completed publishes, got %d", bp.completed())
	}
}

// blockingPublisher blocks every Publish until release is closed, recording how
// many are in flight and completed.
type blockingPublisher struct {
	release chan struct{}
	mu      sync.Mutex
	started int
	done    int
}

func (b *blockingPublisher) Publish(ctx context.Context, _ Message) error {
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
func (b *blockingPublisher) Close() error { return nil }
func (b *blockingPublisher) inflight() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.started - b.done
}
func (b *blockingPublisher) completed() int {
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

// countingMetrics records the metric calls relevant to the retry tests.
type countingMetrics struct {
	consumed  int
	published int
	failed    map[string]int
	retries   int
}

func (m *countingMetrics) IncConsumed() { m.consumed++ }
func (m *countingMetrics) IncPublished(string) {
	m.published++
}
func (m *countingMetrics) IncFailed(et string) {
	if m.failed == nil {
		m.failed = map[string]int{}
	}
	m.failed[et]++
}
func (m *countingMetrics) ObservePublish(time.Duration)    {}
func (m *countingMetrics) IncRetry()                       { m.retries++ }
func (m *countingMetrics) SetBackoffSeconds(time.Duration) {}
func (m *countingMetrics) SetBlocked(bool)                 {}
func (m *countingMetrics) SetConsecutiveFailures(int)      {}

// TestRouterRejectsUnsupportedMode verifies router construction validates the
// partition-key mode.
func TestRouterRejectsUnsupportedMode(t *testing.T) {
	if _, err := NewRouter("t", nil, PartitionKeyMode("bogus")); err == nil {
		t.Fatal("expected an error for an unsupported partition_key mode")
	}
}

// TestRouterRequiresTopic verifies a record with no topic at all is an error
// (never silently dropped).
func TestRouterRequiresTopic(t *testing.T) {
	if _, err := NewRouter("", nil, PartitionIdentity); err == nil {
		t.Fatal("expected an error when neither a default topic nor a map is set")
	}
	r := newRouterOrFatal(t, "", map[string]string{"event": "evm.events"}, PartitionIdentity)
	_, _, err := r.Route(sampleEnv("x", 1)) // balance_sample has no mapping and no default.
	if err == nil {
		t.Fatal("expected an error routing a record with no topic")
	}
}

// TestClassify confirms the transient/permanent split.
func TestClassify(t *testing.T) {
	if Classify(errors.New("dial tcp: i/o timeout")) != ClassTransient {
		t.Errorf("network error should be transient")
	}
	if Classify(&PermanentError{Reason: "x"}) != ClassPermanent {
		t.Errorf("PermanentError should be permanent")
	}
	if Classify(fmt.Errorf("wrapped: %w", &PermanentError{Reason: "x"})) != ClassPermanent {
		t.Errorf("wrapped PermanentError should be permanent")
	}
}
