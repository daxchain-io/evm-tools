package stream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// finalizedMetrics captures SetFinalizedBlock so a test can assert the loop
// publishes the finalized height (and how often).
type finalizedMetrics struct {
	noopMetrics
	mu        sync.Mutex
	finalized uint64
	calls     int
}

func (m *finalizedMetrics) SetFinalizedBlock(n uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.finalized = n
	m.calls++
}

func (m *finalizedMetrics) snap() (uint64, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.finalized, m.calls
}

func newFinalizedStream(t *testing.T, fc *fakeClient, m Metrics) *Stream {
	t.Helper()
	s, err := New(Options{
		Client: fc, Emitter: &captureEmitter{}, Metrics: m,
		ChainName: "test", ChainID: 1, PollInterval: time.Second, LogChunkBlocks: 2000,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// TestFinalizedBlockPublished verifies a poll fetches the finalized block tag and
// publishes its height to the gauge.
func TestFinalizedBlockPublished(t *testing.T) {
	fc := &fakeClient{chainID: 1, heads: []uint64{10}, finalized: 7}
	m := &finalizedMetrics{}
	s := newFinalizedStream(t, fc, m)
	if _, err := s.pollOnce(context.Background(), 11); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	if got, calls := m.snap(); got != 7 || calls != 1 {
		t.Errorf("want finalized=7 set once, got value=%d calls=%d", got, calls)
	}
}

// TestFinalizedUnsupportedDisablesPolling verifies a chain that rejects the
// "finalized" tag (a non-transient error) disables further finalized polling for
// the run rather than wasting a call every poll.
func TestFinalizedUnsupportedDisablesPolling(t *testing.T) {
	fc := &fakeClient{
		chainID:      1,
		heads:        []uint64{10},
		finalizedErr: errors.New("the method finalized is not available"),
	}
	m := &finalizedMetrics{}
	s := newFinalizedStream(t, fc, m)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := s.pollOnce(ctx, 11); err != nil {
			t.Fatalf("pollOnce %d: %v", i, err)
		}
	}
	if _, calls := m.snap(); calls != 0 {
		t.Errorf("SetFinalizedBlock must not be called when unsupported, got %d", calls)
	}
	if n := fc.finalizedCallCount(); n != 1 {
		t.Errorf("finalized should be polled once then disabled, got %d calls", n)
	}
}
