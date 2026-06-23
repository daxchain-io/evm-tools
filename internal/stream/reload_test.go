package stream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// reloadMetrics records the reload-related metric calls for assertions.
type reloadMetrics struct {
	noopMetrics
	mu         sync.Mutex
	resets     []string
	reloads    int
	reloadErrs int
}

func (m *reloadMetrics) ResetContractSeries(name, addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resets = append(m.resets, name+"|"+addr)
}
func (m *reloadMetrics) IncConfigReload() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reloads++
}
func (m *reloadMetrics) IncConfigReloadError() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reloadErrs++
}
func (m *reloadMetrics) snap() (resets []string, reloads, errs int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.resets...), m.reloads, m.reloadErrs
}

func resolveNamed(t *testing.T, specs ...config.StreamContract) []ResolvedContract {
	t.Helper()
	rcs, err := ResolveContracts(specs)
	if err != nil {
		t.Fatalf("ResolveContracts: %v", err)
	}
	return rcs
}

// TestStreamReloadWatchSet verifies QueueReload + applyPendingReload swaps the
// derived watch set, resets the metric series of removed contracts, and counts
// the reload.
func TestStreamReloadWatchSet(t *testing.T) {
	a := config.StreamContract{Name: "a", Address: "0xaaa", Events: []string{"Transfer"}}
	b := config.StreamContract{Name: "b", Address: "0xbbb", Events: []string{"Transfer"}}
	c := config.StreamContract{Name: "c", Address: "0xccc", Events: []string{"Transfer"}}

	rm := &reloadMetrics{}
	s, err := New(Options{
		Client: &fakeClient{chainID: 1}, Emitter: &captureEmitter{}, Metrics: rm,
		ChainName: "test", ChainID: 1, PollInterval: time.Second, LogChunkBlocks: 2000,
		Contracts: resolveNamed(t, a, b),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Reload to {b, c}: a removed, c added, b retained.
	s.QueueReload(resolveNamed(t, b, c), NativeFilter{})
	s.applyPendingReload()

	if _, ok := s.byAddrTopic["0xaaa"]; ok {
		t.Error("removed contract a should be gone from the watch set")
	}
	if _, ok := s.byAddrTopic["0xccc"]; !ok {
		t.Error("added contract c should be present in the watch set")
	}
	if _, ok := s.byAddrTopic["0xbbb"]; !ok {
		t.Error("retained contract b should still be present")
	}
	if len(s.addresses) != 2 {
		t.Errorf("addresses = %v, want 2 entries", s.addresses)
	}

	resets, reloads, errs := rm.snap()
	if reloads != 1 || errs != 0 {
		t.Errorf("reloads=%d errs=%d, want 1/0", reloads, errs)
	}
	if len(resets) != 1 || resets[0] != "a|0xaaa" {
		t.Errorf("removed-series resets = %v, want [a|0xaaa]", resets)
	}
}

// TestStreamReloadNoPendingIsNoop verifies applyPendingReload with nothing staged
// neither resets series nor counts a reload.
func TestStreamReloadNoPendingIsNoop(t *testing.T) {
	rm := &reloadMetrics{}
	s, err := New(Options{
		Client: &fakeClient{chainID: 1}, Emitter: &captureEmitter{}, Metrics: rm,
		ChainName: "test", ChainID: 1, PollInterval: time.Second, LogChunkBlocks: 2000,
		Contracts: resolveNamed(t, config.StreamContract{Name: "a", Address: "0xaaa", Events: []string{"Transfer"}}),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.applyPendingReload()
	if resets, reloads, _ := rm.snap(); len(resets) != 0 || reloads != 0 {
		t.Errorf("no-op reload changed state: resets=%v reloads=%d", resets, reloads)
	}
}

// TestStreamReloadAddedContractMatches verifies a contract added by a reload is
// matched by the log filter afterward (its logs decode and emit).
func TestStreamReloadAddedContractMatches(t *testing.T) {
	fc := &fakeClient{
		chainID: 1,
		heads:   []uint64{1},
		getLogs: func(rpc.LogFilter) ([]rpc.Log, error) {
			// makeTransferLog uses address 0xtoken; only emit once the watch set
			// includes it (added by the reload below).
			return []rpc.Log{makeTransferLog(1, "1")}, nil
		},
	}
	em := &captureEmitter{}
	s, err := New(Options{
		Client: fc, Emitter: em, Metrics: &reloadMetrics{},
		ChainName: "test", ChainID: 1, PollInterval: time.Second, LogChunkBlocks: 2000,
		FromBlock: "1",
		Contracts: resolveNamed(t, config.StreamContract{Name: "other", Address: "0xother", Events: []string{"Transfer"}}),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Add the usdc contract (address 0xtoken, matching makeTransferLog).
	s.QueueReload(resolvedUSDC(t), NativeFilter{})
	s.applyPendingReload()
	if _, err := s.pollOnce(context.Background(), 1); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	if em.count() == 0 {
		t.Error("expected the reloaded contract's log to be matched and emitted")
	}
}
