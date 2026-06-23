package balance

import (
	"context"
	"math/big"
	"sync"
	"testing"
	"time"
)

// reloadMetrics records the reload-related metric calls for assertions.
type reloadMetrics struct {
	noopMetrics
	mu            sync.Mutex
	accountResets []string
	tokenResets   []string
	reloads       int
	reloadErrs    int
}

func (m *reloadMetrics) ResetAccountSeries(name, addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.accountResets = append(m.accountResets, name+"|"+addr)
}
func (m *reloadMetrics) ResetAccountTokenSeries(name, addr, tokenName, tokenAddr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tokenResets = append(m.tokenResets, name+"|"+addr+"|"+tokenName+"|"+tokenAddr)
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

// TestBalanceReloadTargets verifies QueueReload + applyPendingReload swaps the
// target lists, resets the gauge series and change-detection state of removed
// targets, and counts the reload.
func TestBalanceReloadTargets(t *testing.T) {
	rm := &reloadMetrics{}
	p, err := New(Options{
		Client: &fakeClient{}, Emitter: &captureEmitter{}, Metrics: rm,
		ChainName: "test", ChainID: 1,
		Cadence: Cadence{Interval: time.Second},
		Native: []NativeTarget{
			{Name: "x", Address: "0x1"},
			{Name: "y", Address: "0x2"},
		},
		ERC20: []ERC20Target{{Name: "tok", Token: "0xt", Address: "0xh"}},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Seed change-detection state for the to-be-removed targets.
	p.prior["native:x"] = big.NewInt(5)
	p.prior["erc20:tok"] = big.NewInt(9)

	// Reload: drop native x and the erc20 target, keep native y, add native z.
	p.QueueReload(
		[]NativeTarget{{Name: "y", Address: "0x2"}, {Name: "z", Address: "0x3"}},
		nil, nil, nil, nil,
	)
	p.applyPendingReload(context.Background())

	if got := len(p.opts.Native); got != 2 {
		t.Errorf("native targets = %d, want 2 (y,z)", got)
	}
	if len(p.opts.ERC20) != 0 {
		t.Errorf("erc20 targets = %d, want 0 (removed)", len(p.opts.ERC20))
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.reloads != 1 || rm.reloadErrs != 0 {
		t.Errorf("reloads=%d errs=%d, want 1/0", rm.reloads, rm.reloadErrs)
	}
	if len(rm.accountResets) != 1 || rm.accountResets[0] != "x|0x1" {
		t.Errorf("account resets = %v, want [x|0x1]", rm.accountResets)
	}
	if len(rm.tokenResets) != 1 || rm.tokenResets[0] != "tok|0xh|tok|0xt" {
		t.Errorf("token resets = %v, want [tok|0xh|tok|0xt]", rm.tokenResets)
	}
	if _, ok := p.prior["native:x"]; ok {
		t.Error("removed target's change-detection state should be cleared")
	}
	if _, ok := p.prior["erc20:tok"]; ok {
		t.Error("removed erc20 target's change-detection state should be cleared")
	}
}

// TestBalanceReloadNoPendingIsNoop verifies applyPendingReload with nothing
// staged neither resets series nor counts a reload.
func TestBalanceReloadNoPendingIsNoop(t *testing.T) {
	rm := &reloadMetrics{}
	p, err := New(Options{
		Client: &fakeClient{}, Emitter: &captureEmitter{}, Metrics: rm,
		ChainName: "test", ChainID: 1,
		Cadence: Cadence{Interval: time.Second},
		Native:  []NativeTarget{{Name: "x", Address: "0x1"}},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.applyPendingReload(context.Background())
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.reloads != 0 || len(rm.accountResets) != 0 {
		t.Errorf("no-op reload changed state: reloads=%d resets=%v", rm.reloads, rm.accountResets)
	}
}
