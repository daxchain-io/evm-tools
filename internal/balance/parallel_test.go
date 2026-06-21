package balance

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/record"
)

// TestSampleAllTargetTimeout verifies the per-target read timeout bounds a tick:
// one target blocks until its context deadline fires while the others return
// quickly, so sampleAll surfaces a (transient) error promptly rather than hanging
// forever, and aborts the tick without emitting a partial sample set.
func TestSampleAllTargetTimeout(t *testing.T) {
	fc := newFakeClient()
	fc.balanceAtHook = func(ctx context.Context, address string) (*big.Int, error) {
		if address == lower("0xslow") {
			<-ctx.Done() // block until the per-target timeout cancels this read
			return nil, ctx.Err()
		}
		return big.NewInt(1), nil
	}
	em := &captureEmitter{}
	p, err := New(Options{
		Client:         fc,
		Emitter:        em,
		Cadence:        Cadence{Interval: time.Second},
		Native:         []NativeTarget{{Name: "fast", Address: "0xfast"}, {Name: "slow", Address: "0xslow"}},
		TargetTimeout:  30 * time.Millisecond,
		MaxConcurrency: 4,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	start := time.Now()
	err = p.sampleAll(context.Background(), 100)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected a timeout error from the hung target")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("cycle should be bounded by the per-target timeout, took %v", elapsed)
	}
	if em.count() != 0 {
		t.Errorf("a failed tick must emit nothing, got %d records", em.count())
	}
}

// TestChangeNotLostOnRecoverableEmitFailure verifies the deferred-commit
// invariant: when a *_change record's emit fails recoverably and the tick is
// retried, the change is re-detected and re-emitted rather than silently dropped
// (the prior value must not advance past an undelivered change).
func TestChangeNotLostOnRecoverableEmitFailure(t *testing.T) {
	fc := newFakeClient()
	fc.setBalance("0xa", big.NewInt(100))
	em := &captureEmitter{}
	p, err := New(Options{
		Client:  fc,
		Emitter: em,
		Cadence: Cadence{Interval: time.Second},
		Native:  []NativeTarget{{Name: "a", Address: "0xa"}},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	// Tick 1: first observation seeds prior=100 (a sample, no change).
	if err := p.sampleAll(ctx, 100); err != nil {
		t.Fatalf("tick 1: %v", err)
	}

	// Value moves to 200. Arm a one-shot recoverable failure on the *change* emit
	// of the next tick (sample = call 1 succeeds, change = call 2 fails).
	fc.setBalance("0xa", big.NewInt(200))
	calls := 0
	em.onEmit = func() error {
		calls++
		if calls == 2 {
			return errors.New("write failed (recoverable, not EPIPE)")
		}
		return nil
	}

	// Tick 2: the change emit fails; sampleAll returns the (wrapped) emit error and
	// must NOT have advanced the prior.
	if err := p.sampleAll(ctx, 101); err == nil {
		t.Fatalf("tick 2: expected an emit error from the failed change")
	}

	// Tick 3 (the retry): the change must be re-detected (prior still 100).
	if err := p.sampleAll(ctx, 102); err != nil {
		t.Fatalf("tick 3: %v", err)
	}
	changes := em.byType("balance_change")
	if len(changes) != 1 {
		t.Fatalf("expected exactly 1 change record after retry, got %d", len(changes))
	}
	bd := changes[0].Data.(record.BalanceData)
	if bd.PreviousWei != "100" || bd.BalanceWei != "200" {
		t.Errorf("change should carry 100 -> 200, got previous=%q current=%q", bd.PreviousWei, bd.BalanceWei)
	}
}

// TestSelectErrorPrioritizesPermanent verifies the read-phase error selection: a
// permanent misconfiguration outranks any transient failure, and an all-nil set
// yields nil.
func TestSelectErrorPrioritizesPermanent(t *testing.T) {
	transient := errors.New("transient rpc blip")
	perm := &permanentErr{err: errors.New("misconfigured target")}

	got := selectError([]error{nil, transient, perm})
	var pe *permanentErr
	if !errors.As(got, &pe) {
		t.Fatalf("selectError should return the permanent error, got %v", got)
	}
	if got := selectError([]error{nil, transient, nil}); got != transient {
		t.Errorf("selectError should return the first transient error, got %v", got)
	}
	if got := selectError([]error{nil, nil}); got != nil {
		t.Errorf("selectError should return nil when all succeed, got %v", got)
	}
}

// TestSampleAllParallelEmitsAllTargets confirms the concurrent read + sequential
// apply path still emits one sample per target (deterministically) when every
// read succeeds.
func TestSampleAllParallelEmitsAllTargets(t *testing.T) {
	fc := newFakeClient()
	fc.setBalance("0xa", big.NewInt(1))
	fc.setBalance("0xb", big.NewInt(2))
	fc.setBalance("0xc", big.NewInt(3))
	em := &captureEmitter{}
	p, err := New(Options{
		Client:  fc,
		Emitter: em,
		Cadence: Cadence{Interval: time.Second},
		Native: []NativeTarget{
			{Name: "a", Address: "0xa"},
			{Name: "b", Address: "0xb"},
			{Name: "c", Address: "0xc"},
		},
		MaxConcurrency: 8,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := p.sampleAll(context.Background(), 100); err != nil {
		t.Fatalf("sampleAll: %v", err)
	}
	if got := len(em.byType("balance_sample")); got != 3 {
		t.Errorf("want 3 balance_sample records, got %d", got)
	}
}
