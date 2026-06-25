package stream

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/checkpoint"
	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// TestRunResumesFromCheckpointEndToEnd exercises the FULL durable-resume loop —
// not just the resolveStart/Save pieces the other tests cover. A first Stream
// runs against a real file-backed checkpoint.Store (stream.go:440-446 persists
// the cursor each poll), processes a known range, and writes the cursor file. A
// SECOND Stream with the SAME store on disk simulates a restart: it must resume
// from cursor+1 (resolveStart, stream.go:865-876) rather than jumping to
// from_block=latest's head+1, and must NOT re-emit any block at or below the
// saved cursor. The assertions are on the actual blocks the resumed stream
// emits, proving the cursor — not the head — drove the restart.
func TestRunResumesFromCheckpointEndToEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor.json")
	store := checkpoint.NewStore(path)

	// emitLogsForRange returns one event log per block in the requested window so
	// the emitted records map 1:1 to processed blocks.
	emitLogsForRange := func(f rpc.LogFilter) ([]rpc.Log, error) {
		var out []rpc.Log
		for b := f.FromBlock; b <= f.ToBlock; b++ {
			out = append(out, makeTransferLog(b, "1"))
		}
		return out, nil
	}

	// ---- First run: from_block=1, head=5 -> process [1,5], persist cursor 5. ----
	fc1 := &fakeClient{chainID: 1, heads: []uint64{5}, getLogs: emitLogsForRange}
	em1 := &captureEmitter{}
	s1, err := New(Options{
		Client: fc1, Emitter: em1, ChainName: "test", ChainID: 1,
		Contracts: resolvedUSDC(t), PollInterval: 2 * time.Millisecond,
		LogChunkBlocks: 2000, FromBlock: "1", Checkpoint: store,
	})
	if err != nil {
		t.Fatalf("New(first): %v", err)
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- s1.Run(ctx1) }()
	waitFor(t, func() bool { return em1.count() >= 5 }, time.Second)
	// The cursor file is written only after the poll's advance; wait for it.
	waitFor(t, func() bool {
		c, ok, lerr := store.Load()
		return lerr == nil && ok && c.LastBlock == 5
	}, time.Second)
	cancel1()
	if err := <-done1; err != nil {
		t.Fatalf("first Run: %v", err)
	}

	// Sanity: the first run emitted blocks 1..5 (the source of the cursor).
	if got := highestBlock(em1); got != 5 {
		t.Fatalf("first run highest emitted block = %d, want 5", got)
	}

	cur, ok, err := store.Load()
	if err != nil || !ok || cur.LastBlock != 5 || cur.ChainID != 1 {
		t.Fatalf("persisted cursor = %+v ok=%v err=%v, want {chain 1, block 5}", cur, ok, err)
	}

	// ---- Restart: SAME store, from_block=latest, head now 8. ----
	// Reading the cursor (5) must win over from_block=latest (which would resolve
	// to head+1=9 and emit NOTHING). The resumed stream must therefore emit
	// exactly blocks 6,7,8 and never re-touch 1..5.
	fc2 := &fakeClient{chainID: 1, heads: []uint64{8}, getLogs: emitLogsForRange}
	em2 := &captureEmitter{}
	s2, err := New(Options{
		Client: fc2, Emitter: em2, ChainName: "test", ChainID: 1,
		Contracts: resolvedUSDC(t), PollInterval: 2 * time.Millisecond,
		LogChunkBlocks: 2000, FromBlock: "latest", Checkpoint: store,
	})
	if err != nil {
		t.Fatalf("New(resume): %v", err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- s2.Run(ctx2) }()
	waitFor(t, func() bool { return em2.count() >= 3 }, time.Second)
	cancel2()
	if err := <-done2; err != nil {
		t.Fatalf("resume Run: %v", err)
	}

	resumed := em2.snapshot()
	// The first block the resumed stream processes must be cursor+1 (6), not head+1
	// (9 from from_block=latest) and not from_block=1 again.
	if resumed[0].BlockNumber != 6 {
		t.Errorf("resumed stream's first emitted block = %d, want 6 (cursor+1)", resumed[0].BlockNumber)
	}
	for _, env := range resumed {
		if env.BlockNumber <= 5 {
			t.Errorf("resumed stream re-emitted block %d at or below the saved cursor (5)", env.BlockNumber)
		}
	}
	if got := highestBlock(em2); got != 8 {
		t.Errorf("resumed stream highest emitted block = %d, want 8", got)
	}
	// Exactly blocks 6,7,8 — three records, no gap, no jump-to-head.
	if len(resumed) != 3 {
		t.Errorf("resumed stream emitted %d records, want 3 (blocks 6,7,8)", len(resumed))
	}
}

// TestRunCheckpointResumeGapFreeAcrossRestart pins the no-gap guarantee: across a
// restart the union of blocks emitted by the two runs covers [1,8] with no block
// skipped and no block emitted twice. This is the at-least-once / gap-free
// contract the checkpoint exists to uphold (checkpoint.go package doc).
func TestRunCheckpointResumeGapFreeAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor.json")
	store := checkpoint.NewStore(path)

	emitLogsForRange := func(f rpc.LogFilter) ([]rpc.Log, error) {
		var out []rpc.Log
		for b := f.FromBlock; b <= f.ToBlock; b++ {
			out = append(out, makeTransferLog(b, "1"))
		}
		return out, nil
	}

	run := func(head uint64, fromBlock string) []uint64 {
		fc := &fakeClient{chainID: 1, heads: []uint64{head}, getLogs: emitLogsForRange}
		em := &captureEmitter{}
		s, err := New(Options{
			Client: fc, Emitter: em, ChainName: "test", ChainID: 1,
			Contracts: resolvedUSDC(t), PollInterval: 2 * time.Millisecond,
			LogChunkBlocks: 2000, FromBlock: fromBlock, Checkpoint: store,
		})
		if err != nil {
			t.Fatalf("New(%s): %v", fromBlock, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- s.Run(ctx) }()
		// Wait until the cursor reflects this head (the poll fully advanced).
		waitFor(t, func() bool {
			c, ok, lerr := store.Load()
			return lerr == nil && ok && c.LastBlock == head
		}, time.Second)
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run(%s): %v", fromBlock, err)
		}
		var blocks []uint64
		for _, e := range em.snapshot() {
			blocks = append(blocks, e.BlockNumber)
		}
		return blocks
	}

	first := run(4, "1")       // blocks 1..4
	second := run(8, "latest") // resumes at 5 -> blocks 5..8 (NOT head+1)

	seen := map[uint64]int{}
	for _, b := range append(first, second...) {
		seen[b]++
	}
	for b := uint64(1); b <= 8; b++ {
		switch seen[b] {
		case 0:
			t.Errorf("block %d was skipped across the restart (gap)", b)
		case 1: // exactly-once across the two runs: the ideal
		default:
			t.Errorf("block %d emitted %d times across the restart (want exactly once)", b, seen[b])
		}
	}
	if len(second) == 0 || second[0] != 5 {
		t.Errorf("second run should resume at cursor+1 (5); first emitted block = %v", second)
	}
}

// highestBlock returns the maximum block number among emitted envelopes, or 0 if
// none were emitted.
func highestBlock(em *captureEmitter) uint64 {
	var hi uint64
	for _, e := range em.snapshot() {
		if e.BlockNumber > hi {
			hi = e.BlockNumber
		}
	}
	return hi
}
