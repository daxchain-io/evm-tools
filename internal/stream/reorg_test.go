package stream

import (
	"context"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// TestReorgDetectedAndMarkerEmitted drives pollOnce across four polls. The first
// three advance the head one block at a time (1,2,3), seeding the tracker's
// recent canonical hashes. On the fourth poll block 3 is replaced (h3a -> h3b)
// and the chain extends to block 4 on the new fork. The stream must detect the
// reorg at its processing frontier, resolve the fork to block 2, and emit one
// reorg marker over the orphaned range [3,3].
func TestReorgDetectedAndMarkerEmitted(t *testing.T) {
	tag := rpc.BlockTag
	reorged := false
	blockByNum := func(n uint64) *rpc.Block {
		switch n {
		case 1:
			return &rpc.Block{Number: tag(1), Hash: "h1", ParentHash: "h0"}
		case 2:
			return &rpc.Block{Number: tag(2), Hash: "h2", ParentHash: "h1"}
		case 3:
			if reorged {
				return &rpc.Block{Number: tag(3), Hash: "h3b", ParentHash: "h2"}
			}
			return &rpc.Block{Number: tag(3), Hash: "h3a", ParentHash: "h2"}
		case 4:
			return &rpc.Block{Number: tag(4), Hash: "h4", ParentHash: "h3b"}
		default:
			return &rpc.Block{Number: tag(n), Hash: "hx"}
		}
	}
	fc := &fakeClient{
		chainID:    1,
		heads:      []uint64{1, 2, 3, 4},
		blockByNum: blockByNum,
	}
	em := &captureEmitter{}
	s, err := New(Options{
		Client:         fc,
		Emitter:        em,
		ChainName:      "test",
		ChainID:        1,
		PollInterval:   time.Second,
		LogChunkBlocks: 2000,
		ReorgDepth:     8,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	nextBlock := uint64(1)
	for poll := 1; poll <= 4; poll++ {
		if poll == 4 {
			reorged = true
		}
		nextBlock, err = s.pollOnce(ctx, nextBlock)
		if err != nil {
			t.Fatalf("poll %d: %v", poll, err)
		}
	}

	var reorgs []record.ReorgData
	for _, env := range em.snapshot() {
		if env.Type == record.TypeReorg {
			d, ok := env.Data.(record.ReorgData)
			if !ok {
				t.Fatalf("reorg record Data is %T, want record.ReorgData", env.Data)
			}
			reorgs = append(reorgs, d)
		}
	}
	if len(reorgs) != 1 {
		t.Fatalf("want exactly 1 reorg marker, got %d", len(reorgs))
	}
	d := reorgs[0]
	if d.ForkBlock != 2 || d.FromBlock != 3 || d.ToBlock != 3 || d.Depth != 1 {
		t.Errorf("reorg range wrong: %+v (want fork=2 from=3 to=3 depth=1)", d)
	}
	if d.OldHash != "h3a" || d.NewHash != "h3b" {
		t.Errorf("reorg hashes wrong: old=%q new=%q (want h3a/h3b)", d.OldHash, d.NewHash)
	}
	if d.DepthExceeded {
		t.Errorf("depth should not be exceeded for a 1-block reorg")
	}
}

// TestNoReorgWhenChainLinear verifies the steady-state path emits no reorg marker
// when the head advances cleanly with consistent parent links.
func TestNoReorgWhenChainLinear(t *testing.T) {
	tag := rpc.BlockTag
	fc := &fakeClient{
		chainID: 1,
		heads:   []uint64{1, 2, 3},
		blockByNum: func(n uint64) *rpc.Block {
			return &rpc.Block{Number: tag(n), Hash: hashAt(n), ParentHash: hashAt(n - 1)}
		},
	}
	em := &captureEmitter{}
	s, err := New(Options{
		Client: fc, Emitter: em, ChainName: "test", ChainID: 1,
		PollInterval: time.Second, LogChunkBlocks: 2000, ReorgDepth: 8,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	nextBlock := uint64(1)
	for poll := 1; poll <= 3; poll++ {
		nextBlock, err = s.pollOnce(context.Background(), nextBlock)
		if err != nil {
			t.Fatalf("poll %d: %v", poll, err)
		}
	}
	for _, env := range em.snapshot() {
		if env.Type == record.TypeReorg {
			t.Fatalf("unexpected reorg marker on a linear chain: %+v", env.Data)
		}
	}
}

func hashAt(n uint64) string {
	return "h" + rpc.BlockTag(n)
}

// TestReorgDepthExceededPinsToFloor drives a reorg DEEPER than the tracked
// ReorgDepth so resolveFork cannot find a common ancestor within its window
// (reorg.go:153-173). It must then take the floor-pinning branch
// (reorg.go:177-185): ForkBlock pinned to the window floor (orphanTip-depth),
// the marker flagged DepthExceeded=true, and the orphaned range spanning the
// whole tracked window — NOT silently continuing as if nothing reorged.
//
// Setup (ReorgDepth=2): polls 1..3 seed canonical hashes h1/h2/h3 and leave the
// frontier at block 3. resolveFork's walk window for an orphanTip of 3 is
// (floor=3-2=1, 3], i.e. heights 2 and 1. On the reorg poll EVERY tracked block
// in that window changes hash (h1b/h2b/h3b) so resolveFork can confirm no common
// ancestor — the true fork is at block 0, below the tracked depth. The code then
// pins ForkBlock to the floor (1) and flags DepthExceeded rather than guessing.
func TestReorgDepthExceededPinsToFloor(t *testing.T) {
	tag := rpc.BlockTag
	reorged := false
	blockByNum := func(n uint64) *rpc.Block {
		switch n {
		case 1:
			if reorged {
				return &rpc.Block{Number: tag(1), Hash: "h1b", ParentHash: "h0"}
			}
			return &rpc.Block{Number: tag(1), Hash: "h1", ParentHash: "h0"}
		case 2:
			if reorged {
				return &rpc.Block{Number: tag(2), Hash: "h2b", ParentHash: "h1b"}
			}
			return &rpc.Block{Number: tag(2), Hash: "h2", ParentHash: "h1"}
		case 3:
			if reorged {
				return &rpc.Block{Number: tag(3), Hash: "h3b", ParentHash: "h2b"}
			}
			return &rpc.Block{Number: tag(3), Hash: "h3", ParentHash: "h2"}
		default:
			return &rpc.Block{Number: tag(n), Hash: "hx"}
		}
	}
	// heads queue 1,2,3 then repeats 3: the reorg replaces the tip in place (head
	// stays at 3), so the deep reorg is exercised without head churn.
	fc := &fakeClient{chainID: 1, heads: []uint64{1, 2, 3}, blockByNum: blockByNum}
	em := &captureEmitter{}
	s, err := New(Options{
		Client: fc, Emitter: em, ChainName: "test", ChainID: 1,
		PollInterval: time.Second, LogChunkBlocks: 2000, ReorgDepth: 2,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	nextBlock := uint64(1)
	for poll := 1; poll <= 3; poll++ {
		nextBlock, err = s.pollOnce(ctx, nextBlock)
		if err != nil {
			t.Fatalf("seed poll %d: %v", poll, err)
		}
	}
	// The deep reorg: blocks 2 and 3 both replaced in place.
	reorged = true
	if _, err = s.pollOnce(ctx, nextBlock); err != nil {
		t.Fatalf("reorg poll: %v", err)
	}

	var reorgs []record.ReorgData
	for _, env := range em.snapshot() {
		if env.Type == record.TypeReorg {
			reorgs = append(reorgs, env.Data.(record.ReorgData))
		}
	}
	if len(reorgs) != 1 {
		t.Fatalf("want exactly 1 reorg marker for the deep reorg, got %d", len(reorgs))
	}
	d := reorgs[0]
	if !d.DepthExceeded {
		t.Errorf("deep reorg (depth > ReorgDepth) must set DepthExceeded; got %+v", d)
	}
	// Floor-pinned to orphanTip-depth = 3-2 = 1, NOT the real ancestor at block 1
	// being "proven" — the code pins to the window floor and signals uncertainty.
	if d.ForkBlock != 1 {
		t.Errorf("ForkBlock = %d, want 1 (pinned to window floor orphanTip-depth)", d.ForkBlock)
	}
	if d.FromBlock != 2 {
		t.Errorf("FromBlock = %d, want 2 (floor+1)", d.FromBlock)
	}
	if d.ToBlock != 3 {
		t.Errorf("ToBlock = %d, want 3 (the orphaned tip)", d.ToBlock)
	}
	if d.Depth != 2 {
		t.Errorf("Depth = %d, want 2 (orphanTip-floor)", d.Depth)
	}
	if d.OldHash != "h3" {
		t.Errorf("OldHash = %q, want h3 (the orphaned tip hash)", d.OldHash)
	}
}

// TestTipReorgWithoutHeadAdvance covers the in-place tip replacement: the head
// stays at block 3 but block 3's hash changes (h3a -> h3b) before a new block is
// mined. The frontier check must run even though nextBlock (4) > head (3), so the
// reorg is detected rather than skipped by the no-op path.
func TestTipReorgWithoutHeadAdvance(t *testing.T) {
	tag := rpc.BlockTag
	reorged := false
	blockByNum := func(n uint64) *rpc.Block {
		switch n {
		case 1:
			return &rpc.Block{Number: tag(1), Hash: "h1", ParentHash: "h0"}
		case 2:
			return &rpc.Block{Number: tag(2), Hash: "h2", ParentHash: "h1"}
		case 3:
			if reorged {
				return &rpc.Block{Number: tag(3), Hash: "h3b", ParentHash: "h2"}
			}
			return &rpc.Block{Number: tag(3), Hash: "h3a", ParentHash: "h2"}
		default:
			return &rpc.Block{Number: tag(n), Hash: "hx"}
		}
	}
	// heads queue repeats its last value, so polls 1,2,3 advance to 3 and poll 4
	// still reports head 3 (no new block mined).
	fc := &fakeClient{chainID: 1, heads: []uint64{1, 2, 3}, blockByNum: blockByNum}
	em := &captureEmitter{}
	s, err := New(Options{
		Client: fc, Emitter: em, ChainName: "test", ChainID: 1,
		PollInterval: time.Second, LogChunkBlocks: 2000, ReorgDepth: 8,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	nextBlock := uint64(1)
	for poll := 1; poll <= 3; poll++ {
		nextBlock, err = s.pollOnce(ctx, nextBlock)
		if err != nil {
			t.Fatalf("seed poll %d: %v", poll, err)
		}
	}
	// In-place tip replacement with the head staying at 3.
	reorged = true
	if _, err = s.pollOnce(ctx, nextBlock); err != nil {
		t.Fatalf("reorg poll: %v", err)
	}

	var reorgs []record.ReorgData
	for _, env := range em.snapshot() {
		if env.Type == record.TypeReorg {
			reorgs = append(reorgs, env.Data.(record.ReorgData))
		}
	}
	if len(reorgs) != 1 {
		t.Fatalf("want exactly 1 reorg marker for the in-place tip reorg, got %d", len(reorgs))
	}
	d := reorgs[0]
	if d.ForkBlock != 2 || d.FromBlock != 3 || d.ToBlock != 3 || d.OldHash != "h3a" || d.NewHash != "h3b" {
		t.Errorf("reorg marker wrong: %+v (want fork=2 from=3 to=3 old=h3a new=h3b)", d)
	}
}
