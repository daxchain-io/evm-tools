package stream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/checkpoint"
	"github.com/daxchain-io/evm-tools/internal/rpc"
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

// fakeCheckpoint is an in-memory Checkpointer for resume/save tests.
type fakeCheckpoint struct {
	mu      sync.Mutex
	cur     checkpoint.Cursor
	loaded  bool
	saves   []checkpoint.Cursor
	loadErr error
}

func (c *fakeCheckpoint) Load() (checkpoint.Cursor, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cur, c.loaded, c.loadErr
}
func (c *fakeCheckpoint) Save(cur checkpoint.Cursor) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cur, c.loaded = cur, true
	c.saves = append(c.saves, cur)
	return nil
}
func (c *fakeCheckpoint) lastSave() (checkpoint.Cursor, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.saves) == 0 {
		return checkpoint.Cursor{}, false
	}
	return c.saves[len(c.saves)-1], true
}

// TestResumeFromCheckpoint verifies a present cursor resumes from cursor+1,
// overriding from_block=latest.
func TestResumeFromCheckpoint(t *testing.T) {
	fc := &fakeClient{chainID: 1, heads: []uint64{100}}
	cp := &fakeCheckpoint{cur: checkpoint.Cursor{ChainID: 1, LastBlock: 42}, loaded: true}
	s, err := New(Options{
		Client: fc, Emitter: &captureEmitter{}, Metrics: &finalizedMetrics{},
		ChainName: "test", ChainID: 1, PollInterval: time.Second, LogChunkBlocks: 2000,
		FromBlock: "latest", Checkpoint: cp,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	start, err := s.resolveStart(context.Background())
	if err != nil {
		t.Fatalf("resolveStart: %v", err)
	}
	if start != 43 {
		t.Errorf("resume should start at cursor+1 (43), got %d", start)
	}
}

// TestCheckpointIgnoredOnChainMismatch verifies a cursor from a different chain
// is ignored, falling back to from_block.
func TestCheckpointIgnoredOnChainMismatch(t *testing.T) {
	fc := &fakeClient{chainID: 1, heads: []uint64{100}}
	cp := &fakeCheckpoint{cur: checkpoint.Cursor{ChainID: 999, LastBlock: 42}, loaded: true}
	s, err := New(Options{
		Client: fc, Emitter: &captureEmitter{}, Metrics: &finalizedMetrics{},
		ChainName: "test", ChainID: 1, PollInterval: time.Second, LogChunkBlocks: 2000,
		FromBlock: "7", Checkpoint: cp,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	start, err := s.resolveStart(context.Background())
	if err != nil {
		t.Fatalf("resolveStart: %v", err)
	}
	if start != 7 {
		t.Errorf("mismatched-chain cursor must be ignored; want from_block 7, got %d", start)
	}
}

// TestCheckpointSavedAfterPoll verifies a successful poll persists the highest
// processed block.
func TestCheckpointSavedAfterPoll(t *testing.T) {
	fc := &fakeClient{chainID: 1, heads: []uint64{5}}
	cp := &fakeCheckpoint{}
	s, err := New(Options{
		Client: fc, Emitter: &captureEmitter{}, Metrics: &finalizedMetrics{},
		ChainName: "test", ChainID: 1, PollInterval: 5 * time.Millisecond, LogChunkBlocks: 2000,
		FromBlock: "1", Checkpoint: cp,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	waitFor(t, func() bool { c, ok := cp.lastSave(); return ok && c.LastBlock == 5 }, time.Second)
	cancel()
	<-done
	c, ok := cp.lastSave()
	if !ok || c.ChainID != 1 || c.LastBlock != 5 {
		t.Errorf("expected checkpoint {chain 1, block 5}, got %+v ok=%v", c, ok)
	}
}

func TestShouldSaveCheckpoint(t *testing.T) {
	cases := []struct {
		next, lastSaved uint64
		want            bool
		desc            string
	}{
		{102, 101, true, "advance persists"},
		{101, 101, false, "idle (unchanged) skips"},
		{99, 101, true, "reorg-lowered frontier persists"},
		{0, 0, false, "zero start skips (no underflow)"},
	}
	for _, c := range cases {
		if got := shouldSaveCheckpoint(c.next, c.lastSaved); got != c.want {
			t.Errorf("%s: shouldSaveCheckpoint(%d,%d)=%v, want %v", c.desc, c.next, c.lastSaved, got, c.want)
		}
	}
}

// TestCheckpointLoweredOnReorg drives polls that climb to head 3, then a poll
// where the head shortens to 2 with block 2 reorged. The persisted cursor must
// drop (not stay at the pre-reorg height), and a fresh resolveStart must resume
// from the lowered point so the re-mined blocks are re-processed — the gap the
// monotonic-guard bug would have caused.
func TestCheckpointLoweredOnReorg(t *testing.T) {
	tag := rpc.BlockTag
	reorged := false
	blockByNum := func(n uint64) *rpc.Block {
		switch n {
		case 1:
			return &rpc.Block{Number: tag(1), Hash: "h1", ParentHash: "h0"}
		case 2:
			if reorged {
				return &rpc.Block{Number: tag(2), Hash: "h2b", ParentHash: "h1"}
			}
			return &rpc.Block{Number: tag(2), Hash: "h2", ParentHash: "h1"}
		case 3:
			return &rpc.Block{Number: tag(3), Hash: "h3", ParentHash: "h2"}
		default:
			return &rpc.Block{Number: tag(n), Hash: "hx"}
		}
	}
	fc := &fakeClient{chainID: 1, heads: []uint64{1, 2, 3, 2}, blockByNum: blockByNum}
	cp := &fakeCheckpoint{}
	s, err := New(Options{
		Client: fc, Emitter: &captureEmitter{}, Metrics: &finalizedMetrics{},
		ChainName: "test", ChainID: 1, PollInterval: time.Second, LogChunkBlocks: 2000,
		FromBlock: "1", ReorgDepth: 8, Checkpoint: cp,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	nextBlock, lastSaved := uint64(1), uint64(1)
	for poll := 1; poll <= 4; poll++ {
		if poll == 4 {
			reorged = true
		}
		next, perr := s.pollOnce(ctx, nextBlock)
		if perr != nil {
			t.Fatalf("poll %d: %v", poll, perr)
		}
		if shouldSaveCheckpoint(next, lastSaved) {
			if serr := cp.Save(checkpoint.Cursor{ChainID: 1, LastBlock: next - 1}); serr != nil {
				t.Fatal(serr)
			}
			lastSaved = next
		}
		nextBlock = next
	}

	c, ok := cp.lastSave()
	if !ok || c.LastBlock > 2 {
		t.Fatalf("cursor must be lowered to <=2 after the shortening reorg, got %+v ok=%v", c, ok)
	}

	// A restart must resume from the lowered cursor (not the stale higher one).
	s2, err := New(Options{
		Client: fc, Emitter: &captureEmitter{}, Metrics: &finalizedMetrics{},
		ChainName: "test", ChainID: 1, PollInterval: time.Second, LogChunkBlocks: 2000,
		FromBlock: "latest", ReorgDepth: 8, Checkpoint: cp,
	})
	if err != nil {
		t.Fatalf("New(resume): %v", err)
	}
	start, err := s2.resolveStart(ctx)
	if err != nil {
		t.Fatalf("resolveStart: %v", err)
	}
	if start != c.LastBlock+1 {
		t.Errorf("restart should resume from cursor+1 (%d), got %d", c.LastBlock+1, start)
	}
}
