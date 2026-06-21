package stream

import (
	"context"
	"strings"

	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// reorgTracker remembers the canonical block hash the stream recorded for each
// recent block height, so a later poll can detect that a block it already
// processed (and emitted records for) was orphaned by a chain reorganization.
//
// To stay cheap it tracks only the chain *tip* the loop confirms each poll (the
// block height becomes the processing frontier on the next poll), bounded to the
// most recent `depth` heights. Deep history is final, so it is never tracked and
// reorgs there are not handled — `depth` is the maximum reorg the stream will
// detect and rewind across. The map is accessed only from the single Run
// goroutine, so it needs no synchronization.
type reorgTracker struct {
	depth  uint64
	hashes map[uint64]string
}

// newReorgTracker returns a tracker for the given depth, or nil when depth is 0
// (reorg handling disabled). A nil *reorgTracker is safe for every method.
func newReorgTracker(depth uint64) *reorgTracker {
	if depth == 0 {
		return nil
	}
	return &reorgTracker{depth: depth, hashes: map[uint64]string{}}
}

// recordHash stores the canonical hash for height n and prunes entries older
// than the depth window relative to head. An empty hash is ignored.
func (r *reorgTracker) recordHash(n uint64, hash string, head uint64) {
	if r == nil || hash == "" {
		return
	}
	r.hashes[n] = hash
	if head >= r.depth {
		floor := head - r.depth
		for k := range r.hashes {
			if k < floor {
				delete(r.hashes, k)
			}
		}
	}
}

// recorded returns the canonical hash tracked for height n, if any.
func (r *reorgTracker) recorded(n uint64) (string, bool) {
	if r == nil {
		return "", false
	}
	h, ok := r.hashes[n]
	return h, ok
}

// forgetAbove drops every tracked height strictly greater than n. It is called
// after a rewind so orphaned hashes do not linger.
func (r *reorgTracker) forgetAbove(n uint64) {
	if r == nil {
		return
	}
	for k := range r.hashes {
		if k > n {
			delete(r.hashes, k)
		}
	}
}

// handleReorgIfAny checks whether the processing frontier (nextBlock-1) is still
// canonical and, if not, emits a reorg marker over the orphaned range and returns
// the rewound nextBlock so the caller re-scans the new canonical chain. It returns
// (nextBlock, false, nil) when there is no reorg (or reorg handling is disabled,
// or the frontier is not tracked yet — i.e. still backfilling). headBlk is the
// already-fetched head header (may be nil); when the head is exactly one past the
// frontier its parentHash lets the common case be checked with no extra RPC.
func (s *Stream) handleReorgIfAny(ctx context.Context, nextBlock, head uint64, headBlk *rpc.Block) (uint64, bool, error) {
	if s.reorg == nil || nextBlock == 0 {
		return nextBlock, false, nil
	}
	// Verify the highest processed block the node can still confirm. Normally that
	// is the frontier (nextBlock-1); but when the head has not advanced past the
	// frontier (a tip replaced in place, or a chain that shortened), nextBlock-1 is
	// at or above the current head, so clamp to head — the highest height the node
	// still has — so an in-place tip reorg is caught instead of being skipped by the
	// no-op path. (Blocks strictly above head that were processed are handled when
	// the head climbs back; see docs/design.md "Operational notes".)
	frontier := nextBlock - 1
	if frontier > head {
		frontier = head
	}
	recordedHash, ok := s.reorg.recorded(frontier)
	if !ok {
		// The frontier predates the tracked window (deep backfill): the blocks we
		// are about to process are final, so there is nothing to reorg-check yet.
		return nextBlock, false, nil
	}

	// Determine the node's current hash at the frontier, reusing the already-fetched
	// head header where possible so the common cases need no extra RPC: the head
	// block IS the frontier when they are equal (idle / in-place), and the head's
	// parentHash IS the frontier's canonical hash when the head is one past it.
	nodeHash := ""
	switch {
	case headBlk != nil && head == frontier && headBlk.Hash != "":
		nodeHash = headBlk.Hash
	case headBlk != nil && head == frontier+1 && headBlk.ParentHash != "":
		nodeHash = headBlk.ParentHash
	default:
		blk, err := s.opts.Client.BlockByNumberUint(ctx, frontier, false)
		if err != nil {
			return nextBlock, false, err
		}
		nodeHash = blk.Hash
	}
	if strings.EqualFold(nodeHash, recordedHash) {
		return nextBlock, false, nil // frontier still canonical: no reorg
	}

	// The frontier block was orphaned. Resolve the fork point and emit the marker.
	data, err := s.resolveFork(ctx, frontier, recordedHash)
	if err != nil {
		return nextBlock, false, err
	}
	if err := s.emitReorg(data); err != nil {
		return nextBlock, false, err
	}
	s.opts.Metrics.IncReorgsDetected()
	s.log.Warn("chain reorg detected; retracting orphaned range and re-scanning canonical chain",
		"fork_block", data.ForkBlock, "from", data.FromBlock, "to", data.ToBlock,
		"depth", data.Depth, "depth_exceeded", data.DepthExceeded)

	s.reorg.forgetAbove(data.ForkBlock)
	return data.ForkBlock + 1, true, nil
}

// resolveFork walks back from the orphaned tip to find the highest tracked block
// whose recorded hash still matches the node — the common ancestor (fork point).
// If none is found within the tracked depth the reorg is deeper than the stream
// tracks: the fork is pinned to the window floor and DepthExceeded is set.
func (s *Stream) resolveFork(ctx context.Context, orphanTip uint64, orphanTipHash string) (record.ReorgData, error) {
	floor := uint64(0)
	if orphanTip > s.reorg.depth {
		floor = orphanTip - s.reorg.depth
	}

	newHash := s.canonicalHashAt(ctx, orphanTip) // best-effort; "" if tip no longer exists

	for n := orphanTip; n > floor; {
		n--
		recordedHash, ok := s.reorg.recorded(n)
		if !ok {
			continue // height not tracked (sparse window): keep walking down
		}
		blk, err := s.opts.Client.BlockByNumberUint(ctx, n, false)
		if err != nil {
			return record.ReorgData{}, err
		}
		if strings.EqualFold(blk.Hash, recordedHash) {
			return record.ReorgData{
				ForkBlock: n,
				FromBlock: n + 1,
				ToBlock:   orphanTip,
				Depth:     int(orphanTip - n),
				OldHash:   orphanTipHash,
				NewHash:   newHash,
			}, nil
		}
	}

	// No common ancestor within the tracked depth: pin the fork to the floor and
	// flag that records below FromBlock may also be affected.
	return record.ReorgData{
		ForkBlock:     floor,
		FromBlock:     floor + 1,
		ToBlock:       orphanTip,
		Depth:         int(orphanTip - floor),
		OldHash:       orphanTipHash,
		NewHash:       newHash,
		DepthExceeded: true,
	}, nil
}

// canonicalHashAt returns the node's current hash at height n, or "" if the block
// no longer exists (the reorg shortened the chain past it) or the fetch fails.
// It is best-effort provenance for the reorg record and never fails the poll.
func (s *Stream) canonicalHashAt(ctx context.Context, n uint64) string {
	blk, err := s.opts.Client.BlockByNumberUint(ctx, n, false)
	if err != nil || blk == nil {
		return ""
	}
	return blk.Hash
}

// emitReorg writes the reorg marker record, wrapping a write failure as an
// emitErr so the run loop treats a broken downstream as terminal.
func (s *Stream) emitReorg(data record.ReorgData) error {
	env := record.Envelope{
		Type:        record.TypeReorg,
		Tool:        record.ToolStream,
		Name:        "reorg",
		Chain:       s.opts.ChainName,
		ChainID:     s.opts.ChainID,
		BlockNumber: data.ToBlock,
		BlockHash:   data.NewHash,
		Data:        data,
	}
	if err := s.opts.Emitter.Emit(env); err != nil {
		return &emitErr{err: err}
	}
	return nil
}

// reorgEnabled reports whether reorg handling is active.
func (s *Stream) reorgEnabled() bool { return s.reorg != nil }
