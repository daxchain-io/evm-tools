package record

import (
	"strconv"
	"strings"
)

// DedupKey derives the record's deduplication / resume identity from the
// envelope, exactly as specified in docs/design.md ("Deduplication and resume
// keys"). Records carry no explicit id; a sink derives one from the envelope,
// and the components differ by record class so the key is genuinely unique and
// reorg-stable for each class:
//
//   - event:           chain_id + tx_hash + log_index (reorg-stable; block_hash
//     is excluded so a log re-observed after a reorg yields the same key).
//   - native_transfer: chain_id + tx_hash (one top-level transfer per tx).
//   - *_sample:         chain_id + type + name + block_number + emitted_at
//     (interval cadence can sample one block more than once, so emitted_at
//     disambiguates; a sink wanting one row per block can ignore the tail).
//   - *_change:         chain_id + type + name + block_number (the block at which
//     the new value was first observed).
//
// The returned key is a stable, printable string suitable as a Kafka partition
// key (per-key ordering) and as a consumer-side dedup key. It never embeds a
// secret. Components are joined with a delimiter that cannot appear in the
// 0x-hex / numeric / configured-name components, so the encoding is unambiguous.
func (e Envelope) DedupKey() string {
	const sep = "|"
	chainID := strconv.FormatInt(e.ChainID, 10)

	switch e.Type {
	case TypeEvent:
		return strings.Join([]string{chainID, e.TxHash, logIndexStr(e.LogIndex)}, sep)
	case TypeNativeTransfer:
		return strings.Join([]string{chainID, e.TxHash}, sep)
	case TypeBalanceSample, TypeOwnershipSample, TypeContractSample:
		return strings.Join([]string{
			chainID, string(e.Type), e.Name,
			strconv.FormatUint(e.BlockNumber, 10), e.EmittedAt,
		}, sep)
	case TypeBalanceChange, TypeOwnershipChange, TypeContractChange:
		return strings.Join([]string{
			chainID, string(e.Type), e.Name,
			strconv.FormatUint(e.BlockNumber, 10),
		}, sep)
	default:
		// Unknown future type: fall back to a key that is still stable per
		// (chain, type, name, block) so ordering is preserved per logical entry.
		return strings.Join([]string{
			chainID, string(e.Type), e.Name,
			strconv.FormatUint(e.BlockNumber, 10), e.EmittedAt,
		}, sep)
	}
}

// PartitionIdentity returns the per-record value that, when used as a Kafka
// partition key, keeps records that share a logical identity on one partition so
// their relative order is preserved. It is the dedup identity *without* the
// emitted_at disambiguator for sampled records, so every sample of the same
// (chain, type, name, block-stream) lands on one partition and stays ordered;
// the full DedupKey (with emitted_at) remains the consumer-side dedup key.
//
// For transaction-backed records the identity is the dedup key itself.
func (e Envelope) PartitionIdentity() string {
	const sep = "|"
	chainID := strconv.FormatInt(e.ChainID, 10)

	switch e.Type {
	case TypeEvent:
		return strings.Join([]string{chainID, e.TxHash, logIndexStr(e.LogIndex)}, sep)
	case TypeNativeTransfer:
		return strings.Join([]string{chainID, e.TxHash}, sep)
	default:
		// Sampled and change records: order is meaningful per configured entry,
		// so partition by (chain, type, name) — every record for one entry stays
		// on a single partition and keeps its block order.
		return strings.Join([]string{chainID, string(e.Type), e.Name}, sep)
	}
}

// logIndexStr renders a *uint64 log index, using "_" for the absent case so the
// key stays well-formed for non-log records that somehow reach this path.
func logIndexStr(p *uint64) string {
	if p == nil {
		return "_"
	}
	return strconv.FormatUint(*p, 10)
}
