package stream

import (
	"context"
	"math/big"
	"strings"
	"sync"

	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// nativeReceiptConcurrency bounds how many per-transaction receipt fetches run in
// parallel within a single block. Native-transfer detection needs one
// eth_getTransactionReceipt per value-bearing tx to gate out reverted txs; a busy
// block holds hundreds, so fetching them serially makes the producer fall behind
// head. A small fixed pool parallelizes the round-trips without hammering the RPC
// endpoint.
const nativeReceiptConcurrency = 8

// NativeFilter scopes native transfer emission. With no addresses set, every
// value-bearing transaction qualifies (the full firehose). When From or To is
// non-empty, a transaction qualifies only if its from/to is in the respective
// allowlist (the lists are ORed: matching either list qualifies, matching the
// design's "from/to allowlist to scope the stream").
type NativeFilter struct {
	enabled bool
	// includeInternal also surfaces value transfers made inside a transaction's
	// execution (via trace RPC), not just the top-level transaction value. It is a
	// refinement of native-transfer detection and shares the from/to allowlist.
	includeInternal bool
	from            map[string]bool
	to              map[string]bool
}

// NativeFilterFromConfig builds the allowlist filter from the
// [stream.native_transfers] section.
func NativeFilterFromConfig(cfg config.NativeTransfersConfig) NativeFilter {
	f := NativeFilter{enabled: cfg.Enabled, includeInternal: cfg.IncludeInternal}
	if len(cfg.From) > 0 {
		f.from = toLowerSet(cfg.From)
	}
	if len(cfg.To) > 0 {
		f.to = toLowerSet(cfg.To)
	}
	return f
}

// matches reports whether a transaction's from/to passes the allowlist. With no
// allowlist configured every transaction matches.
func (f NativeFilter) matches(from, to string) bool {
	if f.from == nil && f.to == nil {
		return true
	}
	from = strings.ToLower(from)
	to = strings.ToLower(to)
	if f.from != nil && f.from[from] {
		return true
	}
	if f.to != nil && f.to[to] {
		return true
	}
	return false
}

func toLowerSet(in []string) map[string]bool {
	m := make(map[string]bool, len(in))
	for _, s := range in {
		m[strings.ToLower(s)] = true
	}
	return m
}

// nativeTransfer is a detected, success-gated top-level value transfer ready to
// be emitted.
type nativeTransfer struct {
	tx               rpc.Transaction
	valueWei         *big.Int
	contractCreation bool
}

// detectNativeTransfers scans a full block's transactions for value-bearing
// top-level transfers that pass the allowlist and whose receipt status is
// success (status == 1). Reverted transactions carry value in the body but
// transfer nothing, so they are gated out. A contract-creation tx (to is null)
// that carries value is flagged with contract_creation.
//
// Receipts are fetched per qualifying transaction; failures propagate so the
// caller can retry the whole block rather than silently dropping a transfer.
func detectNativeTransfers(ctx context.Context, r Client, f NativeFilter, blk *rpc.Block) ([]nativeTransfer, error) {
	if !f.enabled {
		return nil, nil
	}

	// First pass (no RPC): collect value-bearing, allowlisted candidates in block
	// order so output order is deterministic regardless of receipt-fetch ordering.
	cands := make([]nativeTransfer, 0, len(blk.Transactions))
	for _, tx := range blk.Transactions {
		val, err := tx.ValueBig()
		if err != nil {
			return nil, err
		}
		if val.Sign() == 0 {
			continue // no value moved
		}
		if !f.matches(tx.From, tx.To) {
			continue
		}
		cands = append(cands, nativeTransfer{tx: tx, valueWei: val, contractCreation: tx.To == ""})
	}
	if len(cands) == 0 {
		return nil, nil
	}

	// Second pass: fetch each candidate's receipt concurrently (bounded) to gate
	// out reverted txs. A single receipt error fails the whole block so the caller
	// retries it rather than silently dropping a transfer (lossless); the first
	// error cancels the rest.
	succeeded := make([]bool, len(cands))
	fctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sem := make(chan struct{}, nativeReceiptConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for i := range cands {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			if fctx.Err() != nil {
				return
			}
			rcpt, err := r.TransactionReceipt(fctx, cands[i].tx.Hash)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				mu.Unlock()
				return
			}
			succeeded[i] = rcpt != nil && rcpt.Succeeded()
		}(i)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}

	out := make([]nativeTransfer, 0, len(cands))
	for i := range cands {
		if succeeded[i] {
			out = append(out, cands[i])
		}
	}
	return out, nil
}

// weiToEther renders a wei amount as an ether-decimal string (18 decimals),
// trimming trailing zeros but keeping at least one fractional digit so values
// like 1 wei don't print as bare "0".
func weiToEther(wei *big.Int) string {
	return formatUnits(wei, 18)
}

// formatUnits renders an integer amount scaled by 10^decimals as a decimal
// string. It trims trailing fractional zeros, leaving "x.0" for whole numbers.
func formatUnits(amount *big.Int, decimals int) string {
	if decimals <= 0 {
		return amount.String()
	}
	neg := amount.Sign() < 0
	abs := new(big.Int).Abs(amount)

	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	intPart := new(big.Int)
	fracPart := new(big.Int)
	intPart.DivMod(abs, scale, fracPart)

	// Zero-pad the fractional part to `decimals` digits.
	frac := fracPart.String()
	if len(frac) < decimals {
		frac = strings.Repeat("0", decimals-len(frac)) + frac
	}
	frac = strings.TrimRight(frac, "0")
	if frac == "" {
		frac = "0"
	}

	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	b.WriteString(intPart.String())
	b.WriteByte('.')
	b.WriteString(frac)
	return b.String()
}
