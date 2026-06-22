// Package chain holds chain metadata and block/header helpers shared by the
// producers: resolving the EVM chain ID, reading the head block, and fetching
// blocks with their transactions. It is a thin, RPC-backed layer so the
// per-tool logic (stream poll loop, balance pollers) can stay focused.
package chain

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// ErrNotImplemented is retained for callers that referenced the scaffold.
var ErrNotImplemented = errors.New("chain: not implemented")

// Reader is the RPC surface the chain helpers depend on. *rpc.Client satisfies
// it; tests can substitute a fake.
type Reader interface {
	ChainID(ctx context.Context) (int64, error)
	BlockNumber(ctx context.Context) (uint64, error)
	BlockByNumberUint(ctx context.Context, n uint64, full bool) (*rpc.Block, error)
	GetLogs(ctx context.Context, f rpc.LogFilter) ([]rpc.Log, error)
	TransactionReceipt(ctx context.Context, txHash string) (*rpc.Receipt, error)
}

// Info is the resolved metadata for a configured chain.
type Info struct {
	// Name is the chain label used in records and metrics. It is the
	// operator-configured name when one is given (--chain / [chain]); when that is
	// blank it is derived from the resolved chain ID (see NameForID).
	Name string
	// ID is the EVM chain ID resolved via eth_chainId.
	ID int64
}

// Resolve reads the chain ID from the node and pairs it with a chain label. A
// non-empty configured name is used as-is; a blank one is filled from the chain
// ID via NameForID, so records/metrics still carry a meaningful label without the
// operator having to pass --chain. The JSON safe-integer guarantee on chain_id is
// enforced here so the envelope's bare-number encoding stays safe.
func Resolve(ctx context.Context, r Reader, name string) (Info, error) {
	id, err := r.ChainID(ctx)
	if err != nil {
		return Info{}, err
	}
	if id < 0 || id > maxSafeInteger {
		return Info{}, errChainIDOutOfRange(id)
	}
	if strings.TrimSpace(name) == "" {
		name = NameForID(id)
	}
	return Info{Name: name, ID: id}, nil
}

// canonicalNames maps well-known EVM chain IDs to a short, lowercase label,
// mirroring the community chain registry (github.com/ethereum-lists/chains) for
// the chains evm-tools users are most likely to point at. It is a small curated
// set, not exhaustive, and is consulted only to fill a blank chain label — an
// explicit --chain / [chain] always wins, and chain_id stays authoritative.
var canonicalNames = map[int64]string{
	// Ethereum + its active testnets.
	1:        "ethereum",
	11155111: "sepolia",
	17000:    "holesky",
	// OP stack.
	10:       "optimism",
	11155420: "optimism-sepolia",
	8453:     "base",
	84532:    "base-sepolia",
	// Arbitrum.
	42161:  "arbitrum",
	42170:  "arbitrum-nova",
	421614: "arbitrum-sepolia",
	// Other major EVM L1/L2s.
	137:     "polygon",
	56:      "bsc",
	43114:   "avalanche",
	100:     "gnosis",
	324:     "zksync-era",
	59144:   "linea",
	534352:  "scroll",
	5000:    "mantle",
	81457:   "blast",
	7777777: "zora",
	250:     "fantom",
	42220:   "celo",
}

// NameForID returns the canonical lowercase label for a known chain ID, or
// "chain-<id>" for an unrecognized one, so the chain label is never empty.
func NameForID(id int64) string {
	if n, ok := canonicalNames[id]; ok {
		return n
	}
	return fmt.Sprintf("chain-%d", id)
}

// maxSafeInteger is 2^53 - 1, the largest integer JSON/float64 round-trips
// exactly. chain_id is guaranteed within this range.
const maxSafeInteger = 1<<53 - 1

func errChainIDOutOfRange(id int64) error {
	return &OutOfRangeError{ID: id}
}

// OutOfRangeError reports a chain ID outside the JSON safe-integer range.
type OutOfRangeError struct{ ID int64 }

func (e *OutOfRangeError) Error() string {
	return "chain: resolved chain_id outside the JSON safe-integer range"
}

// Head reads the latest block number.
func Head(ctx context.Context, r Reader) (uint64, error) {
	return r.BlockNumber(ctx)
}

// BlockTime converts a block's unix-seconds timestamp into a time.Time in UTC.
func BlockTime(b *rpc.Block) (time.Time, bool) {
	if b == nil {
		return time.Time{}, false
	}
	ts, err := b.TimestampUint()
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(int64(ts), 0).UTC(), true
}
