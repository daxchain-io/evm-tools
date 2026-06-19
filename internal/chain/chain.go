// Package chain holds chain metadata and block/header helpers shared by the
// producers: resolving the EVM chain ID, reading the head block, and fetching
// blocks with their transactions. It is a thin, RPC-backed layer so the
// per-tool logic (stream poll loop, balance pollers) can stay focused.
package chain

import (
	"context"
	"errors"
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
	// Name is the operator-configured chain name (e.g. "codex-chain"); it is a
	// label, not derived from the node.
	Name string
	// ID is the EVM chain ID resolved via eth_chainId.
	ID int64
}

// Resolve reads the chain ID from the node and pairs it with the configured
// name. The JSON safe-integer guarantee on chain_id is enforced here so the
// envelope's bare-number encoding stays safe.
func Resolve(ctx context.Context, r Reader, name string) (Info, error) {
	id, err := r.ChainID(ctx)
	if err != nil {
		return Info{}, err
	}
	if id < 0 || id > maxSafeInteger {
		return Info{}, errChainIDOutOfRange(id)
	}
	return Info{Name: name, ID: id}, nil
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
