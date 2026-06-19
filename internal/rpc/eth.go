package rpc

import (
	"context"
	"fmt"
	"math/big"
	"strings"
)

// Operation names used as the `operation` metric label and in method calls.
const (
	OpChainID            = "eth_chainId"
	OpBlockNumber        = "eth_blockNumber"
	OpGetBlockByNumber   = "eth_getBlockByNumber"
	OpGetLogs            = "eth_getLogs"
	OpGetTransactionRcpt = "eth_getTransactionReceipt"
	OpGetBalance         = "eth_getBalance"
	OpCall               = "eth_call"
)

// hexUint parses a 0x-prefixed hex quantity into a uint64.
func hexUint(s string) (uint64, error) {
	v, ok := new(big.Int).SetString(strings.TrimPrefix(strings.TrimPrefix(s, "0x"), "0X"), 16)
	if !ok {
		return 0, fmt.Errorf("invalid hex quantity %q", s)
	}
	if !v.IsUint64() {
		return 0, fmt.Errorf("hex quantity %q overflows uint64", s)
	}
	return v.Uint64(), nil
}

// hexBig parses a 0x-prefixed hex quantity into a *big.Int (for 256-bit values).
func hexBig(s string) (*big.Int, error) {
	v, ok := new(big.Int).SetString(strings.TrimPrefix(strings.TrimPrefix(s, "0x"), "0X"), 16)
	if !ok {
		return nil, fmt.Errorf("invalid hex quantity %q", s)
	}
	return v, nil
}

// ChainID resolves the EVM chain ID via eth_chainId.
func (c *Client) ChainID(ctx context.Context) (int64, error) {
	var hex string
	if err := c.call(ctx, OpChainID, &hex); err != nil {
		return 0, err
	}
	v, err := hexUint(hex)
	if err != nil {
		return 0, &decodeError{op: OpChainID, err: err}
	}
	return int64(v), nil
}

// BlockNumber returns the latest block number via eth_blockNumber.
func (c *Client) BlockNumber(ctx context.Context) (uint64, error) {
	var hex string
	if err := c.call(ctx, OpBlockNumber, &hex); err != nil {
		return 0, err
	}
	v, err := hexUint(hex)
	if err != nil {
		return 0, &decodeError{op: OpBlockNumber, err: err}
	}
	return v, nil
}

// Transaction is a single transaction in a full block.
type Transaction struct {
	Hash  string `json:"hash"`
	From  string `json:"from"`
	To    string `json:"to"` // empty for contract-creation
	Value string `json:"value"`
}

// ValueBig parses the transaction value (wei) as a *big.Int.
func (t Transaction) ValueBig() (*big.Int, error) { return hexBig(t.Value) }

// Block is the subset of eth_getBlockByNumber fields the stream needs.
type Block struct {
	Number       string        `json:"number"`
	Hash         string        `json:"hash"`
	Timestamp    string        `json:"timestamp"`
	Transactions []Transaction `json:"transactions"`
}

// NumberUint parses the block number.
func (b Block) NumberUint() (uint64, error) { return hexUint(b.Number) }

// TimestampUint parses the block timestamp (unix seconds).
func (b Block) TimestampUint() (uint64, error) { return hexUint(b.Timestamp) }

// BlockByNumber fetches a block. When full is true the Transactions slice is
// populated with full transaction objects; otherwise only the header fields are
// meaningful. tag is a block number or one of "latest"/"earliest"/"pending".
func (c *Client) BlockByNumber(ctx context.Context, tag string, full bool) (*Block, error) {
	var blk *Block
	if err := c.call(ctx, OpGetBlockByNumber, &blk, tag, full); err != nil {
		return nil, err
	}
	if blk == nil {
		return nil, fmt.Errorf("rpc: block %q not found", tag)
	}
	return blk, nil
}

// BlockByNumberUint is BlockByNumber for a concrete height.
func (c *Client) BlockByNumberUint(ctx context.Context, n uint64, full bool) (*Block, error) {
	return c.BlockByNumber(ctx, BlockTag(n), full)
}

// BlockTag formats a block height as the 0x-hex tag eth_* methods expect.
func BlockTag(n uint64) string { return "0x" + new(big.Int).SetUint64(n).Text(16) }

// Log is a single eth_getLogs result.
type Log struct {
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	BlockNumber string   `json:"blockNumber"`
	BlockHash   string   `json:"blockHash"`
	TxHash      string   `json:"transactionHash"`
	LogIndex    string   `json:"logIndex"`
	Removed     bool     `json:"removed"`
}

// BlockNumberUint parses the log's block number.
func (l Log) BlockNumberUint() (uint64, error) { return hexUint(l.BlockNumber) }

// LogIndexUint parses the log index within its block.
func (l Log) LogIndexUint() (uint64, error) { return hexUint(l.LogIndex) }

// LogFilter is the eth_getLogs filter object. FromBlock/ToBlock are inclusive
// block heights; Addresses and Topics scope the query.
type LogFilter struct {
	FromBlock uint64
	ToBlock   uint64
	Addresses []string
	// Topics is the JSON-RPC topic filter. Each position may be a single
	// topic (string), a set (any-of, []string), or nil (wildcard).
	Topics []any
}

// MarshalJSON renders the filter in the eth_getLogs param shape.
func (f LogFilter) toParam() map[string]any {
	p := map[string]any{
		"fromBlock": BlockTag(f.FromBlock),
		"toBlock":   BlockTag(f.ToBlock),
	}
	if len(f.Addresses) > 0 {
		p["address"] = f.Addresses
	}
	if len(f.Topics) > 0 {
		p["topics"] = f.Topics
	}
	return p
}

// GetLogs runs eth_getLogs for the given filter.
func (c *Client) GetLogs(ctx context.Context, f LogFilter) ([]Log, error) {
	var logs []Log
	if err := c.call(ctx, OpGetLogs, &logs, f.toParam()); err != nil {
		return nil, err
	}
	return logs, nil
}

// Receipt is the subset of eth_getTransactionReceipt the stream needs to gate
// native transfers on execution success.
type Receipt struct {
	Status          string `json:"status"`
	TxHash          string `json:"transactionHash"`
	ContractAddress string `json:"contractAddress"`
}

// Succeeded reports whether the receipt status is 1 (success). Pre-Byzantium
// receipts without a status field are treated as not-success so reverted-value
// transactions are never emitted.
func (r Receipt) Succeeded() bool {
	v, err := hexUint(r.Status)
	if err != nil {
		return false
	}
	return v == 1
}

// TransactionReceipt fetches a receipt by transaction hash. A nil receipt
// (transaction not yet mined) is returned as (nil, nil).
func (c *Client) TransactionReceipt(ctx context.Context, txHash string) (*Receipt, error) {
	var r *Receipt
	if err := c.call(ctx, OpGetTransactionRcpt, &r, txHash); err != nil {
		return nil, err
	}
	return r, nil
}

// BalanceAt returns the native (wei) balance of an account at the given block
// tag ("latest" or a 0x-hex block number) via eth_getBalance.
func (c *Client) BalanceAt(ctx context.Context, address, blockTag string) (*big.Int, error) {
	var hex string
	if err := c.call(ctx, OpGetBalance, &hex, address, blockTag); err != nil {
		return nil, err
	}
	v, err := hexBig(hex)
	if err != nil {
		return nil, &decodeError{op: OpGetBalance, err: err}
	}
	return v, nil
}

// CallMsg is the subset of an eth_call message the balance poller needs: a
// destination contract and ABI-encoded call data.
type CallMsg struct {
	To   string // contract address (0x-hex)
	Data string // ABI-encoded calldata (0x-hex)
}

func (m CallMsg) toParam() map[string]any {
	return map[string]any{"to": m.To, "data": m.Data}
}

// Call performs a read-only eth_call against a contract at the given block tag
// ("latest" or a 0x-hex block number) and returns the raw 0x-hex result. The
// caller ABI-decodes the result.
func (c *Client) Call(ctx context.Context, msg CallMsg, blockTag string) (string, error) {
	var hex string
	if err := c.call(ctx, OpCall, &hex, msg.toParam(), blockTag); err != nil {
		return "", err
	}
	return hex, nil
}
