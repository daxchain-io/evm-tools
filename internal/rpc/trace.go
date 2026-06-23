package rpc

import (
	"context"
	"errors"
	"math/big"
	"strings"
)

// Trace method names (and metric operation labels) for the three backends used to
// surface internal native (ETH) transfers. evm-stream cascades through them: the
// efficient block-level geth tracer, the parity flat tracer (Erigon/Nethermind/
// anvil), then the per-transaction geth tracer, self-disabling if none respond.
const (
	OpDebugTraceBlock = "debug_traceBlockByNumber"
	OpDebugTraceTx    = "debug_traceTransaction"
	OpTraceBlock      = "trace_block"
)

// CallFrame is one node of a geth callTracer call tree (debug_traceBlockByNumber
// / debug_traceTransaction with tracer "callTracer"). Only the fields needed to
// surface internal native value transfers are decoded; calldata/output/gas are
// intentionally omitted (large, and input can carry data we must never log).
type CallFrame struct {
	Type  string      `json:"type"` // CALL, CALLCODE, DELEGATECALL, STATICCALL, CREATE, CREATE2, SELFDESTRUCT
	From  string      `json:"from"`
	To    string      `json:"to"` // beneficiary/destination; the created address for CREATE/CREATE2
	Value string      `json:"value"`
	Error string      `json:"error"` // non-empty when the frame reverted/failed (its value did not move)
	Calls []CallFrame `json:"calls"`
}

// ValueBig parses the frame's transferred value (wei); an absent/empty value is
// treated as zero (non-value calls omit the field).
func (f CallFrame) ValueBig() (*big.Int, error) {
	if f.Value == "" || f.Value == "0x" {
		return new(big.Int), nil
	}
	return hexBig(f.Value)
}

// TxTrace pairs a transaction hash with its traced top-level call frame, as
// returned per transaction by debug_traceBlockByNumber under callTracer.
type TxTrace struct {
	TxHash string     `json:"txHash"`
	Result *CallFrame `json:"result"`
}

// TraceBlockByNumber traces every transaction in a block with geth's callTracer
// in a SINGLE RPC call. It reads under the larger trace body cap. Nodes without
// the debug_ namespace reject it (caught by [IsMethodUnsupported]).
func (c *Client) TraceBlockByNumber(ctx context.Context, n uint64) ([]TxTrace, error) {
	var out []TxTrace
	if err := c.callLimited(ctx, OpDebugTraceBlock, &out, traceMaxBodyBytes, BlockTag(n), callTracerConfig(false)); err != nil {
		return nil, err
	}
	return out, nil
}

// TraceTransaction traces a single transaction with geth's callTracer — the
// per-tx fallback for nodes that expose debug_traceTransaction but not the block
// variant (e.g. anvil, older geth). Returns the top-level call frame.
func (c *Client) TraceTransaction(ctx context.Context, txHash string) (*CallFrame, error) {
	var out *CallFrame
	if err := c.callLimited(ctx, OpDebugTraceTx, &out, traceMaxBodyBytes, txHash, callTracerConfig(false)); err != nil {
		return nil, err
	}
	return out, nil
}

// ProbeTraceBlock issues a cheap debug_traceBlockByNumber with onlyTopCall=true
// (a tiny response: just each tx's top frame), so a capability probe is not
// derailed by a heavy block's size/latency. Used by `check rpc`.
func (c *Client) ProbeTraceBlock(ctx context.Context, n uint64) error {
	var out []TxTrace
	return c.callLimited(ctx, OpDebugTraceBlock, &out, defaultMaxBodyBytes, BlockTag(n), callTracerConfig(true))
}

func callTracerConfig(onlyTopCall bool) map[string]any {
	return map[string]any{
		"tracer":       "callTracer",
		"tracerConfig": map[string]any{"onlyTopCall": onlyTopCall, "withLog": false},
	}
}

// ParityTrace is one flat entry of an OpenEthereum/parity trace_block result
// (Erigon, Nethermind, Besu, anvil). Unlike callTracer's nested tree, parity
// returns a flat list where each entry already carries its traceAddress (the call
// path) — the field the internal-transfer dedup key needs.
type ParityTrace struct {
	Type         string       `json:"type"` // "call", "create", "suicide", "reward"
	Action       ParityAction `json:"action"`
	Result       *ParityRes   `json:"result"`
	Error        string       `json:"error"`        // non-empty when this trace failed
	TraceAddress []int        `json:"traceAddress"` // call path from the top-level ([] for the root)
	TxHash       string       `json:"transactionHash"`
}

// ParityAction is the per-type action payload of a parity trace.
type ParityAction struct {
	CallType      string `json:"callType"`      // call/callcode/delegatecall/staticcall (type=="call")
	From          string `json:"from"`          // call/create caller
	To            string `json:"to"`            // call recipient
	Value         string `json:"value"`         // call value / create endowment
	Address       string `json:"address"`       // suicide: the self-destructing contract
	RefundAddress string `json:"refundAddress"` // suicide: the beneficiary
	Balance       string `json:"balance"`       // suicide: the swept balance
}

// ParityRes is the result payload; for a create it carries the new address.
type ParityRes struct {
	Address string `json:"address"` // create/create2: the created contract address
}

// TraceBlockParity traces a block via the parity trace_block method in a single
// call, returning the flat trace list. For nodes that expose the trace_ namespace
// but not debug_traceBlockByNumber.
func (c *Client) TraceBlockParity(ctx context.Context, n uint64) ([]ParityTrace, error) {
	var out []ParityTrace
	if err := c.callLimited(ctx, OpTraceBlock, &out, traceMaxBodyBytes, BlockTag(n)); err != nil {
		return nil, err
	}
	return out, nil
}

// IsMethodUnsupported reports whether err means the RPC endpoint does not expose
// the called method — a definitive capability gap, as opposed to a transient
// failure. It recognizes a method-rejecting HTTP status (a proxy/provider
// blocking a namespace) and a JSON-RPC method-not-found error. Timeouts,
// connection errors, 5xx, decode/too-large, and any other rpc error are NOT
// capability gaps (they are transient or per-call) and return false, so a blip
// never permanently disables a trace backend.
func IsMethodUnsupported(err error) bool {
	var he *HTTPError
	if errors.As(err, &he) {
		switch he.Status {
		case 401, 403, 404, 405, 501:
			return true
		default:
			return false // 5xx / 429 / other: transient
		}
	}
	if Classify(err) != ErrorRPC {
		return false
	}
	// Match only phrasings specific to a MISSING METHOD. Bare "not available" /
	// "not supported" are deliberately excluded — they also appear in transient
	// operational rpc errors, and a false match here permanently disables a working
	// backend. geth's canonical "the method X does not exist/is not available" is
	// still caught by "does not exist" (and by the -32601 code).
	msg := strings.ToLower(err.Error())
	for _, sub := range []string{
		"-32601", // JSON-RPC "method not found"
		"method not found",
		"does not exist",
		"method not enabled",
		"unsupported method",
	} {
		if strings.Contains(msg, sub) {
			return true
		}
	}
	return false
}
