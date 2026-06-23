package stream

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// internalMetrics records the internal-transfer metric calls for assertions.
type internalMetrics struct {
	noopMetrics
	mu       sync.Mutex
	count    int
	skipped  int
	disabled bool
}

func (m *internalMetrics) IncInternalTransferRecord() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.count++
}
func (m *internalMetrics) IncInternalTraceSkipped() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.skipped++
}
func (m *internalMetrics) SetInternalTransfersDisabled(d bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disabled = d
}

// newInternalStream builds a stream wired for internal-transfer detection with a
// fast backoff (so retry-then-skip tests don't sleep for seconds).
func newInternalStream(t *testing.T, fc Client, m Metrics, nf config.NativeTransfersConfig) (*Stream, *captureEmitter) {
	t.Helper()
	em := &captureEmitter{}
	nf.Enabled = true
	nf.IncludeInternal = true
	s, err := New(Options{
		Client: fc, Emitter: em, Metrics: m, ChainName: "test", ChainID: 1,
		PollInterval: time.Second, LogChunkBlocks: 2000,
		BackoffBase: time.Millisecond, BackoffMax: time.Millisecond,
		NativeFilter: NativeFilterFromConfig(nf),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, em
}

func tracesOf(txHash string, root *rpc.CallFrame) []rpc.TxTrace {
	return []rpc.TxTrace{{TxHash: txHash, Result: root}}
}

func internalEnvs(em *captureEmitter) []record.Envelope {
	out := []record.Envelope{}
	for _, e := range em.snapshot() {
		if e.Type == record.TypeInternalTransfer {
			out = append(out, e)
		}
	}
	return out
}

func block1(t string) *rpc.Block { return &rpc.Block{Transactions: []rpc.Transaction{{Hash: t}}} }

// TestEmitInternalTransfersWalk covers the callTracer (block-level) walk: root
// skipped, value>0 sub-calls emitted with their trace_address, zero-value skipped,
// DELEGATECALL excluded by type (but its value-moving child still emitted), and a
// reverted frame plus its whole subtree pruned.
func TestEmitInternalTransfersWalk(t *testing.T) {
	root := &rpc.CallFrame{
		Type: "CALL", From: "0xeoa", To: "0xrouter", Value: "0xde0b6b3a7640000", // top-level, skipped
		Calls: []rpc.CallFrame{
			{Type: "CALL", From: "0xrouter", To: "0xbene", Value: "0x1"}, // [0] emit
			{Type: "DELEGATECALL", From: "0xrouter", To: "0xlib", Value: "0x2", // [1] excluded by type
				Calls: []rpc.CallFrame{
					{Type: "CALL", From: "0xrouter", To: "0xgrand", Value: "0x3"}, // [1,0] emit
				}},
			{Type: "CALL", From: "0xrouter", To: "0xzero", Value: "0x0"}, // [2] zero value, skip
			{Type: "CALL", From: "0xrouter", To: "0xrevert", Value: "0x4", Error: "execution reverted", // [3] pruned
				Calls: []rpc.CallFrame{
					{Type: "CALL", From: "0xrevert", To: "0xdeep", Value: "0x5"}, // [3,0] pruned with parent
				}},
		},
	}
	fc := &fakeClient{traceBlock: func(uint64) ([]rpc.TxTrace, error) { return tracesOf("0xtx", root), nil }}
	m := &internalMetrics{}
	s, em := newInternalStream(t, fc, m, config.NativeTransfersConfig{})

	if err := s.emitInternalTransfers(context.Background(), block1("0xtx"), 100, ""); err != nil {
		t.Fatalf("emitInternalTransfers: %v", err)
	}
	got := internalEnvs(em)
	if len(got) != 2 {
		t.Fatalf("emitted %d internal transfers, want 2: %+v", len(got), got)
	}
	byKey := map[string]record.Envelope{}
	for _, e := range got {
		byKey[e.DedupKey()] = e
	}
	if d, ok := byKey["1|0xtx|0"]; !ok || d.Data.(record.InternalTransferData).To != "0xbene" {
		t.Errorf("missing/incorrect transfer at [0]; keys=%v", keysOf(byKey))
	}
	if d, ok := byKey["1|0xtx|1-0"]; !ok || d.Data.(record.InternalTransferData).ValueWei != "3" {
		t.Errorf("missing value-moving child of a DELEGATECALL at [1,0]; keys=%v", keysOf(byKey))
	}
	if s.traceBackend != backendBlock {
		t.Errorf("backend = %v, want block-level", s.traceBackend)
	}
	if m.count != 2 {
		t.Errorf("metric count = %d, want 2", m.count)
	}
}

func keysOf(m map[string]record.Envelope) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestEmitInternalTransfersWholeTxRevert(t *testing.T) {
	root := &rpc.CallFrame{Type: "CALL", From: "0xa", To: "0xb", Value: "0x1", Error: "out of gas",
		Calls: []rpc.CallFrame{{Type: "CALL", From: "0xb", To: "0xc", Value: "0x9"}}}
	fc := &fakeClient{traceBlock: func(uint64) ([]rpc.TxTrace, error) { return tracesOf("0xtx", root), nil }}
	s, em := newInternalStream(t, fc, &internalMetrics{}, config.NativeTransfersConfig{})
	if err := s.emitInternalTransfers(context.Background(), block1("0xtx"), 1, ""); err != nil {
		t.Fatalf("emitInternalTransfers: %v", err)
	}
	if got := internalEnvs(em); len(got) != 0 {
		t.Errorf("a fully-reverted tx should emit nothing, got %d", len(got))
	}
}

func TestEmitInternalTransfersFilter(t *testing.T) {
	root := &rpc.CallFrame{Type: "CALL", From: "0xeoa", To: "0xc", Value: "0x0",
		Calls: []rpc.CallFrame{
			{Type: "CALL", From: "0xc", To: "0xwatched", Value: "0x1"},
			{Type: "CALL", From: "0xc", To: "0xother", Value: "0x2"},
		}}
	fc := &fakeClient{traceBlock: func(uint64) ([]rpc.TxTrace, error) { return tracesOf("0xtx", root), nil }}
	s, em := newInternalStream(t, fc, &internalMetrics{}, config.NativeTransfersConfig{To: []string{"0xwatched"}})
	if err := s.emitInternalTransfers(context.Background(), block1("0xtx"), 1, ""); err != nil {
		t.Fatalf("emitInternalTransfers: %v", err)
	}
	got := internalEnvs(em)
	if len(got) != 1 || got[0].Data.(record.InternalTransferData).To != "0xwatched" {
		t.Fatalf("want 1 allowlisted transfer to 0xwatched, got %+v", got)
	}
}

func TestEmitInternalTransfersCreateAndSelfdestruct(t *testing.T) {
	root := &rpc.CallFrame{Type: "CALL", From: "0xeoa", To: "0xfactory", Value: "0x0",
		Calls: []rpc.CallFrame{
			{Type: "CREATE2", From: "0xfactory", To: "0xnew", Value: "0x1"},
			{Type: "SELFDESTRUCT", From: "0xdying", To: "0xbene", Value: "0x2"},
		}}
	fc := &fakeClient{traceBlock: func(uint64) ([]rpc.TxTrace, error) { return tracesOf("0xtx", root), nil }}
	s, em := newInternalStream(t, fc, &internalMetrics{}, config.NativeTransfersConfig{})
	if err := s.emitInternalTransfers(context.Background(), block1("0xtx"), 1, ""); err != nil {
		t.Fatalf("emitInternalTransfers: %v", err)
	}
	got := internalEnvs(em)
	if len(got) != 2 {
		t.Fatalf("want 2 internal transfers, got %d", len(got))
	}
	create := got[0].Data.(record.InternalTransferData)
	if create.CallType != "create2" || !create.ContractCreation || create.To != "0xnew" {
		t.Errorf("create transfer = %+v, want call_type=create2 contract_creation=true to=0xnew", create)
	}
	sd := got[1].Data.(record.InternalTransferData)
	if sd.CallType != "selfdestruct" || sd.From != "0xdying" || sd.To != "0xbene" {
		t.Errorf("selfdestruct = %+v, want call_type=selfdestruct from=0xdying to=0xbene", sd)
	}
}

// TestEmitInternalTransfersPersistentErrorSkips verifies a persistent non-capability
// trace error skips the block's internal transfers after bounded retries (logged +
// counted) rather than wedging — and never self-disables.
func TestEmitInternalTransfersPersistentErrorSkips(t *testing.T) {
	fc := &fakeClient{traceBlock: func(uint64) ([]rpc.TxTrace, error) { return nil, errors.New("connection reset") }}
	m := &internalMetrics{}
	s, em := newInternalStream(t, fc, m, config.NativeTransfersConfig{})
	if err := s.emitInternalTransfers(context.Background(), block1("0xtx"), 1, ""); err != nil {
		t.Fatalf("a persistent trace error must be skipped, not returned: %v", err)
	}
	if len(internalEnvs(em)) != 0 {
		t.Error("no records on a skipped block")
	}
	if m.skipped != 1 {
		t.Errorf("skipped metric = %d, want 1", m.skipped)
	}
	if m.disabled || s.traceBackend == backendDisabled {
		t.Error("a transient/persistent per-block error must not self-disable")
	}
}

// TestEmitInternalTransfersReselectsOnMidRunCapabilityLoss verifies that a cached
// backend which stops being served mid-run (e.g. a provider downgrade) triggers a
// re-cascade to a sibling backend rather than retry-skipping every block forever.
func TestEmitInternalTransfersReselectsOnMidRunCapabilityLoss(t *testing.T) {
	blockUp := true
	parity := []rpc.ParityTrace{
		{Type: "call", TraceAddress: []int{}, TxHash: "0xtx", Action: rpc.ParityAction{CallType: "call", From: "0xeoa", To: "0xc", Value: "0x0"}},
		{Type: "call", TraceAddress: []int{0}, TxHash: "0xtx", Action: rpc.ParityAction{CallType: "call", From: "0xc", To: "0xbene", Value: "0x1"}},
	}
	fc := &fakeClient{
		traceBlock: func(uint64) ([]rpc.TxTrace, error) {
			if blockUp {
				return tracesOf("0xtx", &rpc.CallFrame{Type: "CALL", From: "0xeoa", To: "0xc", Value: "0x0",
					Calls: []rpc.CallFrame{{Type: "CALL", From: "0xc", To: "0xbene", Value: "0x1"}}}), nil
			}
			return nil, &rpc.HTTPError{Method: rpc.OpDebugTraceBlock, Status: 403} // namespace revoked
		},
		traceParity: func(uint64) ([]rpc.ParityTrace, error) { return parity, nil },
	}
	m := &internalMetrics{}
	s, em := newInternalStream(t, fc, m, config.NativeTransfersConfig{})

	// First block selects block-level.
	if err := s.emitInternalTransfers(context.Background(), block1("0xtx"), 1, ""); err != nil {
		t.Fatalf("block 1: %v", err)
	}
	if s.traceBackend != backendBlock {
		t.Fatalf("backend = %v, want block-level", s.traceBackend)
	}
	// Block-level now 403s mid-run: the next block must re-cascade to parity, not
	// skip, and still emit the internal transfer.
	blockUp = false
	if err := s.emitInternalTransfers(context.Background(), block1("0xtx"), 2, ""); err != nil {
		t.Fatalf("block 2: %v", err)
	}
	if s.traceBackend != backendParity {
		t.Errorf("backend = %v, want re-selected parity", s.traceBackend)
	}
	if got := internalEnvs(em); len(got) != 2 { // one from each block
		t.Errorf("emitted %d, want 2 (block-level then parity)", len(got))
	}
	if m.skipped != 0 {
		t.Errorf("skipped = %d, want 0 (re-cascade, not skip)", m.skipped)
	}
}

// methodRPCServer answers JSON-RPC by method: results[method] is the raw JSON for
// the "result" field; an unlisted method returns -32601 (method not found).
func methodRPCServer(t *testing.T, results map[string]string) *rpc.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     uint64 `json:"id"`
			Method string `json:"method"`
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")
		id := strconv.FormatUint(req.ID, 10)
		if res, ok := results[req.Method]; ok {
			_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":`+id+`,"result":`+res+`}`)
			return
		}
		_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":`+id+`,"error":{"code":-32601,"message":"method `+req.Method+` does not exist/is not available"}}`)
	}))
	t.Cleanup(srv.Close)
	c, err := rpc.New(rpc.Options{URL: srv.URL})
	if err != nil {
		t.Fatalf("rpc.New: %v", err)
	}
	return c
}

// TestEmitInternalTransfersCapabilityCascadeDisables verifies a node serving no
// trace method cascades through all three then self-disables (no fail, metric set).
func TestEmitInternalTransfersCapabilityCascadeDisables(t *testing.T) {
	client := methodRPCServer(t, map[string]string{}) // every method -> -32601
	m := &internalMetrics{}
	s, _ := newInternalStream(t, client, m, config.NativeTransfersConfig{})

	if err := s.emitInternalTransfers(context.Background(), block1("0xtx"), 1, ""); err != nil {
		t.Fatalf("a capability gap must not fail the poll: %v", err)
	}
	if s.traceBackend != backendDisabled {
		t.Errorf("backend = %v, want disabled after the full cascade gapped", s.traceBackend)
	}
	if !m.disabled {
		t.Error("disabled metric should be set")
	}
}

// TestEmitInternalTransfersCascadeSelectsParity verifies that when the node lacks
// debug_traceBlockByNumber but serves trace_block (the anvil/Erigon case), the
// cascade selects parity and emits the internal transfer from the flat trace.
func TestEmitInternalTransfersCascadeSelectsParity(t *testing.T) {
	parity := `[
      {"type":"call","traceAddress":[],"transactionHash":"0xtx","action":{"callType":"call","from":"0xeoa","to":"0xc","value":"0xde0b6b3a7640000"}},
      {"type":"call","traceAddress":[0],"transactionHash":"0xtx","action":{"callType":"call","from":"0xc","to":"0xbene","value":"0x1"}}
    ]`
	client := methodRPCServer(t, map[string]string{rpc.OpTraceBlock: parity}) // only trace_block works
	m := &internalMetrics{}
	s, em := newInternalStream(t, client, m, config.NativeTransfersConfig{})

	if err := s.emitInternalTransfers(context.Background(), block1("0xtx"), 1, ""); err != nil {
		t.Fatalf("emitInternalTransfers: %v", err)
	}
	if s.traceBackend != backendParity {
		t.Fatalf("backend = %v, want parity", s.traceBackend)
	}
	got := internalEnvs(em)
	if len(got) != 1 {
		t.Fatalf("want 1 internal transfer from parity, got %d", len(got))
	}
	d := got[0].Data.(record.InternalTransferData)
	if got[0].DedupKey() != "1|0xtx|0" || d.To != "0xbene" || d.ValueWei != "1" {
		t.Errorf("parity transfer = %s / %+v", got[0].DedupKey(), d)
	}
}

// TestParseParityTraces unit-tests the flat-trace normalizer: root skipped,
// DELEGATECALL excluded, a reverted call's descendant pruned, create/suicide mapped.
func TestParseParityTraces(t *testing.T) {
	traces := []rpc.ParityTrace{
		{Type: "call", TraceAddress: []int{}, TxHash: "0xtx", Action: rpc.ParityAction{CallType: "call", From: "0xeoa", To: "0xc", Value: "0xde0b6b3a7640000"}}, // root, skipped
		{Type: "call", TraceAddress: []int{0}, TxHash: "0xtx", Action: rpc.ParityAction{CallType: "call", From: "0xc", To: "0xbene", Value: "0x1"}},             // emit
		{Type: "call", TraceAddress: []int{1}, TxHash: "0xtx", Action: rpc.ParityAction{CallType: "delegatecall", From: "0xc", To: "0xlib", Value: "0x2"}},      // excluded
		{Type: "call", TraceAddress: []int{2}, TxHash: "0xtx", Error: "Reverted", Action: rpc.ParityAction{CallType: "call", From: "0xc", To: "0xrev", Value: "0x3"}},
		{Type: "call", TraceAddress: []int{2, 0}, TxHash: "0xtx", Action: rpc.ParityAction{CallType: "call", From: "0xrev", To: "0xdeep", Value: "0x4"}}, // pruned (ancestor [2] reverted)
		{Type: "create", TraceAddress: []int{3}, TxHash: "0xtx", Action: rpc.ParityAction{From: "0xc", Value: "0x5"}, Result: &rpc.ParityRes{Address: "0xnew"}},
		{Type: "suicide", TraceAddress: []int{4}, TxHash: "0xtx", Action: rpc.ParityAction{Address: "0xdying", RefundAddress: "0xbene2", Balance: "0x6"}},
	}
	got, err := parseParityTraces(traces)
	if err != nil {
		t.Fatalf("parseParityTraces: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 internal transfers (call, create, suicide), got %d: %+v", len(got), got)
	}
	if got[0].to != "0xbene" || got[0].valueWei.String() != "1" || got[0].callType != "call" {
		t.Errorf("call = %+v", got[0])
	}
	if got[1].callType != "create" || !got[1].contractCreation || got[1].to != "0xnew" {
		t.Errorf("create = %+v", got[1])
	}
	if got[2].callType != "selfdestruct" || got[2].from != "0xdying" || got[2].to != "0xbene2" || got[2].valueWei.String() != "6" {
		t.Errorf("suicide = %+v", got[2])
	}
}
