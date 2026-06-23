//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/chain"
	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
	"github.com/daxchain-io/evm-tools/internal/stream"
)

type captureEmitter struct {
	mu   sync.Mutex
	recs []record.Envelope
}

func (c *captureEmitter) Emit(env record.Envelope) error {
	c.mu.Lock()
	c.recs = append(c.recs, env)
	c.mu.Unlock()
	return nil
}

func (c *captureEmitter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.recs)
}

func (c *captureEmitter) has(pred func(record.Envelope) bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.recs {
		if pred(e) {
			return true
		}
	}
	return false
}

// rpcCall makes a raw JSON-RPC POST to the dev chain (anvil unlocks its dev
// accounts, so eth_sendTransaction needs no signing).
func rpcCall(t *testing.T, url, method string, params ...any) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	resp, err := http.Post(url, "application/json", bytes.NewReader(body)) //nolint:noctx // test helper
	if err != nil {
		t.Fatalf("rpc %s: %v", method, err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out struct {
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode %s: %v", method, err)
	}
	if out.Error != nil {
		t.Fatalf("rpc %s error: %s", method, out.Error.Message)
	}
}

// TestProducerNativeTransferE2E is the producer→record end-to-end: it runs the
// evm-stream core against a live dev chain (anvil), sends a value tx, and asserts
// a native_transfer record is emitted.
func TestProducerNativeTransferE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	url := envOr("EVM_TEST_RPC_URL", "http://localhost:8545")

	cl, err := rpc.New(rpc.Options{URL: url})
	if err != nil {
		t.Fatalf("rpc.New: %v", err)
	}
	info, err := chain.Resolve(ctx, cl, "anvil")
	if err != nil {
		t.Fatalf("chain.Resolve: %v", err)
	}

	em := &captureEmitter{}
	s, err := stream.New(stream.Options{
		Client:         cl,
		Emitter:        em,
		ChainName:      info.Name,
		ChainID:        info.ID,
		NativeFilter:   stream.NativeFilterFromConfig(config.NativeTransfersConfig{Enabled: true}),
		PollInterval:   500 * time.Millisecond,
		FromBlock:      "latest", // start at head+1; the tx below lands in a new block
		LogChunkBlocks: 2000,
	})
	if err != nil {
		t.Fatalf("stream.New: %v", err)
	}
	go func() { _ = s.Run(ctx) }()

	// Let the stream resolve head+1 and begin polling, then send a value tx
	// between anvil's prefunded accounts (account[0] -> account[1], 1 ETH).
	time.Sleep(1500 * time.Millisecond)
	rpcCall(t, url, "eth_sendTransaction", map[string]any{
		"from":  "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
		"to":    "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
		"value": "0xde0b6b3a7640000",
	})

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if em.has(func(e record.Envelope) bool { return e.Type == record.TypeNativeTransfer }) {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("no native_transfer emitted after a value tx (captured %d records)", em.count())
}
