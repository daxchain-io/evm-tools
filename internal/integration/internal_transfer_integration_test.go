//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/chain"
	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
	"github.com/daxchain-io/evm-tools/internal/stream"
)

// forwarderAddr is where we install a tiny ETH-forwarder contract via anvil_setCode.
const forwarderAddr = "0x00000000000000000000000000000000c0ffee01"

// beneficiary is anvil's account[1] — the forwarder's hardcoded CALL target, and
// so the recipient of the internal transfer.
const beneficiary = "0x70997970c51812dc3a010c7d01b50e0d17dc79c8"

// forwarderCode forwards msg.value to beneficiary via CALL:
// PUSH1 0 x4; CALLVALUE; PUSH20 <beneficiary>; GAS; CALL; STOP.
const forwarderCode = "0x6000600060006000347370997970c51812dc3a010c7d01b50e0d17dc79c85af100"

// TestProducerInternalTransferE2E proves internal-transfer detection end to end
// against live anvil. anvil does not serve debug_traceBlockByNumber, so the trace
// backend cascade selects the parity trace_block tracer. We install a forwarder
// contract, send it 1 ETH, and assert an internal_transfer record is emitted for
// the forwarder→beneficiary leg (which carries no log and is invisible to the
// top-level native path).
func TestProducerInternalTransferE2E(t *testing.T) {
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

	// Install the forwarder contract's runtime code (no transaction).
	rpcCall(t, url, "anvil_setCode", forwarderAddr, forwarderCode)

	em := &captureEmitter{}
	s, err := stream.New(stream.Options{
		Client:    cl,
		Emitter:   em,
		ChainName: info.Name,
		ChainID:   info.ID,
		NativeFilter: stream.NativeFilterFromConfig(config.NativeTransfersConfig{
			Enabled: true, IncludeInternal: true,
		}),
		PollInterval:   500 * time.Millisecond,
		FromBlock:      "latest",
		LogChunkBlocks: 2000,
	})
	if err != nil {
		t.Fatalf("stream.New: %v", err)
	}
	go func() { _ = s.Run(ctx) }()

	// Let the stream begin polling, then send 1 ETH to the forwarder; it CALLs the
	// beneficiary with the value — an internal transfer.
	time.Sleep(1500 * time.Millisecond)
	rpcCall(t, url, "eth_sendTransaction", map[string]any{
		"from":  "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
		"to":    forwarderAddr,
		"value": "0xde0b6b3a7640000",
	})

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if em.has(func(e record.Envelope) bool {
			if e.Type != record.TypeInternalTransfer {
				return false
			}
			d, ok := e.Data.(record.InternalTransferData)
			return ok && strings.EqualFold(d.To, beneficiary)
		}) {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("no internal_transfer to the beneficiary emitted (captured %d records)", em.count())
}
