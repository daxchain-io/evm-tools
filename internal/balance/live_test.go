//go:build livenode

// Live-node tests run against a real EVM node (anvil or `geth --dev`) and are
// excluded from the default `go test ./...` so the offline suite stays green.
// Run with: go test -tags livenode ./internal/balance/...
//
// Point EVM_TOOLS_TEST_RPC_URL at a reachable node (default
// http://127.0.0.1:8545). For an mTLS endpoint, also set the cert env vars.
// EVM_TOOLS_TEST_ACCOUNT optionally overrides the polled native account; it
// defaults to anvil's first dev account.
package balance

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/chain"
	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
)

func liveClient(t *testing.T) *rpc.Client {
	t.Helper()
	url := os.Getenv("EVM_TOOLS_TEST_RPC_URL")
	if url == "" {
		url = "http://127.0.0.1:8545"
	}
	c, err := rpc.New(rpc.Options{
		URL: url,
		TLS: rpc.TLSConfig{
			ClientCert: os.Getenv("EVM_TOOLS_TEST_CLIENT_CERT"),
			ClientKey:  os.Getenv("EVM_TOOLS_TEST_CLIENT_KEY"),
			CACert:     os.Getenv("EVM_TOOLS_TEST_CA_CERT"),
			ServerName: os.Getenv("EVM_TOOLS_TEST_SERVER_NAME"),
		},
		Timeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("rpc.New: %v", err)
	}
	return c
}

// TestLiveNativeSample polls a real account's native balance once and asserts a
// well-formed balance_sample record is emitted, exercising eth_getBalance and
// the full record path end to end.
func TestLiveNativeSample(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	c := liveClient(t)
	info, err := chain.Resolve(ctx, c, "live-test")
	if err != nil {
		t.Fatalf("resolve chain id: %v", err)
	}

	account := os.Getenv("EVM_TOOLS_TEST_ACCOUNT")
	if account == "" {
		// anvil/hardhat first dev account.
		account = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
	}

	em := &captureEmitter{}
	p, err := New(Options{
		Client:    c,
		Emitter:   em,
		ChainName: info.Name,
		ChainID:   info.ID,
		Cadence:   Cadence{Interval: 50 * time.Millisecond},
		Native:    []NativeTarget{{Name: "dev-account", Address: account}},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- p.Run(runCtx) }()
	waitFor(t, func() bool { return len(em.byType(record.TypeBalanceSample)) >= 1 }, 15*time.Second)
	runCancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	s := em.byType(record.TypeBalanceSample)[0].Data.(record.BalanceData)
	if s.Kind != record.KindNative || s.BalanceWei == "" {
		t.Fatalf("malformed live native sample: %+v", s)
	}
	t.Logf("live native balance of %s = %s wei", account, s.BalanceWei)
}
