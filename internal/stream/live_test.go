//go:build livenode

// Live-node tests run against a real EVM node (anvil or `geth --dev`) and are
// excluded from the default `go test ./...` so the offline suite stays green.
// Run with: go test -tags livenode ./internal/stream/...
//
// Point EVM_TOOLS_TEST_RPC_URL at a reachable node (default
// http://127.0.0.1:8545). For an mTLS endpoint, also set the cert env vars.
package stream

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/chain"
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

// TestLiveChainIDAndHead resolves the chain ID and reads the head against a real
// node, exercising the full mTLS/HTTP transport end to end.
func TestLiveChainIDAndHead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c := liveClient(t)

	info, err := chain.Resolve(ctx, c, "live-test")
	if err != nil {
		t.Fatalf("resolve chain id: %v", err)
	}
	if info.ID <= 0 {
		t.Fatalf("unexpected chain id %d", info.ID)
	}
	head, err := chain.Head(ctx, c)
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	t.Logf("live node: chain_id=%d head=%d", info.ID, head)
}
