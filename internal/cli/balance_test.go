package cli

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// balanceRPCServer is a minimal HTTP JSON-RPC server answering the methods
// evm-balance uses: chainId, blockNumber, getBalance, getBlockByNumber, call,
// and getLogs. It lets the run path drive a full sample end to end with no node.
func balanceRPCServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     uint64 `json:"id"`
			Method string `json:"method"`
		}
		body, _ := readAll(r)
		_ = json.Unmarshal(body, &req)

		var result string
		switch req.Method {
		case "eth_chainId":
			result = `"0x1092"` // 4242
		case "eth_blockNumber":
			result = `"0x64"` // 100
		case "eth_getBalance":
			result = `"0xde0b6b3a7640000"` // 1 ETH
		case "eth_getBlockByNumber":
			result = `{"number":"0x64","hash":"0xblk","timestamp":"0x65503700"}`
		case "eth_call":
			// Return 6 (decimals) / a balance; the same word works for both since
			// the test asserts on records, not exact totals.
			result = `"0x0000000000000000000000000000000000000000000000000000000000000006"`
		case "eth_getLogs":
			result = `[]`
		default:
			result = `null`
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":` + result + `}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestBalanceCheckRPCSuccess verifies check rpc against a reachable node prints a
// JSON status and exits 0.
func TestBalanceCheckRPCSuccess(t *testing.T) {
	srv := balanceRPCServer(t)
	cfg := writeStreamConfig(t, "chain = \"codex-chain\"\n[rpc]\nurl = \""+srv.URL+"\"\n")
	out, err := runWithCtx(context.Background(), t, ToolBalance, "check", "rpc", "--config", cfg)
	if err != nil {
		t.Fatalf("check rpc: %v\n%s", err, out)
	}
	var res struct {
		OK      bool  `json:"ok"`
		ChainID int64 `json:"chain_id"`
	}
	if err := json.Unmarshal([]byte(lastJSONLine(out)), &res); err != nil {
		t.Fatalf("status not JSON: %v\n%s", err, out)
	}
	if !res.OK || res.ChainID != 4242 {
		t.Errorf("unexpected status: %+v", res)
	}
}

// TestBalanceValidateGood verifies validate passes for a well-formed [balance]
// config (no network).
func TestBalanceValidateGood(t *testing.T) {
	cfg := writeStreamConfig(t, `
chain = "codex-chain"
[rpc]
url = "http://localhost:8545"
[balance]
interval = "1m"
[[balance.native]]
name = "treasury"
address = "0xabc"
[[balance.erc20]]
name = "treasury-usdc"
token = "0xtok"
address = "0xabc"
decimals = 6
[[balance.contracts]]
name = "usdc"
address = "0xtok"
token_supply = true
transfer_count_window_blocks = 1000
`)
	out, err := runWithCtx(context.Background(), t, ToolBalance, "validate", "--config", cfg)
	if err != nil {
		t.Fatalf("validate: %v\n%s", err, out)
	}
	if !strings.Contains(out, "ok:") {
		t.Errorf("expected ok message, got:\n%s", out)
	}
}

// TestBalanceValidateBadCadence verifies setting both interval and every_blocks
// fails validation.
func TestBalanceValidateBadCadence(t *testing.T) {
	cfg := writeStreamConfig(t, `
chain = "c"
[rpc]
url = "http://localhost:8545"
[balance]
interval = "1m"
every_blocks = 50
[[balance.native]]
name = "n"
address = "0x1"
`)
	_, err := runWithCtx(context.Background(), t, ToolBalance, "validate", "--config", cfg)
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("expected cadence XOR error, got %v", err)
	}
}

// TestBalanceValidateNothingToPoll verifies a config with no targets fails.
func TestBalanceValidateNothingToPoll(t *testing.T) {
	cfg := writeStreamConfig(t, `
chain = "c"
[rpc]
url = "http://localhost:8545"
[balance]
interval = "1m"
`)
	_, err := runWithCtx(context.Background(), t, ToolBalance, "validate", "--config", cfg)
	if err == nil || !strings.Contains(err.Error(), "nothing to poll") {
		t.Fatalf("expected nothing-to-poll error, got %v", err)
	}
}

// TestBalanceUnknownKeyRejected verifies a typo in the tool's own section is a
// fatal strict-decode error.
func TestBalanceUnknownKeyRejected(t *testing.T) {
	cfg := writeStreamConfig(t, `
chain = "c"
[rpc]
url = "http://localhost:8545"
[balance]
intervial = "1m"
`)
	_, err := runWithCtx(context.Background(), t, ToolBalance, "validate", "--config", cfg)
	if err == nil {
		t.Fatal("expected strict-decode error for unknown key")
	}
}

// TestBalanceRunEmitsSamples drives the full run path against an httptest node
// and verifies it emits balance_sample / contract_sample JSONL on stdout, then
// shuts down cleanly on context cancel.
func TestBalanceRunEmitsSamples(t *testing.T) {
	srv := balanceRPCServer(t)
	cfg := writeStreamConfig(t, `
chain = "codex-chain"
[rpc]
url = "`+srv.URL+`"
[balance]
interval = "5ms"
[[balance.native]]
name = "treasury"
address = "0xabc"
[[balance.contracts]]
name = "usdc"
address = "0xtok"
token_supply = true
decimals = 6
`)

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		out string
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		// Bind the health/metrics server to an ephemeral port so parallel tests
		// never collide on the default :9001.
		out, err := runWithCtx(ctx, t, ToolBalance, "run", "--config", cfg, "--metrics-addr", ":0")
		resCh <- result{out, err}
	}()

	// Let a few 5ms ticks emit, then signal a clean shutdown.
	time.Sleep(150 * time.Millisecond)
	cancel()

	var got result
	select {
	case got = <-resCh:
	case <-time.After(3 * time.Second):
		t.Fatal("run did not shut down within 3s of context cancel")
	}
	if got.err != nil {
		t.Fatalf("run returned error: %v\n%s", got.err, got.out)
	}

	if !strings.Contains(got.out, `"type":"balance_sample"`) {
		t.Errorf("expected a balance_sample record in stdout:\n%s", got.out)
	}
	if !strings.Contains(got.out, `"type":"contract_sample"`) {
		t.Errorf("expected a contract_sample record in stdout:\n%s", got.out)
	}
	// Records must carry the balance tool name and the chain envelope fields.
	if !strings.Contains(got.out, `"tool":"evm-balance"`) {
		t.Errorf("records missing tool name:\n%s", got.out)
	}
}
