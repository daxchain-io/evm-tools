package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// runWithCtx executes a root command with a context, capturing stdout/stderr.
func runWithCtx(ctx context.Context, t *testing.T, tool Tool, args ...string) (string, error) {
	t.Helper()
	root := NewRootCommand(tool)
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(args)
	if ctx != nil {
		root.SetContext(ctx)
	}
	err := root.Execute()
	return out.String(), err
}

// ethRPCServer is a minimal HTTP JSON-RPC server answering chainId/blockNumber.
func ethRPCServer(t *testing.T) *httptest.Server {
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
			result = "0x1092" // 4242
		case "eth_blockNumber":
			result = "0x10"
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":"` + result + `"}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func readAll(r *http.Request) ([]byte, error) {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(r.Body)
	return buf.Bytes(), err
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func writeStreamConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "evm-tools.toml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestStreamCheckRPCSuccess verifies check rpc against a reachable HTTP node
// prints a JSON status and exits 0.
func TestStreamCheckRPCSuccess(t *testing.T) {
	srv := ethRPCServer(t)
	cfg := writeStreamConfig(t, "chain = \"my-chain\"\n[rpc]\nurl = \""+srv.URL+"\"\n")
	out, err := runWithCtx(context.Background(), t, ToolStream, "check", "rpc", "--config", cfg)
	if err != nil {
		t.Fatalf("check rpc: %v\n%s", err, out)
	}
	var res struct {
		OK          bool   `json:"ok"`
		ChainID     int64  `json:"chain_id"`
		BlockNumber uint64 `json:"block_number"`
		Endpoint    string `json:"endpoint"`
	}
	// The JSON status is the last line of stdout.
	if err := json.Unmarshal([]byte(lastJSONLine(out)), &res); err != nil {
		t.Fatalf("status not JSON: %v\n%s", err, out)
	}
	if !res.OK || res.ChainID != 4242 || res.BlockNumber != 16 {
		t.Errorf("unexpected status: %+v", res)
	}
}

// TestStreamCheckRPCUnreachable verifies a bad endpoint exits non-zero with a
// redacted endpoint and never echoes a token.
func TestStreamCheckRPCUnreachable(t *testing.T) {
	cfg := writeStreamConfig(t, "chain = \"c\"\n[rpc]\nurl = \"http://127.0.0.1:1?token=supersecret\"\n")
	out, err := runWithCtx(context.Background(), t, ToolStream, "check", "rpc", "--config", cfg)
	if err == nil {
		t.Fatalf("expected non-zero exit for unreachable endpoint\n%s", out)
	}
	if strings.Contains(out, "supersecret") {
		t.Errorf("output leaked token:\n%s", out)
	}
}

// TestStreamValidateGood verifies validate passes for a well-formed config with
// built-in ERC-20 events (no network).
func TestStreamValidateGood(t *testing.T) {
	cfg := writeStreamConfig(t, `
chain = "my-chain"
[rpc]
url = "http://localhost:8545"
[stream]
poll_interval = "2s"
log_chunk_blocks = 1000
[[stream.contracts]]
name = "usdc"
address = "0xabc"
events = ["Transfer", "Approval"]
`)
	out, err := runWithCtx(context.Background(), t, ToolStream, "validate", "--config", cfg)
	if err != nil {
		t.Fatalf("validate: %v\n%s", err, out)
	}
	if !strings.Contains(out, "ok:") {
		t.Errorf("expected ok message, got:\n%s", out)
	}
}

// TestStreamValidateBadABI verifies validate fails when an event resolves to no
// signature.
func TestStreamValidateBadABI(t *testing.T) {
	cfg := writeStreamConfig(t, `
chain = "c"
[rpc]
url = "http://localhost:8545"
[stream]
poll_interval = "2s"
log_chunk_blocks = 1000
[[stream.contracts]]
name = "x"
address = "0x1"
events = ["Frobnicate"]
`)
	out, err := runWithCtx(context.Background(), t, ToolStream, "validate", "--config", cfg)
	if err == nil {
		t.Fatalf("expected ABI resolution error\n%s", out)
	}
	if !strings.Contains(err.Error(), "no known signature") {
		t.Errorf("error should name the unresolved event: %v", err)
	}
}

// TestStreamValidateNothingToMonitor verifies a config with no contracts and no
// native transfers fails validation.
func TestStreamValidateNothingToMonitor(t *testing.T) {
	cfg := writeStreamConfig(t, `
chain = "c"
[rpc]
url = "http://localhost:8545"
[stream]
poll_interval = "2s"
log_chunk_blocks = 1000
`)
	_, err := runWithCtx(context.Background(), t, ToolStream, "validate", "--config", cfg)
	if err == nil || !strings.Contains(err.Error(), "nothing to monitor") {
		t.Fatalf("expected nothing-to-monitor error, got %v", err)
	}
}

// TestStreamValidateMissingRPCURL verifies a missing rpc.url is fatal.
func TestStreamValidateMissingRPCURL(t *testing.T) {
	cfg := writeStreamConfig(t, `
chain = "c"
[stream]
poll_interval = "2s"
log_chunk_blocks = 1000
[[stream.contracts]]
name = "x"
address = "0x1"
events = ["Transfer"]
`)
	_, err := runWithCtx(context.Background(), t, ToolStream, "validate", "--config", cfg)
	if err == nil || !strings.Contains(err.Error(), "rpc.url is required") {
		t.Fatalf("expected rpc.url error, got %v", err)
	}
}

func lastJSONLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		l := strings.TrimSpace(lines[i])
		if strings.HasPrefix(l, "{") {
			return l
		}
	}
	return ""
}
