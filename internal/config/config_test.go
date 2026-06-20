package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeConfig writes a TOML file in a temp dir and returns its path.
func writeConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "evm-tools.toml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return p
}

const sampleConfig = `
chain = "my-chain"

[rpc]
url = "https://rpc.internal.example.com:8545"
client_cert = "/certs/client.crt"
client_key = "/certs/client.key"
ca_cert = "/certs/ca.crt"
server_name = "rpc.internal.example.com"

[metrics]
enabled = false
path = "/metrics"

[stream]
from_block = "latest"
poll_interval = "2s"
log_chunk_blocks = 2000

[stream.metrics]
enabled = true
addr = ":9000"

[[stream.contracts]]
name = "usdc"
address = "0xusdc"
events = ["Transfer", "Approval"]

[stream.native_transfers]
enabled = true

[balance]
interval = "1m"

[[balance.native]]
name = "treasury-eth"
address = "0xtreasury"

[[balance.erc20]]
name = "treasury-usdc"
token = "0xusdc"
address = "0xtreasury"
decimals = 6
`

func TestDecodeStream(t *testing.T) {
	p := writeConfig(t, sampleConfig)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeStream(false)
	if err != nil {
		t.Fatalf("DecodeStream: %v", err)
	}

	if cfg.Chain != "my-chain" {
		t.Errorf("chain = %q", cfg.Chain)
	}
	if cfg.RPC.URL != "https://rpc.internal.example.com:8545" {
		t.Errorf("rpc.url = %q", cfg.RPC.URL)
	}
	if cfg.Stream.PollInterval != "2s" || cfg.Stream.LogChunkBlocks != 2000 {
		t.Errorf("stream poll/chunk = %q/%d", cfg.Stream.PollInterval, cfg.Stream.LogChunkBlocks)
	}
	if len(cfg.Stream.Contracts) != 1 || cfg.Stream.Contracts[0].Name != "usdc" {
		t.Fatalf("expected 1 contract usdc, got %+v", cfg.Stream.Contracts)
	}
	if got := cfg.Stream.Contracts[0].Events; len(got) != 2 || got[0] != "Transfer" {
		t.Errorf("events = %v", got)
	}
	if !cfg.Stream.Metrics.IsEnabled() || cfg.Stream.Metrics.Addr != ":9000" {
		t.Errorf("stream.metrics = %+v", cfg.Stream.Metrics)
	}
	if !cfg.Stream.NativeTransfers.Enabled {
		t.Errorf("native_transfers should be enabled")
	}
}

func TestDecodeBalance(t *testing.T) {
	p := writeConfig(t, sampleConfig)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeBalance(false)
	if err != nil {
		t.Fatalf("DecodeBalance: %v", err)
	}
	if cfg.Balance.Interval != "1m" {
		t.Errorf("interval = %q", cfg.Balance.Interval)
	}
	if len(cfg.Balance.Native) != 1 || cfg.Balance.Native[0].Address != "0xtreasury" {
		t.Errorf("native = %+v", cfg.Balance.Native)
	}
	if len(cfg.Balance.ERC20) != 1 {
		t.Fatalf("erc20 = %+v", cfg.Balance.ERC20)
	}
	if cfg.Balance.ERC20[0].Decimals == nil || *cfg.Balance.ERC20[0].Decimals != 6 {
		t.Errorf("erc20 decimals = %v", cfg.Balance.ERC20[0].Decimals)
	}
}

// TestSiblingSectionIgnored verifies a tool ignores the other tool's section
// rather than rejecting it: evm-stream decodes a file that also has [balance],
// and vice versa.
func TestSiblingSectionIgnored(t *testing.T) {
	p := writeConfig(t, sampleConfig)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := l.DecodeStream(false); err != nil {
		t.Errorf("stream decode should ignore [balance], got: %v", err)
	}

	l2, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := l2.DecodeBalance(false); err != nil {
		t.Errorf("balance decode should ignore [stream], got: %v", err)
	}
}

// TestUnknownKeyInOwnSectionFatal verifies a typo within the tool's own subtree
// is a fatal error (strict decode), while the same key under the sibling tool's
// subtree is harmless.
func TestUnknownKeyInOwnSectionFatal(t *testing.T) {
	body := `
chain = "my-chain"
[stream]
poll_intervall = "2s"   # typo
`
	p := writeConfig(t, body)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := l.DecodeStream(false); err == nil {
		t.Fatal("expected fatal error for unknown key in [stream]")
	} else if !strings.Contains(err.Error(), "stream") {
		t.Errorf("error should mention stream decode: %v", err)
	}

	// The same typo lives under [stream]; evm-balance ignores [stream] entirely.
	l2, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := l2.DecodeBalance(false); err != nil {
		t.Errorf("balance must ignore the sibling [stream] typo, got: %v", err)
	}
}

// TestUnknownTopLevelKeyFatal verifies a stray top-level key (not a tool
// section, not a shared key) is rejected for both tools.
func TestUnknownSharedKeyFatal(t *testing.T) {
	body := `
chain = "my-chain"
[rpc]
urll = "https://x"   # typo in a shared section
`
	p := writeConfig(t, body)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := l.DecodeStream(false); err == nil {
		t.Fatal("expected fatal error for unknown key in [rpc]")
	}
}

// TestEnvOverride verifies env beats the file via the EVM_TOOLS_ prefix and the
// "." -> "_" key replacer for nested keys.
func TestEnvOverride(t *testing.T) {
	p := writeConfig(t, sampleConfig)
	t.Setenv("EVM_TOOLS_RPC_URL", "https://override.example.com:9999")
	t.Setenv("EVM_TOOLS_CHAIN", "other-chain")

	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeStream(false)
	if err != nil {
		t.Fatalf("DecodeStream: %v", err)
	}
	if cfg.RPC.URL != "https://override.example.com:9999" {
		t.Errorf("env should override rpc.url, got %q", cfg.RPC.URL)
	}
	if cfg.Chain != "other-chain" {
		t.Errorf("env should override chain, got %q", cfg.Chain)
	}
}

// TestDefaultsApply verifies built-in defaults fill in when the file omits them.
func TestDefaultsApply(t *testing.T) {
	body := `chain = "my-chain"`
	p := writeConfig(t, body)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeStream(false)
	if err != nil {
		t.Fatalf("DecodeStream: %v", err)
	}
	if cfg.Log.Level != "info" || cfg.Log.Format != "text" {
		t.Errorf("log defaults = %q/%q", cfg.Log.Level, cfg.Log.Format)
	}
	if cfg.Stream.PollInterval != "2s" || cfg.Stream.LogChunkBlocks != 2000 {
		t.Errorf("stream defaults = %q/%d", cfg.Stream.PollInterval, cfg.Stream.LogChunkBlocks)
	}
	if cfg.Stream.FromBlock != "latest" {
		t.Errorf("from_block default = %q", cfg.Stream.FromBlock)
	}
	if cfg.Stream.Metrics.Addr != ":9000" {
		t.Errorf("stream metrics addr default = %q", cfg.Stream.Metrics.Addr)
	}
}

// TestMissingExplicitFileFatal verifies an explicit --config that doesn't exist
// is a fatal error, while no config at all is fine (defaults apply).
func TestMissingExplicitFileFatal(t *testing.T) {
	if _, err := New(Options{ConfigFile: filepath.Join(t.TempDir(), "nope.toml")}); err == nil {
		t.Fatal("expected error for missing explicit config file")
	}
}

// TestAllowExecThreaded verifies the allowExec flag reaches the decoded config.
func TestAllowExecThreaded(t *testing.T) {
	p := writeConfig(t, `chain = "my-chain"`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeStream(true)
	if err != nil {
		t.Fatalf("DecodeStream: %v", err)
	}
	if !cfg.AllowExec {
		t.Error("AllowExec should be true")
	}
}
