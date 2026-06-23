package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/pflag"
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
include_internal = true

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
	if !cfg.Stream.NativeTransfers.IncludeInternal {
		t.Errorf("native_transfers.include_internal should decode true")
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

// discoverChain points HOME at a hermetic temp dir, writes the named files under
// ~/.evm-tools/ (name -> chain value), runs default discovery (no --config), and
// returns the decoded chain (or "" when no file was discovered).
func discoverChain(t *testing.T, files map[string]string) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)        // os.UserHomeDir on Linux
	t.Setenv("USERPROFILE", home) // os.UserHomeDir on Windows
	// Keep the XDG/OS user-config dir out of the way so ~/.evm-tools is
	// unambiguously the directory that wins on every platform.
	empty := filepath.Join(home, "user-config-empty")
	t.Setenv("XDG_CONFIG_HOME", empty) // os.UserConfigDir on Linux
	t.Setenv("APPDATA", empty)         // os.UserConfigDir on Windows

	dir := filepath.Join(home, ".evm-tools")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	for name, chain := range files {
		body := "chain = \"" + chain + "\"\n[rpc]\nurl = \"https://rpc.example\"\n"
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	l, err := New(Options{}) // no ConfigFile -> default search
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeStream(false)
	if err != nil {
		t.Fatalf("DecodeStream: %v", err)
	}
	return cfg.Chain
}

// TestDefaultSearchDiscoversConfigToml verifies config.toml is the primary
// discovered name in ~/.evm-tools when --config is not given.
func TestDefaultSearchDiscoversConfigToml(t *testing.T) {
	if got := discoverChain(t, map[string]string{"config.toml": "via-config"}); got != "via-config" {
		t.Errorf("expected discovery of ~/.evm-tools/config.toml, got chain=%q", got)
	}
}

// TestDefaultSearchFallbackEvmToolsToml verifies the legacy evm-tools.toml name is
// still discovered as a backward-compatible fallback.
func TestDefaultSearchFallbackEvmToolsToml(t *testing.T) {
	if got := discoverChain(t, map[string]string{"evm-tools.toml": "via-legacy"}); got != "via-legacy" {
		t.Errorf("expected fallback discovery of ~/.evm-tools/evm-tools.toml, got chain=%q", got)
	}
}

// TestDefaultSearchConfigTomlWins verifies config.toml takes precedence over the
// legacy evm-tools.toml when both are present in the same directory.
func TestDefaultSearchConfigTomlWins(t *testing.T) {
	got := discoverChain(t, map[string]string{
		"config.toml":    "primary",
		"evm-tools.toml": "legacy",
	})
	if got != "primary" {
		t.Errorf("config.toml should win over evm-tools.toml, got chain=%q", got)
	}
}

// TestStreamScalarFlagsBindAndPreserve verifies the evm-stream --from-block /
// --poll-interval bindings at the precedence layer: a flag the user *set* must
// override the config file, and a flag left *unset* must not clobber the file
// value (the binding fires only for changed flags). It decodes and compares the
// resolved values rather than relying on validate succeeding, which would pass
// under both correct and clobbered behavior.
func TestStreamScalarFlagsBindAndPreserve(t *testing.T) {
	const body = `
[rpc]
url = "https://x"

[stream]
from_block = "12345"
poll_interval = "7s"

[[stream.contracts]]
name = "t"
address = "0xtoken"
events = ["Transfer"]
`
	// streamFlags mirrors the pflag names bound in internal/cli (bindStreamFlags)
	// to the stream.from_block / stream.poll_interval keys via flagBindings.
	streamFlags := func() *pflag.FlagSet {
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		fs.String("from-block", "", "")
		fs.String("poll-interval", "", "")
		return fs
	}

	t.Run("unset flags preserve the file values", func(t *testing.T) {
		l, err := New(Options{ConfigFile: writeConfig(t, body), Flags: streamFlags()})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		cfg, err := l.DecodeStream(false)
		if err != nil {
			t.Fatalf("DecodeStream: %v", err)
		}
		if cfg.Stream.FromBlock != "12345" || cfg.Stream.PollInterval != "7s" {
			t.Errorf("unset flags clobbered the file: from_block=%q poll_interval=%q (want 12345/7s)",
				cfg.Stream.FromBlock, cfg.Stream.PollInterval)
		}
	})

	t.Run("set flags override the file values", func(t *testing.T) {
		fs := streamFlags()
		if err := fs.Set("from-block", "999"); err != nil {
			t.Fatal(err)
		}
		if err := fs.Set("poll-interval", "30s"); err != nil {
			t.Fatal(err)
		}
		l, err := New(Options{ConfigFile: writeConfig(t, body), Flags: fs})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		cfg, err := l.DecodeStream(false)
		if err != nil {
			t.Fatalf("DecodeStream: %v", err)
		}
		if cfg.Stream.FromBlock != "999" || cfg.Stream.PollInterval != "30s" {
			t.Errorf("set flags did not override the file: from_block=%q poll_interval=%q (want 999/30s)",
				cfg.Stream.FromBlock, cfg.Stream.PollInterval)
		}
	})
}

// TestSharedOutputInputDecode verifies the top-level output/input transport keys
// decode (squashed onto every tool's config) and that AutomaticEnv supplies them.
func TestSharedOutputInputDecode(t *testing.T) {
	const body = `
output = "unix:/run/evm-out.sock"
input = "unix:/run/evm-in.sock"
`
	// A producer reads the output key.
	l, err := New(Options{ConfigFile: writeConfig(t, body)})
	if err != nil {
		t.Fatal(err)
	}
	sc, err := l.DecodeStream(false)
	if err != nil {
		t.Fatalf("DecodeStream: %v", err)
	}
	if sc.Output != "unix:/run/evm-out.sock" {
		t.Errorf("stream output = %q, want unix:/run/evm-out.sock", sc.Output)
	}

	// A sink reads the input key from the same shared surface.
	l2, err := New(Options{ConfigFile: writeConfig(t, body)})
	if err != nil {
		t.Fatal(err)
	}
	kc, err := l2.DecodeKafka(false)
	if err != nil {
		t.Fatalf("DecodeKafka: %v", err)
	}
	if kc.Input != "unix:/run/evm-in.sock" {
		t.Errorf("kafka input = %q, want unix:/run/evm-in.sock", kc.Input)
	}

	// AutomaticEnv supplies the top-level output key (EVM_TOOLS_OUTPUT) with no
	// config file present.
	t.Setenv("EVM_TOOLS_OUTPUT", "unix:/env-out.sock")
	lEnv, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	scEnv, err := lEnv.DecodeStream(false)
	if err != nil {
		t.Fatalf("DecodeStream(env): %v", err)
	}
	if scEnv.Output != "unix:/env-out.sock" {
		t.Errorf("stream output from env = %q, want unix:/env-out.sock", scEnv.Output)
	}
}

// TestSharedDeadLetterFileDecode verifies the top-level dead_letter_file key
// decodes onto a sink's shared surface (so strict decode accepts it) and that
// AutomaticEnv supplies it via EVM_TOOLS_DEAD_LETTER_FILE.
func TestSharedDeadLetterFileDecode(t *testing.T) {
	const body = `
dead_letter_file = "/var/lib/evm-tools/dead-letter.jsonl"

[kafka]
brokers = ["localhost:9092"]
topic = "t"
`
	l, err := New(Options{ConfigFile: writeConfig(t, body)})
	if err != nil {
		t.Fatal(err)
	}
	kc, err := l.DecodeKafka(false)
	if err != nil {
		t.Fatalf("DecodeKafka: %v", err)
	}
	if kc.DeadLetterFile != "/var/lib/evm-tools/dead-letter.jsonl" {
		t.Errorf("kafka dead_letter_file = %q, want /var/lib/evm-tools/dead-letter.jsonl", kc.DeadLetterFile)
	}

	t.Setenv("EVM_TOOLS_DEAD_LETTER_FILE", "/env/dead-letter.jsonl")
	lEnv, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	rc, err := lEnv.DecodeRedis(false)
	if err != nil {
		t.Fatalf("DecodeRedis(env): %v", err)
	}
	if rc.DeadLetterFile != "/env/dead-letter.jsonl" {
		t.Errorf("redis dead_letter_file from env = %q, want /env/dead-letter.jsonl", rc.DeadLetterFile)
	}
}

// TestBalanceScalarFlagsBindAndPreserve is the evm-balance analogue: --interval
// and --every-blocks bind to balance.interval / balance.every_blocks via
// flagBindings, a set flag overrides the config file, and an unset flag leaves
// the file value intact.
func TestBalanceScalarFlagsBindAndPreserve(t *testing.T) {
	const body = `
[rpc]
url = "https://x"

[balance]
interval = "1m"

[[balance.native]]
name = "t"
address = "0xholder"
`
	balanceFlags := func() *pflag.FlagSet {
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		fs.String("interval", "", "")
		fs.Int("every-blocks", 0, "")
		return fs
	}

	t.Run("unset flags preserve the file values", func(t *testing.T) {
		l, err := New(Options{ConfigFile: writeConfig(t, body), Flags: balanceFlags()})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		cfg, err := l.DecodeBalance(false)
		if err != nil {
			t.Fatalf("DecodeBalance: %v", err)
		}
		if cfg.Balance.Interval != "1m" || cfg.Balance.EveryBlocks != 0 {
			t.Errorf("unset flags clobbered the file: interval=%q every_blocks=%d (want 1m/0)",
				cfg.Balance.Interval, cfg.Balance.EveryBlocks)
		}
	})

	t.Run("set --interval overrides the file value", func(t *testing.T) {
		fs := balanceFlags()
		if err := fs.Set("interval", "15s"); err != nil {
			t.Fatal(err)
		}
		l, err := New(Options{ConfigFile: writeConfig(t, body), Flags: fs})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		cfg, err := l.DecodeBalance(false)
		if err != nil {
			t.Fatalf("DecodeBalance: %v", err)
		}
		if cfg.Balance.Interval != "15s" {
			t.Errorf("set --interval did not override the file: interval=%q (want 15s)", cfg.Balance.Interval)
		}
	})

	t.Run("set --every-blocks binds", func(t *testing.T) {
		fs := balanceFlags()
		if err := fs.Set("every-blocks", "99"); err != nil {
			t.Fatal(err)
		}
		l, err := New(Options{ConfigFile: writeConfig(t, body), Flags: fs})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		cfg, err := l.DecodeBalance(false)
		if err != nil {
			t.Fatalf("DecodeBalance: %v", err)
		}
		if cfg.Balance.EveryBlocks != 99 {
			t.Errorf("set --every-blocks did not bind: every_blocks=%d (want 99)", cfg.Balance.EveryBlocks)
		}
	})
}
