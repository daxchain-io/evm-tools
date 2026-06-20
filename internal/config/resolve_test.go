package config

import (
	"strings"
	"testing"
)

func TestInterpolationEnv(t *testing.T) {
	t.Setenv("TEST_RPC_HOST", "rpc.example.com")
	t.Setenv("TEST_RPC_TOKEN", "s3cr3t")
	p := writeConfig(t, `
chain = "my-chain"
[rpc]
url = "https://${TEST_RPC_HOST}:8545?token=${TEST_RPC_TOKEN}"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeStream(false)
	if err != nil {
		t.Fatalf("DecodeStream: %v", err)
	}
	if got, want := cfg.RPC.URL, "https://rpc.example.com:8545?token=s3cr3t"; got != want {
		t.Errorf("rpc.url = %q, want %q", got, want)
	}
}

func TestInterpolationDefault(t *testing.T) {
	// Unset -> default.
	p := writeConfig(t, `chain = "${MAYBE_CHAIN:-fallback-chain}"`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeStream(false)
	if err != nil {
		t.Fatalf("DecodeStream: %v", err)
	}
	if cfg.Chain != "fallback-chain" {
		t.Errorf("chain = %q, want fallback-chain", cfg.Chain)
	}

	// Set -> value wins over default.
	t.Setenv("MAYBE_CHAIN", "real-chain")
	l2, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg2, err := l2.DecodeStream(false)
	if err != nil {
		t.Fatalf("DecodeStream: %v", err)
	}
	if cfg2.Chain != "real-chain" {
		t.Errorf("chain = %q, want real-chain", cfg2.Chain)
	}
}

func TestInterpolationDollarLiteral(t *testing.T) {
	p := writeConfig(t, `chain = "a$$b"`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeStream(false)
	if err != nil {
		t.Fatalf("DecodeStream: %v", err)
	}
	if cfg.Chain != "a$b" {
		t.Errorf("chain = %q, want a$b", cfg.Chain)
	}
}

func TestInterpolationUnsetFatal(t *testing.T) {
	p := writeConfig(t, `chain = "${DEFINITELY_UNSET_VAR_XYZ}"`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := l.DecodeStream(false); err == nil {
		t.Fatal("expected error for unset ${VAR} with no default")
	} else if !strings.Contains(err.Error(), "DEFINITELY_UNSET_VAR_XYZ") {
		t.Errorf("error should name the variable: %v", err)
	}
}

func TestInterpolationInArray(t *testing.T) {
	t.Setenv("USDC_ADDR", "0xabc")
	p := writeConfig(t, `
chain = "my-chain"
[[stream.contracts]]
name = "usdc"
address = "${USDC_ADDR}"
events = ["Transfer"]
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeStream(false)
	if err != nil {
		t.Fatalf("DecodeStream: %v", err)
	}
	if len(cfg.Stream.Contracts) != 1 || cfg.Stream.Contracts[0].Address != "0xabc" {
		t.Errorf("contract address = %+v", cfg.Stream.Contracts)
	}
}

func TestCmdDisabledFatal(t *testing.T) {
	p := writeConfig(t, `
chain = "my-chain"
[rpc]
url_cmd = "echo https://x:8545"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = l.DecodeStream(false)
	if err == nil {
		t.Fatal("expected error: _cmd while exec disabled")
	}
	if !strings.Contains(err.Error(), "allow-exec") && !strings.Contains(err.Error(), "ALLOW_EXEC") {
		t.Errorf("error should point at --allow-exec: %v", err)
	}
}

func TestCmdResolves(t *testing.T) {
	p := writeConfig(t, `
chain = "my-chain"
[rpc]
url_cmd = "echo https://resolved:8545"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeStream(true)
	if err != nil {
		t.Fatalf("DecodeStream: %v", err)
	}
	if cfg.RPC.URL != "https://resolved:8545" {
		t.Errorf("rpc.url = %q, want https://resolved:8545", cfg.RPC.URL)
	}
}

func TestCmdBothSetFatal(t *testing.T) {
	p := writeConfig(t, `
chain = "my-chain"
[rpc]
url = "https://literal:8545"
url_cmd = "echo https://other:8545"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := l.DecodeStream(true); err == nil {
		t.Fatal("expected error: both url and url_cmd set")
	} else if !strings.Contains(err.Error(), "not both") {
		t.Errorf("error should explain both-set: %v", err)
	}
}

func TestCmdShortCircuitedByEnvBinding(t *testing.T) {
	// A higher-precedence env binding provides rpc.url, so url_cmd must not run
	// (the command would fail if it did).
	t.Setenv("EVM_TOOLS_RPC_URL", "https://from-env:9999")
	p := writeConfig(t, `
chain = "my-chain"
[rpc]
url_cmd = "exit 1"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeStream(true)
	if err != nil {
		t.Fatalf("DecodeStream should short-circuit the failing _cmd, got: %v", err)
	}
	if cfg.RPC.URL != "https://from-env:9999" {
		t.Errorf("rpc.url = %q, want the env binding value", cfg.RPC.URL)
	}
}

func TestCmdInterpolatedCommand(t *testing.T) {
	t.Setenv("VAULT_HOST", "h")
	p := writeConfig(t, `
chain = "my-chain"
[rpc]
url_cmd = "echo https://${VAULT_HOST}:1"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeStream(true)
	if err != nil {
		t.Fatalf("DecodeStream: %v", err)
	}
	if cfg.RPC.URL != "https://h:1" {
		t.Errorf("rpc.url = %q, want https://h:1", cfg.RPC.URL)
	}
}

func TestCmdNonZeroExitFatal(t *testing.T) {
	p := writeConfig(t, `
chain = "my-chain"
[rpc]
url_cmd = "echo boom-detail 1>&2; exit 3"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = l.DecodeStream(true)
	if err == nil {
		t.Fatal("expected error for non-zero _cmd exit")
	}
	if !strings.Contains(err.Error(), "boom-detail") {
		t.Errorf("error should surface command stderr: %v", err)
	}
}

func TestCmdNoShellFatal(t *testing.T) {
	t.Setenv("PATH", "")
	p := writeConfig(t, `
chain = "my-chain"
[rpc]
url_cmd = "echo x"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = l.DecodeStream(true)
	if err == nil {
		t.Fatal("expected error when no shell is available")
	}
	if !strings.Contains(err.Error(), "shell") {
		t.Errorf("error should mention the missing shell: %v", err)
	}
}

// TestSiblingCmdIgnored verifies a _cmd in the OTHER tool's section neither
// triggers the disabled-exec error nor runs: evm-stream ignores [balance].
func TestSiblingCmdIgnored(t *testing.T) {
	p := writeConfig(t, `
chain = "my-chain"
[balance]
interval_cmd = "echo 1m"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := l.DecodeStream(false); err != nil {
		t.Errorf("stream must ignore a _cmd in [balance], got: %v", err)
	}

	// The owning tool, with exec disabled, must fail fast on it.
	l2, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := l2.DecodeBalance(false); err == nil {
		t.Error("balance must reject its own _cmd when exec is disabled")
	}
}

// TestCmdNotShortCircuitedByDefault guards the regression where a built-in
// default made viper.IsSet true and silently skipped the command.
func TestCmdNotShortCircuitedByDefault(t *testing.T) {
	// stream.from_block has a default ("latest"); a _cmd on it must still run.
	p := writeConfig(t, `
chain = "my-chain"
[stream]
from_block_cmd = "echo 12345"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeStream(true)
	if err != nil {
		t.Fatalf("DecodeStream: %v", err)
	}
	if cfg.Stream.FromBlock != "12345" {
		t.Errorf("from_block = %q, want 12345 (a default must not short-circuit the _cmd)", cfg.Stream.FromBlock)
	}
}

// TestInterpolationNotAppliedToBindingValue guards the regression where
// interpolation ran on the merged map and expanded env/flag binding values.
func TestInterpolationNotAppliedToBindingValue(t *testing.T) {
	// A value supplied via an env binding is not file-sourced: ${...} inside it
	// is left literal and an unset reference is not fatal.
	t.Setenv("EVM_TOOLS_RPC_URL", "https://x:1?token=${SHOULD_NOT_EXPAND}")
	p := writeConfig(t, `chain = "my-chain"`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeStream(false)
	if err != nil {
		t.Fatalf("DecodeStream should not interpolate a binding value: %v", err)
	}
	if cfg.RPC.URL != "https://x:1?token=${SHOULD_NOT_EXPAND}" {
		t.Errorf("rpc.url = %q, want the literal env value", cfg.RPC.URL)
	}
}
