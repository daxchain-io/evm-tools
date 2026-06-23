package cli

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestStreamConfigFree verifies evm-stream can be driven entirely from flags, with
// no config file: --native-transfers and/or --contract specify the monitor target
// that otherwise comes only from [[stream.contracts]] / [stream.native_transfers].
// `validate` is offline (no RPC), so it exercises the flag merge + ABI resolution.
func TestStreamConfigFree(t *testing.T) {
	home := t.TempDir() // isolate so no real ~/.evm-tools config is discovered
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home) // os.UserHomeDir on Windows
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "empty"))
	t.Setenv("APPDATA", filepath.Join(home, "empty")) // os.UserConfigDir on Windows

	const usdc = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
	cases := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{"native transfers only", []string{"validate", "--rpc-url", "https://x", "--native-transfers"}, false},
		{"contract + events", []string{"validate", "--rpc-url", "https://x", "--contract", usdc, "--events", "Transfer,Approval"}, false},
		{"contract default events", []string{"validate", "--rpc-url", "https://x", "--contract", usdc}, false},
		{"nothing to monitor", []string{"validate", "--rpc-url", "https://x"}, true},
		{"events without contract", []string{"validate", "--rpc-url", "https://x", "--events", "Transfer"}, true},
		{"no rpc-url", []string{"validate", "--native-transfers"}, true},
		{"from-block backfill number", []string{"validate", "--rpc-url", "https://x", "--native-transfers", "--from-block", "19000000"}, false},
		{"from-block latest", []string{"validate", "--rpc-url", "https://x", "--native-transfers", "--from-block", "latest"}, false},
		{"from-block invalid", []string{"validate", "--rpc-url", "https://x", "--native-transfers", "--from-block", "not-a-number"}, true},
		{"poll-interval valid", []string{"validate", "--rpc-url", "https://x", "--native-transfers", "--poll-interval", "5s"}, false},
		{"poll-interval invalid", []string{"validate", "--rpc-url", "https://x", "--native-transfers", "--poll-interval", "nope"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := runWithCtx(context.Background(), t, ToolStream, c.args...)
			if (err != nil) != c.wantErr {
				t.Errorf("args %v: err=%v, wantErr=%v", c.args, err, c.wantErr)
			}
		})
	}
}

// TestStreamScalarFlagOverridesConfig is the end-to-end CLI proof that
// --poll-interval outranks the config file: a file whose poll_interval is
// unparseable fails validate, but passing the flag overrides the file value so
// validate succeeds. The config-layer TestStreamScalarFlagsBindAndPreserve proves
// the same for --from-block and that an *unset* flag does not clobber the file.
func TestStreamScalarFlagOverridesConfig(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	const cfg = `
[rpc]
url = "https://x"

[stream]
poll_interval = "not-a-duration"

[[stream.contracts]]
name = "t"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
events = ["Transfer"]
`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	// Bare: the file's bogus poll_interval makes validate fail.
	if _, err := runWithCtx(context.Background(), t, ToolStream, "validate", "-c", cfgPath); err == nil {
		t.Fatal("expected validate to fail on the file's unparseable poll_interval")
	}
	// The flag binds at a higher precedence than the file, so validate passes.
	if _, err := runWithCtx(context.Background(), t, ToolStream, "validate", "-c", cfgPath, "--poll-interval", "5s"); err != nil {
		t.Fatalf("--poll-interval should override the file value; got %v", err)
	}
}
