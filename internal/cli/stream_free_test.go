package cli

import (
	"context"
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
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "empty"))

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
