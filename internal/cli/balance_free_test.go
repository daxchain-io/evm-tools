package cli

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// TestBalanceConfigFree verifies evm-balance can be driven entirely from flags,
// with no config file: --native / --erc20 specify targets and --interval /
// --every-blocks the cadence — the data that otherwise comes only from
// [[balance.*]] / [balance]. `validate` is offline (no RPC), so it exercises the
// flag merge, the interval-XOR-every_blocks rule, and target resolution. Negative
// cases pin the error cause (not just that an error occurred) so a case cannot
// pass for the wrong reason.
func TestBalanceConfigFree(t *testing.T) {
	home := t.TempDir() // isolate so no real ~/.evm-tools config is discovered
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "empty"))

	const (
		holder = "0x1111111111111111111111111111111111111111"
		token  = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
	)
	cases := []struct {
		name    string
		args    []string
		wantErr bool
		wantSub string // for wantErr cases, a substring the error must contain
	}{
		{"native + interval", []string{"validate", "--rpc-url", "https://x", "--native", holder, "--interval", "30s"}, false, ""},
		{"erc20 + interval", []string{"validate", "--rpc-url", "https://x", "--erc20", token + ":" + holder, "--interval", "30s"}, false, ""},
		{"native + every-blocks", []string{"validate", "--rpc-url", "https://x", "--native", holder, "--every-blocks", "50"}, false, ""},
		{"erc20 no colon", []string{"validate", "--rpc-url", "https://x", "--erc20", "just-a-token", "--interval", "30s"}, true, "token:holder"},
		{"erc20 leading colon", []string{"validate", "--rpc-url", "https://x", "--erc20", ":" + holder, "--interval", "30s"}, true, "token:holder"},
		{"erc20 trailing colon", []string{"validate", "--rpc-url", "https://x", "--erc20", token + ":", "--interval", "30s"}, true, "token:holder"},
		{"erc20 extra colon", []string{"validate", "--rpc-url", "https://x", "--erc20", token + ":" + holder + ":extra", "--interval", "30s"}, true, "token:holder"},
		{"targets but no cadence", []string{"validate", "--rpc-url", "https://x", "--native", holder}, true, "no sampling cadence"},
		{"cadence but no targets", []string{"validate", "--rpc-url", "https://x", "--interval", "30s"}, true, "nothing to poll"},
		{"both cadences", []string{"validate", "--rpc-url", "https://x", "--native", holder, "--interval", "30s", "--every-blocks", "50"}, true, "exactly one"},
		{"no rpc-url", []string{"validate", "--native", holder, "--interval", "30s"}, true, "rpc.url"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := runWithCtx(context.Background(), t, ToolBalance, c.args...)
			if (err != nil) != c.wantErr {
				t.Fatalf("args %v: err=%v, wantErr=%v", c.args, err, c.wantErr)
			}
			if c.wantSub != "" && (err == nil || !strings.Contains(err.Error(), c.wantSub)) {
				t.Errorf("args %v: error %v, want substring %q", c.args, err, c.wantSub)
			}
		})
	}
}

// TestBalanceCadenceFlagConflictsWithFile pins the cross-source precedence: a
// config file's every_blocks plus a --interval flag leaves BOTH cadences set (the
// flag binds its own key but does not clear the sibling), so validate reports the
// interval-XOR-every_blocks conflict rather than silently picking one.
func TestBalanceCadenceFlagConflictsWithFile(t *testing.T) {
	cfg := writeStreamConfig(t, `
chain = "c"
[rpc]
url = "http://localhost:8545"
[balance]
every_blocks = 50
[[balance.native]]
name = "n"
address = "0x1"
`)
	_, err := runWithCtx(context.Background(), t, ToolBalance, "validate", "--config", cfg, "--interval", "30s")
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("expected file+flag cadence XOR error, got %v", err)
	}
}
