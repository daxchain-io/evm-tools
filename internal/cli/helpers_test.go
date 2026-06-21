package cli

import (
	"os"
	"path/filepath"
	"testing"
)

// writeTempConfig writes a minimal valid config and returns its path, so the
// command paths under test load real config before reaching their scaffolded
// not-implemented bodies.
func writeTempConfig(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "evm-tools.toml")
	if err := os.WriteFile(p, []byte("chain = \"my-chain\"\n"), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return p
}

// TestParseDisableableDuration covers the shared opt-in safety-knob parser used by
// head_staleness_threshold, balance.target_timeout, and redis.dedup_ttl: disable
// spellings map to 0, a positive value parses, and a NEGATIVE value is rejected
// (not silently treated as "disabled").
func TestParseDisableableDuration(t *testing.T) {
	for _, s := range []string{"", "0", "0s", "off", "none", "disabled"} {
		if d, err := parseDisableableDuration(s, "k"); err != nil || d != 0 {
			t.Errorf("parseDisableableDuration(%q) = (%v,%v), want (0,nil)", s, d, err)
		}
	}
	if d, err := parseDisableableDuration("90s", "k"); err != nil || d.Seconds() != 90 {
		t.Errorf("parseDisableableDuration(\"90s\") = (%v,%v), want (90s,nil)", d, err)
	}
	if _, err := parseDisableableDuration("-5s", "k"); err == nil {
		t.Errorf("parseDisableableDuration(\"-5s\") should be rejected, not silently disabled")
	}
	if _, err := parseDisableableDuration("nonsense", "k"); err == nil {
		t.Errorf("parseDisableableDuration(\"nonsense\") should error")
	}
}
