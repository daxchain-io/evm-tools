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
