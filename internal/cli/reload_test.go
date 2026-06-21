package cli

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"

	"github.com/daxchain-io/evm-tools/internal/logging"
)

// TestReloadLoggingFromFile verifies a SIGHUP-style reload re-reads the config
// file and live-applies the log level to the default logger.
func TestReloadLoggingFromFile(t *testing.T) {
	// Start at info; restore afterwards so other tests see a clean default.
	if _, err := logging.Setup("info", "text"); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	t.Cleanup(func() { _, _ = logging.Setup("info", "text") })

	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		t.Fatal("debug should be disabled at info before reload")
	}

	dir := t.TempDir()
	p := filepath.Join(dir, "evm-tools.toml")
	if err := os.WriteFile(p, []byte("[log]\nlevel = \"debug\"\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	reloadLogging(&cobra.Command{}, p, false)

	if !slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		t.Error("debug should be enabled after reloading a config with log.level=debug")
	}
}
