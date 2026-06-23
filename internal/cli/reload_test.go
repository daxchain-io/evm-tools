package cli

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/daxchain-io/evm-tools/internal/logging"
	"github.com/daxchain-io/evm-tools/internal/metrics"
	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
	"github.com/daxchain-io/evm-tools/internal/stream"
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

// noopStreamClient satisfies stream.Client; the watch-set reload helper never
// reaches the client (it only re-decodes config and resolves ABIs).
type noopStreamClient struct{}

func (noopStreamClient) ChainID(context.Context) (int64, error)               { return 1, nil }
func (noopStreamClient) BlockNumber(context.Context) (uint64, error)          { return 1, nil }
func (noopStreamClient) FinalizedBlockNumber(context.Context) (uint64, error) { return 0, nil }
func (noopStreamClient) BlockByNumberUint(context.Context, uint64, bool) (*rpc.Block, error) {
	return &rpc.Block{}, nil
}
func (noopStreamClient) GetLogs(context.Context, rpc.LogFilter) ([]rpc.Log, error) { return nil, nil }
func (noopStreamClient) TransactionReceipt(context.Context, string) (*rpc.Receipt, error) {
	return nil, nil
}
func (noopStreamClient) TraceBlockByNumber(context.Context, uint64) ([]rpc.TxTrace, error) {
	return nil, nil
}
func (noopStreamClient) TraceTransaction(context.Context, string) (*rpc.CallFrame, error) {
	return nil, nil
}
func (noopStreamClient) TraceBlockParity(context.Context, uint64) ([]rpc.ParityTrace, error) {
	return nil, nil
}

type discardEmitter struct{}

func (discardEmitter) Emit(record.Envelope) error { return nil }

func newTestStream(t *testing.T) *stream.Stream {
	t.Helper()
	s, err := stream.New(stream.Options{
		Client: noopStreamClient{}, Emitter: discardEmitter{},
		ChainName: "test", ChainID: 1, PollInterval: time.Second, LogChunkBlocks: 2000,
	})
	if err != nil {
		t.Fatalf("stream.New: %v", err)
	}
	return s
}

// counterValue reads a single counter sample from a stream metric registry.
func counterValue(t *testing.T, m *metrics.Stream, name string) float64 {
	t.Helper()
	fams, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range fams {
		if f.GetName() == name && len(f.Metric) > 0 {
			return f.Metric[0].GetCounter().GetValue()
		}
	}
	return 0
}

// gaugeValue reads a single gauge sample from a stream metric registry.
func gaugeValue(t *testing.T, m *metrics.Stream, name string) float64 {
	t.Helper()
	fams, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range fams {
		if f.GetName() == name && len(f.Metric) > 0 {
			return f.Metric[0].GetGauge().GetValue()
		}
	}
	return 0
}

// TestReloadStreamWatchSetFailureKeepsConfig verifies a bad config on reload
// counts the failure and keeps running (no panic, no success counted).
func TestReloadStreamWatchSetFailureKeepsConfig(t *testing.T) {
	bad := writeStreamConfig(t, "this is = not = valid = toml ===")
	root := NewRootCommand(ToolStream)
	root.SetContext(context.Background())
	f := &sharedFlags{configFile: bad}
	m := metrics.NewStream("test", "1")

	reloadStreamWatchSet(root, f, newTestStream(t), m)

	if got := counterValue(t, m, "evm_stream_config_reload_errors_total"); got != 1 {
		t.Errorf("config_reload_errors_total = %v, want 1", got)
	}
}

// TestReloadStreamWatchSetSuccessStages verifies a valid reload re-resolves
// contracts (no error counted) and updates the configured-contracts gauge.
func TestReloadStreamWatchSetSuccessStages(t *testing.T) {
	good := writeStreamConfig(t, `
[[stream.contracts]]
name = "usdc"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
events = ["Transfer"]
`)
	root := NewRootCommand(ToolStream)
	root.SetContext(context.Background())
	f := &sharedFlags{configFile: good}
	m := metrics.NewStream("test", "1")

	reloadStreamWatchSet(root, f, newTestStream(t), m)

	if got := counterValue(t, m, "evm_stream_config_reload_errors_total"); got != 0 {
		t.Errorf("config_reload_errors_total = %v, want 0", got)
	}
	if got := gaugeValue(t, m, "evm_stream_configured_contracts"); got != 1 {
		t.Errorf("configured_contracts gauge = %v, want 1", got)
	}
}
