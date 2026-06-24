package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/metrics"
	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/stdoutsink"
)

// stdoutRun implements `evm-sink-stdout run`: read JSONL records from the input
// transport and write each one's verbatim line to stdout — the composability
// hatch for `| jq` and piping. Its own diagnostics go to stderr (set up in
// setupLogging) so the stdout record stream stays clean.
func stdoutRun(cmd *cobra.Command, f *sinkFlags) error {
	// Ignore SIGPIPE first (before any stdout write) so a broken downstream (e.g.
	// `| jq` exiting) returns EPIPE to the writer — which the run loop treats as a
	// clean "downstream gone" stop — instead of the default disposition killing the
	// sink with a signal.
	signal.Ignore(syscall.SIGPIPE)

	cfg, err := f.decodeShared(cmd)
	if err != nil {
		return err
	}

	logger := slog.Default()

	// A sink resolves no chain, so the chain labels are empty/"unknown"; they stay
	// on the metric set for dashboard parity with the producers.
	m := metrics.NewSinkMetrics("evm_sink_stdout", cfg.Chain, "")
	m.SetUp(true)

	healthBase := metrics.NewHealth(readyEmitBlockedThreshold, 0) // lag disabled for a sink.
	// The stdout sink never blocks on a destination (it fails fast on a broken
	// pipe), so it is reachable for as long as it runs.
	healthBase.SetRPCReachable(true)

	mc := f.stdoutMetricsConfig(cmd, cfg)
	srv, err := metrics.NewServer(metrics.ServerOptions{
		Addr:           mc.Addr,
		MetricsEnabled: mc.Enabled,
		MetricsPath:    mc.Path,
		Registry:       m.Registry(),
		Health:         healthBase,
	})
	if err != nil {
		return err
	}
	go func() {
		if serveErr := srv.Serve(); serveErr != nil {
			logger.Error("metrics server stopped unexpectedly; marking process not-live", "error", serveErr)
			healthBase.SetLive(false)
		}
	}()
	logger.Info("health/metrics server listening", "addr", srv.Addr(), "metrics_enabled", mc.Enabled)
	logger.Info("stdout sink started")

	in, err := f.openInput(cmd, cfg.Input)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	reader := record.NewReader(in)
	dlq, err := f.installDeadLetter(cmd, reader, string(ToolSinkStdout), cfg.DeadLetterFile, m, logger)
	if err != nil {
		return err
	}
	if dlq != nil {
		defer func() { _ = dlq.Close() }()
	}

	sink, err := stdoutsink.New(stdoutsink.Options{
		Reader:  reader,
		Writer:  stdoutsink.NewLineWriter(cmd.OutOrStdout()),
		Metrics: m,
		Logger:  logger,
	})
	if err != nil {
		return err
	}

	m.SetWorkers(1)
	runErr := sink.Run(cmd.Context())

	m.SetWorkers(0)
	m.SetUp(false)
	shutCtx, cancelShut := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancelShut()
	if shutErr := srv.Shutdown(shutCtx); shutErr != nil {
		logger.Warn("metrics server shutdown", "error", shutErr)
	}
	return runErr
}

// stdoutValidate implements `evm-sink-stdout validate`: load+decode config. There
// is no destination to validate (stdout is always available), so this just
// confirms the shared config parses.
func stdoutValidate(cmd *cobra.Command, f *sinkFlags) error {
	if _, err := f.decodeShared(cmd); err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), "ok: config validated")
	return nil
}

// decodeShared loads and strict-decodes only the shared config keys (the stdout
// sink has no tool-specific subtree).
func (f *sinkFlags) decodeShared(cmd *cobra.Command) (*config.Shared, error) {
	loader, err := f.loadConfig(cmd)
	if err != nil {
		return nil, err
	}
	return loader.DecodeShared(f.allowExecEnabled())
}

// stdoutMetricsConfig resolves the metrics endpoint config for evm-sink-stdout
// (shared [metrics] only; there is no [stdout] subtree).
func (f *sinkFlags) stdoutMetricsConfig(cmd *cobra.Command, cfg *config.Shared) resolvedMetrics {
	cf := commandFlags{
		metricsChanged:     cmd.Flags().Changed("metrics"),
		metricsAddrChanged: cmd.Flags().Changed("metrics-addr"),
		metricsPathChanged: cmd.Flags().Changed("metrics-path"),
	}
	return f.resolveSinkMetrics(cf, cfg.Metrics, config.MetricsConfig{}, ":9009")
}
