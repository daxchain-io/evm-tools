package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/daxchain-io/evm-tools/internal/chain"
	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/metrics"
	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
	"github.com/daxchain-io/evm-tools/internal/stream"
)

// shutdownGrace bounds the graceful-shutdown window before the metrics server
// and stream are forced down.
const shutdownGrace = 10 * time.Second

// readyEmitBlockedThreshold and readyLagThreshold are the /readyz bounds. A
// stdout write blocked beyond the first, or lag beyond the second, flips
// readiness to not-ready (see docs/design.md, "RPC Health Checks").
const (
	readyEmitBlockedThreshold = 30 * time.Second
	readyLagThreshold         = 5000
)

// streamRun implements `evm-stream run`: load config, resolve mTLS material and
// ABIs, connect, follow the head, and emit JSONL until a signal arrives.
func streamRun(cmd *cobra.Command, f *sharedFlags) error {
	cfg, err := f.decodeStream(cmd)
	if err != nil {
		return err
	}
	if err := validateStream(cfg); err != nil {
		return err
	}

	logger := slog.Default()
	// Wire the client-key permission warning to slog (path + mode only).
	rpc.SetKeyPermWarner(func(path string, mode os.FileMode) {
		logger.Warn("rpc client_key is group/world-readable; tighten its mode",
			"path", path, "mode", fmt.Sprintf("%#o", mode))
	})

	contracts, err := stream.ResolveContracts(cfg.Stream.Contracts)
	if err != nil {
		return fmt.Errorf("resolve stream events: %w", err)
	}

	// Build the metrics set after resolving chain ID so chain_id is a stable
	// const label. Connect first to resolve it.
	client, err := rpc.New(rpc.Options{
		URL: cfg.RPC.URL,
		TLS: rpcTLSFromConfig(cfg.RPC),
	})
	if err != nil {
		return err
	}

	rootCtx := cmd.Context()
	resolveCtx, cancelResolve := context.WithTimeout(rootCtx, 20*time.Second)
	info, err := chain.Resolve(resolveCtx, client, cfg.Chain)
	cancelResolve()
	if err != nil {
		return fmt.Errorf("resolve chain id: %w", err)
	}

	m := metrics.NewStream(info.Name, fmt.Sprintf("%d", info.ID))
	m.SetUp(true)
	m.SetConfiguredContracts(len(contracts))
	m.SetConfiguredNativeTransfers(cfg.Stream.NativeTransfers.Enabled)

	// Rebuild the client with the metrics observer now that the set exists.
	client, err = rpc.New(rpc.Options{
		URL:      cfg.RPC.URL,
		TLS:      rpcTLSFromConfig(cfg.RPC),
		Observer: m.RPCObserver(),
	})
	if err != nil {
		return err
	}

	headStaleness, err := parseDisableableDuration(cfg.Stream.HeadStalenessThreshold, "stream.head_staleness_threshold")
	if err != nil {
		return err
	}
	health := metrics.NewHealth(readyEmitBlockedThreshold, readyLagThreshold)
	health.SetHeadStalenessThreshold(headStaleness)
	health.SetRPCReachable(true)

	mc := f.streamMetricsConfig(cmd, cfg)
	srv, err := metrics.NewServer(metrics.ServerOptions{
		Addr:           mc.Addr,
		MetricsEnabled: mc.Enabled,
		MetricsPath:    mc.Path,
		Registry:       m.Registry(),
		Health:         health,
	})
	if err != nil {
		return err
	}
	go func() {
		if serveErr := srv.Serve(); serveErr != nil {
			// An unexpected Serve() return (clean shutdown returns nil) means the
			// health/metrics endpoint is gone; flip liveness so an orchestrator
			// probe restarts the pod rather than letting it run blind.
			logger.Error("metrics server stopped unexpectedly; marking process not-live", "error", serveErr)
			health.SetLive(false)
		}
	}()
	logger.Info("health/metrics server listening", "addr", srv.Addr(), "metrics_enabled", mc.Enabled)

	pollInterval, err := time.ParseDuration(cfg.Stream.PollInterval)
	if err != nil {
		return fmt.Errorf("invalid stream.poll_interval %q: %w", cfg.Stream.PollInterval, err)
	}

	writer := record.NewWriter(cmd.OutOrStdout())

	s, err := stream.New(stream.Options{
		Client:         client,
		Emitter:        writer,
		Metrics:        m,
		Health:         health,
		Logger:         logger,
		ChainName:      info.Name,
		ChainID:        info.ID,
		Contracts:      contracts,
		NativeFilter:   stream.NativeFilterFromConfig(cfg.Stream.NativeTransfers),
		PollInterval:   pollInterval,
		LogChunkBlocks: uint64(cfg.Stream.LogChunkBlocks),
		FromBlock:      cfg.Stream.FromBlock,
		ReorgDepth:     uint64(cfg.Stream.ReorgDepth),
	})
	if err != nil {
		return err
	}

	// One poll loop drives the M1 stream, so a single worker is active for the
	// run; per-monitor goroutines arrive with later milestones.
	m.SetWorkers(1)

	runErr := s.Run(rootCtx)

	// Graceful shutdown: stop the server and flush stdout within the grace
	// window. A clean (signal) stop returns nil.
	m.SetWorkers(0)
	m.SetUp(false)
	shutCtx, cancelShut := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancelShut()
	if shutErr := srv.Shutdown(shutCtx); shutErr != nil {
		logger.Warn("metrics server shutdown", "error", shutErr)
	}
	if flushErr := writer.Flush(); flushErr != nil {
		logger.Warn("final stdout flush", "error", flushErr)
	}
	return runErr
}

// streamValidate implements `evm-stream validate`: load+decode config, validate
// mTLS material (without connecting), and resolve all configured event ABIs.
func streamValidate(cmd *cobra.Command, f *sharedFlags) error {
	cfg, err := f.decodeStream(cmd)
	if err != nil {
		return err
	}
	if err := validateStream(cfg); err != nil {
		return err
	}

	// mTLS material check: building the client validates the certs/key for an
	// HTTPS URL without performing any network I/O.
	if _, err := rpc.New(rpc.Options{URL: cfg.RPC.URL, TLS: rpcTLSFromConfig(cfg.RPC)}); err != nil {
		return fmt.Errorf("rpc transport: %w", err)
	}

	// ABI resolution: a name that resolves to no signature (or an ambiguous
	// overload) is fatal here, at validate time.
	if _, err := stream.ResolveContracts(cfg.Stream.Contracts); err != nil {
		return fmt.Errorf("resolve stream events: %w", err)
	}

	fmt.Fprintln(cmd.OutOrStdout(), "ok: config, mTLS material, and event ABIs validated")
	return nil
}

// streamCheckRPC implements `evm-stream check rpc`.
func streamCheckRPC(cmd *cobra.Command, f *sharedFlags) error {
	cfg, err := f.decodeStream(cmd)
	if err != nil {
		return err
	}
	return runCheckRPC(cmd, rpcMaterial{URL: cfg.RPC.URL, TLS: rpcTLSFromConfig(cfg.RPC)})
}

// validateStream applies the cross-field invariants that strict decoding cannot
// express on its own.
func validateStream(cfg *config.StreamFull) error {
	if cfg.RPC.URL == "" {
		return fmt.Errorf("rpc.url is required")
	}
	if cfg.Stream.LogChunkBlocks <= 0 {
		return fmt.Errorf("stream.log_chunk_blocks must be positive (got %d)", cfg.Stream.LogChunkBlocks)
	}
	if _, err := time.ParseDuration(cfg.Stream.PollInterval); err != nil {
		return fmt.Errorf("invalid stream.poll_interval %q: %w", cfg.Stream.PollInterval, err)
	}
	if cfg.Stream.ReorgDepth < 0 {
		return fmt.Errorf("stream.reorg_depth must be >= 0 (got %d; 0 disables reorg handling)", cfg.Stream.ReorgDepth)
	}
	if _, err := parseDisableableDuration(cfg.Stream.HeadStalenessThreshold, "stream.head_staleness_threshold"); err != nil {
		return err
	}
	if len(cfg.Stream.Contracts) == 0 && !cfg.Stream.NativeTransfers.Enabled {
		return fmt.Errorf("nothing to monitor: configure [[stream.contracts]] or enable [stream.native_transfers]")
	}
	return nil
}

// decodeStream loads and strict-decodes the evm-stream config.
func (f *sharedFlags) decodeStream(cmd *cobra.Command) (*config.StreamFull, error) {
	loader, err := f.loadConfig(cmd)
	if err != nil {
		return nil, err
	}
	return loader.DecodeStream(f.allowExecEnabled())
}

// streamMetricsConfig resolves the metrics endpoint config for evm-stream.
func (f *sharedFlags) streamMetricsConfig(cmd *cobra.Command, cfg *config.StreamFull) resolvedMetrics {
	cf := commandFlags{
		metricsChanged:     cmd.Flags().Changed("metrics"),
		metricsAddrChanged: cmd.Flags().Changed("metrics-addr"),
		metricsPathChanged: cmd.Flags().Changed("metrics-path"),
	}
	return f.resolveMetrics(cf, cfg.Metrics, cfg.Stream.Metrics, ":9000")
}
