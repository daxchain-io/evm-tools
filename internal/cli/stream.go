package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/daxchain-io/evm-tools/internal/chain"
	"github.com/daxchain-io/evm-tools/internal/checkpoint"
	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/metrics"
	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
	"github.com/daxchain-io/evm-tools/internal/stream"
	"github.com/daxchain-io/evm-tools/internal/transport"
)

// shutdownGrace bounds the graceful-shutdown window before the metrics server
// and stream are forced down.
const shutdownGrace = 10 * time.Second

// readyEmitBlockedThreshold and readyLagThreshold are the /readyz bounds. A
// record emit blocked beyond the first (a stalled consumer on the output socket),
// or lag beyond the second, flips readiness to not-ready (see docs/design.md,
// "RPC Health Checks").
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
	if err := applyStreamFlags(cmd, f, cfg); err != nil {
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

	out, err := transport.OpenWriter(rootCtx, f.outputSpec(cmd, cfg.Output),
		transport.WriterOptions{BlockUntilConsumer: f.blockUntilConsumer})
	if err != nil {
		return fmt.Errorf("open output: %w", err)
	}
	// Close the output on every return path (including an error before Run);
	// deferred so it runs after the final writer.Flush below. Close is idempotent.
	defer func() {
		if cerr := out.Close(); cerr != nil {
			logger.Warn("output transport close", "error", cerr)
		}
	}()
	writer := record.NewWriter(out)

	// Durable resume cursor: when configured, the stream persists progress each
	// poll and resumes from it on restart (gap-free) instead of jumping to head.
	var cp stream.Checkpointer
	if path := strings.TrimSpace(cfg.Stream.CheckpointFile); path != "" {
		cp = checkpoint.NewStore(path)
		logger.Info("checkpoint/resume enabled", "checkpoint_file", path)
	}

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
		Checkpoint:     cp,
	})
	if err != nil {
		return err
	}

	// SIGHUP also hot-reloads the watched contract set / native-transfer filter:
	// re-decode the config and stage the new set, which the poll loop applies at
	// the next tick (added/removed contracts take effect then; removed contracts'
	// metric series are reset). Connection-level changes (RPC URL, chain) still
	// need a restart. The run command's own SIGHUP watcher handles log level/format.
	defer watchReload(rootCtx, func() { reloadStreamWatchSet(cmd, f, s, m) })()

	// One poll loop drives the M1 stream, so a single worker is active for the
	// run; per-monitor goroutines arrive with later milestones.
	m.SetWorkers(1)

	runErr := s.Run(rootCtx)

	// Graceful shutdown: stop the server and flush the record writer within the
	// grace window. A clean (signal) stop returns nil.
	m.SetWorkers(0)
	m.SetUp(false)
	shutCtx, cancelShut := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancelShut()
	if shutErr := srv.Shutdown(shutCtx); shutErr != nil {
		logger.Warn("metrics server shutdown", "error", shutErr)
	}
	if flushErr := writer.Flush(); flushErr != nil {
		logger.Warn("final record flush", "error", flushErr)
	}
	return runErr
}

// reloadStreamWatchSet re-decodes the config on SIGHUP and stages the new watched
// contract set + native-transfer filter for the poll loop to apply. On any decode/
// resolve failure it keeps the running watch set, logs, and counts the failure —
// a bad reload never takes down a healthy stream. Only the watched set is applied
// at runtime; connection-level changes (RPC, chain) require a restart.
func reloadStreamWatchSet(cmd *cobra.Command, f *sharedFlags, s *stream.Stream, m *metrics.Stream) {
	fail := func(err error) {
		slog.Warn("config reload: keeping running watch set", "error", err.Error())
		m.IncConfigReloadError()
	}
	cfg, err := f.decodeStream(cmd)
	if err != nil {
		fail(err)
		return
	}
	if err := applyStreamFlags(cmd, f, cfg); err != nil {
		fail(err)
		return
	}
	contracts, err := stream.ResolveContracts(cfg.Stream.Contracts)
	if err != nil {
		fail(err)
		return
	}
	s.QueueReload(contracts, stream.NativeFilterFromConfig(cfg.Stream.NativeTransfers))
	// Reflect the new configured-entry gauges immediately; the success counter and
	// the actual swap land on the poll goroutine in applyPendingReload.
	m.SetConfiguredContracts(len(contracts))
	m.SetConfiguredNativeTransfers(cfg.Stream.NativeTransfers.Enabled)
}

// streamValidate implements `evm-stream validate`: load+decode config, validate
// mTLS material (without connecting), and resolve all configured event ABIs.
func streamValidate(cmd *cobra.Command, f *sharedFlags) error {
	cfg, err := f.decodeStream(cmd)
	if err != nil {
		return err
	}
	if err := applyStreamFlags(cmd, f, cfg); err != nil {
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
	if err := applyStreamFlags(cmd, f, cfg); err != nil {
		return err
	}
	return runCheckRPC(cmd, rpcMaterial{URL: cfg.RPC.URL, TLS: rpcTLSFromConfig(cfg.RPC)},
		cfg.Stream.NativeTransfers.IncludeInternal)
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
	// Reject a malformed from_block offline so `validate` is a true preflight for
	// --from-block / stream.from_block, agreeing with what run's resolveStart accepts.
	if err := stream.ValidateFromBlock(cfg.Stream.FromBlock); err != nil {
		return err
	}
	if cfg.Stream.ReorgDepth < 0 {
		return fmt.Errorf("stream.reorg_depth must be >= 0 (got %d; 0 disables reorg handling)", cfg.Stream.ReorgDepth)
	}
	if _, err := parseDisableableDuration(cfg.Stream.HeadStalenessThreshold, "stream.head_staleness_threshold"); err != nil {
		return err
	}
	if len(cfg.Stream.Contracts) == 0 && !cfg.Stream.NativeTransfers.Enabled {
		return fmt.Errorf("nothing to monitor: pass --contract (with --events) and/or --native-transfers, " +
			"or configure [[stream.contracts]] / [stream.native_transfers]")
	}
	if cfg.Stream.NativeTransfers.IncludeInternal && !cfg.Stream.NativeTransfers.Enabled {
		return fmt.Errorf("native_transfers.include_internal requires native_transfers.enabled " +
			"(internal transfers refine native-transfer detection): set --native-transfers or [stream.native_transfers].enabled")
	}
	return nil
}

// applyStreamFlags merges the evm-stream config-free flags (--contract/--events/
// --native-transfers/--include-internal) onto the decoded config, so the producer can run with no
// config file. Flags add to any configured contracts. A bool/list flag is only
// applied when the user actually set it, so it never overrides config with a zero
// default. Each --contract becomes a contract resolved against the built-in
// standard ABIs (no per-contract abi), with --events (default "Transfer") as its
// event set; the address doubles as the record/metric name.
func applyStreamFlags(cmd *cobra.Command, f *sharedFlags, cfg *config.StreamFull) error {
	if cmd.Flags().Changed("native-transfers") {
		cfg.Stream.NativeTransfers.Enabled = f.streamNativeTransfers
	}
	if cmd.Flags().Changed("include-internal") {
		cfg.Stream.NativeTransfers.IncludeInternal = f.streamIncludeInternal
	}
	if cmd.Flags().Changed("events") && len(f.streamContracts) == 0 {
		return fmt.Errorf("--events requires at least one --contract")
	}
	events := f.streamEvents
	if len(events) == 0 {
		events = []string{"Transfer"} // the common token case
	}
	for _, addr := range f.streamContracts {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		cfg.Stream.Contracts = append(cfg.Stream.Contracts, config.StreamContract{
			Name:    addr,
			Address: addr,
			Events:  events,
		})
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
