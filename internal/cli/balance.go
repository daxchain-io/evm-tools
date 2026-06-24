package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/daxchain-io/evm-tools/internal/balance"
	"github.com/daxchain-io/evm-tools/internal/chain"
	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/metrics"
	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
	"github.com/daxchain-io/evm-tools/internal/transport"
)

// balanceRun implements `evm-balance run`: load config, resolve mTLS material
// and targets, connect, then sample balances/contract state on the configured
// cadence, emitting JSONL until a signal arrives.
func balanceRun(cmd *cobra.Command, f *sharedFlags) error {
	cfg, err := f.decodeBalance(cmd)
	if err != nil {
		return err
	}
	if err := applyBalanceFlags(f, cfg); err != nil {
		return err
	}
	resolved, err := validateBalance(cfg)
	if err != nil {
		return err
	}

	logger := slog.Default()
	rpc.SetKeyPermWarner(func(path string, mode os.FileMode) {
		logger.Warn("rpc client_key is group/world-readable; tighten its mode",
			"path", path, "mode", fmt.Sprintf("%#o", mode))
	})

	// Connect first to resolve chain ID, so the metric set's chain_id const
	// label is stable for the process lifetime.
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

	m := metrics.NewBalance(info.Name, fmt.Sprintf("%d", info.ID))
	m.SetUp(true)
	m.SetConfiguredNative(len(resolved.Native))
	m.SetConfiguredERC20(len(resolved.ERC20))
	m.SetConfiguredERC721Balances(len(resolved.ERC721Balances))
	m.SetConfiguredERC721Ownership(len(resolved.ERC721Ownership))
	m.SetConfiguredContracts(len(resolved.Contracts))

	// Rebuild the client with the metrics observer now that the set exists.
	client, err = rpc.New(rpc.Options{
		URL:      cfg.RPC.URL,
		TLS:      rpcTLSFromConfig(cfg.RPC),
		Observer: m.RPCObserver(),
	})
	if err != nil {
		return err
	}

	headStaleness, err := parseDisableableDuration(cfg.Balance.HeadStalenessThreshold, "balance.head_staleness_threshold")
	if err != nil {
		return err
	}
	targetTimeout, err := parseDisableableDuration(cfg.Balance.TargetTimeout, "balance.target_timeout")
	if err != nil {
		return err
	}

	// Lag is informational for the balance poller (a sampler is expected to be
	// behind head between samples — that gap is the configured cadence, not an
	// unhealthy state), so the lag dimension of /readyz is disabled here. The
	// evm_balance_lag_blocks gauge still reports real staleness for dashboards;
	// emit-blocked (a stalled consumer on the output socket) remains the meaningful
	// readiness signal.
	health := metrics.NewHealth(readyEmitBlockedThreshold, 0)
	health.SetHeadStalenessThreshold(headStaleness)
	health.SetRPCReachable(true)

	mc := f.balanceMetricsConfig(cmd, cfg)
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
			logger.Error("metrics server stopped unexpectedly; marking process not-live", "error", serveErr)
			health.SetLive(false)
		}
	}()
	logger.Info("health/metrics server listening", "addr", srv.Addr(), "metrics_enabled", mc.Enabled)

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

	poller, err := balance.New(balance.Options{
		Client:          client,
		Emitter:         writer,
		Metrics:         m,
		Health:          health,
		Logger:          logger,
		ChainName:       info.Name,
		ChainID:         info.ID,
		Cadence:         resolved.Cadence,
		Native:          resolved.Native,
		ERC20:           resolved.ERC20,
		Contracts:       resolved.Contracts,
		ERC721Balances:  resolved.ERC721Balances,
		ERC721Ownership: resolved.ERC721Ownership,
		MaxConcurrency:  cfg.Balance.MaxConcurrency,
		TargetTimeout:   targetTimeout,
	})
	if err != nil {
		return err
	}

	// SIGHUP also hot-reloads the watched target set: re-decode the config and
	// stage the new targets, which the poll loop applies at the next tick (removed
	// targets' gauge series are reset). Cadence and connection-level changes still
	// need a restart. The run command's own SIGHUP watcher handles log level/format.
	defer watchReload(rootCtx, func() { reloadBalanceTargets(cmd, f, poller, m) })()

	m.SetWorkers(1)
	runErr := poller.Run(rootCtx)

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

// reloadBalanceTargets re-decodes the config on SIGHUP and stages the new watched
// target set for the poll loop to apply. On any decode/resolve failure — or a
// reload that leaves no targets — it keeps the running set, logs, and counts the
// failure. Only the target lists are applied at runtime; the cadence and the RPC
// connection require a restart.
func reloadBalanceTargets(cmd *cobra.Command, f *sharedFlags, p *balance.Poller, m *metrics.Balance) {
	fail := func(err error) {
		slog.Warn("config reload: keeping running target set", "error", err.Error())
		m.IncConfigReloadError()
	}
	cfg, err := f.decodeBalance(cmd)
	if err != nil {
		fail(err)
		return
	}
	if err := applyBalanceFlags(f, cfg); err != nil {
		fail(err)
		return
	}
	resolved, err := balance.Resolve(cfg.Balance)
	if err != nil {
		fail(err)
		return
	}
	if len(resolved.Native)+len(resolved.ERC20)+len(resolved.Contracts)+
		len(resolved.ERC721Balances)+len(resolved.ERC721Ownership) == 0 {
		fail(fmt.Errorf("reloaded config has no targets to poll"))
		return
	}
	p.QueueReload(resolved.Native, resolved.ERC20, resolved.Contracts,
		resolved.ERC721Balances, resolved.ERC721Ownership)
	// Reflect the new configured-entry gauges immediately; the success counter and
	// the actual swap land on the poll goroutine in applyPendingReload.
	m.SetConfiguredNative(len(resolved.Native))
	m.SetConfiguredERC20(len(resolved.ERC20))
	m.SetConfiguredERC721Balances(len(resolved.ERC721Balances))
	m.SetConfiguredERC721Ownership(len(resolved.ERC721Ownership))
	m.SetConfiguredContracts(len(resolved.Contracts))
}

// balanceValidate implements `evm-balance validate`: load+decode config,
// validate mTLS material (without connecting), and resolve all configured
// targets and the sampling cadence.
func balanceValidate(cmd *cobra.Command, f *sharedFlags) error {
	cfg, err := f.decodeBalance(cmd)
	if err != nil {
		return err
	}
	if err := applyBalanceFlags(f, cfg); err != nil {
		return err
	}
	if _, err := validateBalance(cfg); err != nil {
		return err
	}

	// mTLS material check: building the client validates the certs/key for an
	// HTTPS URL without performing any network I/O.
	if _, err := rpc.New(rpc.Options{URL: cfg.RPC.URL, TLS: rpcTLSFromConfig(cfg.RPC)}); err != nil {
		return fmt.Errorf("rpc transport: %w", err)
	}

	fmt.Fprintln(cmd.OutOrStdout(), "ok: config, mTLS material, and balance targets validated")
	return nil
}

// balanceCheckRPC implements `evm-balance check rpc`.
func balanceCheckRPC(cmd *cobra.Command, f *sharedFlags) error {
	cfg, err := f.decodeBalance(cmd)
	if err != nil {
		return err
	}
	return runCheckRPC(cmd, rpcMaterial{URL: cfg.RPC.URL, TLS: rpcTLSFromConfig(cfg.RPC)}, false)
}

// validateBalance applies the cross-field invariants strict decoding cannot
// express, returning the resolved targets so callers (run) can reuse them.
func validateBalance(cfg *config.BalanceFull) (balance.Resolved, error) {
	if cfg.RPC.URL == "" {
		return balance.Resolved{}, fmt.Errorf("rpc.url is required")
	}
	if cfg.Balance.MaxConcurrency < 0 {
		return balance.Resolved{}, fmt.Errorf("balance.max_concurrency must be >= 0 (got %d; 0 uses the built-in default)", cfg.Balance.MaxConcurrency)
	}
	if _, err := parseDisableableDuration(cfg.Balance.TargetTimeout, "balance.target_timeout"); err != nil {
		return balance.Resolved{}, err
	}
	if _, err := parseDisableableDuration(cfg.Balance.HeadStalenessThreshold, "balance.head_staleness_threshold"); err != nil {
		return balance.Resolved{}, err
	}
	resolved, err := balance.Resolve(cfg.Balance)
	if err != nil {
		return balance.Resolved{}, fmt.Errorf("resolve balance config: %w", err)
	}
	return resolved, nil
}

// applyBalanceFlags merges the evm-balance config-free target flags (--native,
// --erc20) onto the decoded config, so the poller can run with no config file.
// Each flag adds to any configured targets. --erc20 takes "token:holder" (two
// addresses); the pair string doubles as the target name, mirroring how --native
// and evm-stream's --contract use the address as the name. The cadence flags
// (--interval / --every-blocks) bind through flagBindings, not here.
func applyBalanceFlags(f *sharedFlags, cfg *config.BalanceFull) error {
	for _, addr := range f.balanceNative {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		cfg.Balance.Native = append(cfg.Balance.Native, config.BalanceNative{Name: addr, Address: addr})
	}
	for _, pair := range f.balanceERC20 {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		// Split on a single colon only: "token:holder". strings.Cut would accept
		// "a:b:c" (holder="b:c"), violating the two-address contract, so require
		// exactly two segments.
		parts := strings.Split(pair, ":")
		token, holder := "", ""
		if len(parts) == 2 {
			token, holder = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		}
		if token == "" || holder == "" {
			return fmt.Errorf("--erc20 %q must be \"token:holder\" (two addresses separated by a colon)", pair)
		}
		cfg.Balance.ERC20 = append(cfg.Balance.ERC20, config.BalanceERC20{Name: pair, Token: token, Address: holder})
	}
	return nil
}

// decodeBalance loads and strict-decodes the evm-balance config.
func (f *sharedFlags) decodeBalance(cmd *cobra.Command) (*config.BalanceFull, error) {
	loader, err := f.loadConfig(cmd)
	if err != nil {
		return nil, err
	}
	return loader.DecodeBalance(f.allowExecEnabled())
}

// balanceMetricsConfig resolves the metrics endpoint config for evm-balance.
func (f *sharedFlags) balanceMetricsConfig(cmd *cobra.Command, cfg *config.BalanceFull) resolvedMetrics {
	cf := commandFlags{
		metricsChanged:     cmd.Flags().Changed("metrics"),
		metricsAddrChanged: cmd.Flags().Changed("metrics-addr"),
		metricsPathChanged: cmd.Flags().Changed("metrics-path"),
	}
	return f.resolveMetrics(cf, cfg.Metrics, cfg.Balance.Metrics, ":9001")
}
