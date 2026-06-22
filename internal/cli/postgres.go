package cli

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/metrics"
	"github.com/daxchain-io/evm-tools/internal/pgsink"
	"github.com/daxchain-io/evm-tools/internal/record"
)

// resolvedPostgres is the validated evm-sink-postgres runtime configuration.
type resolvedPostgres struct {
	Table         string
	BackoffBase   time.Duration
	BackoffMax    time.Duration
	ProbeInterval time.Duration
}

// validatePostgres applies the cross-field invariants without connecting, so
// `validate` stays offline-safe.
func validatePostgres(cfg *config.PostgresFull) (resolvedPostgres, error) {
	if strings.TrimSpace(cfg.Postgres.DSN) == "" {
		return resolvedPostgres{}, fmt.Errorf("postgres.dsn is required (set [postgres].dsn, dsn_cmd, or ${VAR})")
	}
	table := strings.TrimSpace(cfg.Postgres.Table)
	if table == "" {
		table = "evm_records"
	}
	if err := pgsink.ValidateTableName(table); err != nil {
		return resolvedPostgres{}, err
	}
	bb, err := parseDurationDefault(cfg.Postgres.BackoffBase, 500*time.Millisecond, "postgres.backoff_base")
	if err != nil {
		return resolvedPostgres{}, err
	}
	bm, err := parseDurationDefault(cfg.Postgres.BackoffMax, 30*time.Second, "postgres.backoff_max")
	if err != nil {
		return resolvedPostgres{}, err
	}
	probe, err := parseProbeInterval(cfg.Postgres.ReadinessProbeInterval, "postgres.readiness_probe_interval")
	if err != nil {
		return resolvedPostgres{}, err
	}
	return resolvedPostgres{Table: table, BackoffBase: bb, BackoffMax: bm, ProbeInterval: probe}, nil
}

// postgresRun implements `evm-sink-postgres run`.
func postgresRun(cmd *cobra.Command, f *sinkFlags) error {
	cfg, err := f.decodePostgres(cmd)
	if err != nil {
		return err
	}
	resolved, err := validatePostgres(cfg)
	if err != nil {
		return err
	}

	logger := slog.Default()

	inserter, err := pgsink.NewInserter(cmd.Context(), cfg.Postgres.DSN, resolved.Table, cfg.Postgres.CreateTable)
	if err != nil {
		return err
	}

	m := metrics.NewSinkMetrics("evm_sink_postgres", cfg.Chain, "")
	m.SetUp(true)
	healthBase := metrics.NewHealth(readyEmitBlockedThreshold, 0) // lag disabled for a sink.
	health := metrics.NewSinkHealth(healthBase)

	cf := commandFlags{
		metricsChanged:     cmd.Flags().Changed("metrics"),
		metricsAddrChanged: cmd.Flags().Changed("metrics-addr"),
		metricsPathChanged: cmd.Flags().Changed("metrics-path"),
	}
	mc := f.resolveSinkMetrics(cf, cfg.Metrics, cfg.Postgres.Metrics, ":9007")
	srv, err := metrics.NewServer(metrics.ServerOptions{
		Addr:           mc.Addr,
		MetricsEnabled: mc.Enabled,
		MetricsPath:    mc.Path,
		Registry:       m.Registry(),
		Health:         healthBase,
	})
	if err != nil {
		_ = inserter.Close()
		return err
	}
	go func() {
		if serveErr := srv.Serve(); serveErr != nil {
			logger.Error("metrics server stopped unexpectedly; marking process not-live", "error", serveErr)
			healthBase.SetLive(false)
		}
	}()
	logger.Info("health/metrics server listening", "addr", srv.Addr(), "metrics_enabled", mc.Enabled)
	logger.Info("postgres sink started",
		"target", inserter.Target(), "table", resolved.Table, "create_table", cfg.Postgres.CreateTable)

	in, err := f.openInput(cmd, cfg.Input)
	if err != nil {
		_ = inserter.Close()
		return err
	}
	defer func() { _ = in.Close() }()
	reader := record.NewReader(in)
	sink, err := pgsink.New(pgsink.Options{
		Reader:        reader,
		Inserter:      inserter,
		Metrics:       m,
		Health:        health,
		Logger:        logger,
		BackoffBase:   resolved.BackoffBase,
		BackoffMax:    resolved.BackoffMax,
		ProbeInterval: resolved.ProbeInterval,
	})
	if err != nil {
		_ = inserter.Close()
		return err
	}

	m.SetWorkers(1)
	runErr := sink.Run(cmd.Context())

	m.SetWorkers(0)
	m.SetUp(false)
	if closeErr := inserter.Close(); closeErr != nil {
		logger.Warn("postgres inserter close", "error", closeErr)
	}
	shutCtx, cancelShut := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancelShut()
	if shutErr := srv.Shutdown(shutCtx); shutErr != nil {
		logger.Warn("metrics server shutdown", "error", shutErr)
	}
	return runErr
}

// postgresValidate implements `evm-sink-postgres validate`: it checks the config
// (dsn present, table name safe, durations) without connecting to the database.
func postgresValidate(cmd *cobra.Command, f *sinkFlags) error {
	cfg, err := f.decodePostgres(cmd)
	if err != nil {
		return err
	}
	if _, err := validatePostgres(cfg); err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), "ok: config, dsn, and table validated (no database connection made)")
	return nil
}

func (f *sinkFlags) decodePostgres(cmd *cobra.Command) (*config.PostgresFull, error) {
	loader, err := f.loadConfig(cmd)
	if err != nil {
		return nil, err
	}
	return loader.DecodePostgres(f.allowExecEnabled())
}
