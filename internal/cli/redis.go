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
	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/redissink"
)

// resolvedRedis is the validated evm-sink-redis runtime configuration.
type resolvedRedis struct {
	Stream        string
	Field         string
	MaxLen        int64
	Dedup         bool
	DedupTTL      time.Duration
	BackoffBase   time.Duration
	BackoffMax    time.Duration
	ProbeInterval time.Duration
}

// validateRedis applies the cross-field invariants without connecting, so
// `validate` stays offline-safe.
func validateRedis(cfg *config.RedisFull) (resolvedRedis, error) {
	if strings.TrimSpace(cfg.Redis.URL) == "" {
		return resolvedRedis{}, fmt.Errorf("redis.url is required (set [redis].url, url_cmd, or ${VAR})")
	}
	stream := strings.TrimSpace(cfg.Redis.Stream)
	if stream == "" {
		return resolvedRedis{}, fmt.Errorf("redis.stream is required (set [redis].stream or --stream)")
	}
	if cfg.Redis.MaxLen < 0 {
		return resolvedRedis{}, fmt.Errorf("redis.max_len must be >= 0 (got %d)", cfg.Redis.MaxLen)
	}
	field := strings.TrimSpace(cfg.Redis.Field)
	if field == "" {
		field = "data"
	}
	bb, err := parseDurationDefault(cfg.Redis.BackoffBase, 500*time.Millisecond, "redis.backoff_base")
	if err != nil {
		return resolvedRedis{}, err
	}
	bm, err := parseDurationDefault(cfg.Redis.BackoffMax, 30*time.Second, "redis.backoff_max")
	if err != nil {
		return resolvedRedis{}, err
	}
	probe, err := parseProbeInterval(cfg.Redis.ReadinessProbeInterval, "redis.readiness_probe_interval")
	if err != nil {
		return resolvedRedis{}, err
	}
	ttl, err := parseDisableableDuration(cfg.Redis.DedupTTL, "redis.dedup_ttl")
	if err != nil {
		return resolvedRedis{}, err
	}
	// Dedup defaults on (the config default is true); an explicit false disables it.
	dedup := cfg.Redis.Dedup == nil || *cfg.Redis.Dedup
	return resolvedRedis{
		Stream:        stream,
		Field:         field,
		MaxLen:        int64(cfg.Redis.MaxLen),
		Dedup:         dedup,
		DedupTTL:      ttl,
		BackoffBase:   bb,
		BackoffMax:    bm,
		ProbeInterval: probe,
	}, nil
}

// redisRun implements `evm-sink-redis run`.
func redisRun(cmd *cobra.Command, f *sinkFlags) error {
	cfg, err := f.decodeRedis(cmd)
	if err != nil {
		return err
	}
	resolved, err := validateRedis(cfg)
	if err != nil {
		return err
	}

	logger := slog.Default()

	appender, err := redissink.NewAppender(redissink.ClientConfig{
		URL:      cfg.Redis.URL,
		Stream:   resolved.Stream,
		Field:    resolved.Field,
		MaxLen:   resolved.MaxLen,
		Dedup:    resolved.Dedup,
		DedupTTL: resolved.DedupTTL,
	})
	if err != nil {
		return err
	}

	m := metrics.NewSinkMetrics("evm_sink_redis", cfg.Chain, "")
	m.SetUp(true)
	healthBase := metrics.NewHealth(readyEmitBlockedThreshold, 0) // lag disabled for a sink.
	health := metrics.NewSinkHealth(healthBase)

	cf := commandFlags{
		metricsChanged:     cmd.Flags().Changed("metrics"),
		metricsAddrChanged: cmd.Flags().Changed("metrics-addr"),
		metricsPathChanged: cmd.Flags().Changed("metrics-path"),
	}
	mc := f.resolveSinkMetrics(cf, cfg.Metrics, cfg.Redis.Metrics, ":9008")
	srv, err := metrics.NewServer(metrics.ServerOptions{
		Addr:           mc.Addr,
		MetricsEnabled: mc.Enabled,
		MetricsPath:    mc.Path,
		Registry:       m.Registry(),
		Health:         healthBase,
	})
	if err != nil {
		_ = appender.Close()
		return err
	}
	go func() {
		if serveErr := srv.Serve(); serveErr != nil {
			logger.Error("metrics server stopped unexpectedly; marking process not-live", "error", serveErr)
			healthBase.SetLive(false)
		}
	}()
	logger.Info("health/metrics server listening", "addr", srv.Addr(), "metrics_enabled", mc.Enabled)
	logger.Info("redis sink started",
		"target", appender.Target(), "stream", resolved.Stream, "dedup", resolved.Dedup,
		"max_len", resolved.MaxLen)

	in, err := f.openInput(cmd, cfg.Input)
	if err != nil {
		_ = appender.Close()
		return err
	}
	defer func() { _ = in.Close() }()
	reader := record.NewReader(in)
	sink, err := redissink.New(redissink.Options{
		Reader:        reader,
		Appender:      appender,
		Metrics:       m,
		Health:        health,
		Logger:        logger,
		BackoffBase:   resolved.BackoffBase,
		BackoffMax:    resolved.BackoffMax,
		ProbeInterval: resolved.ProbeInterval,
	})
	if err != nil {
		_ = appender.Close()
		return err
	}

	m.SetWorkers(1)
	runErr := sink.Run(cmd.Context())

	m.SetWorkers(0)
	m.SetUp(false)
	if closeErr := appender.Close(); closeErr != nil {
		logger.Warn("redis appender close", "error", closeErr)
	}
	shutCtx, cancelShut := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancelShut()
	if shutErr := srv.Shutdown(shutCtx); shutErr != nil {
		logger.Warn("metrics server shutdown", "error", shutErr)
	}
	return runErr
}

// redisValidate implements `evm-sink-redis validate`: it checks the config (url
// present, stream set, durations) without connecting to Redis.
func redisValidate(cmd *cobra.Command, f *sinkFlags) error {
	cfg, err := f.decodeRedis(cmd)
	if err != nil {
		return err
	}
	if _, err := validateRedis(cfg); err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), "ok: config, url, and stream validated (no redis connection made)")
	return nil
}

func (f *sinkFlags) decodeRedis(cmd *cobra.Command) (*config.RedisFull, error) {
	loader, err := f.loadConfig(cmd)
	if err != nil {
		return nil, err
	}
	return loader.DecodeRedis(f.allowExecEnabled())
}
