package cli

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/filesink"
	"github.com/daxchain-io/evm-tools/internal/metrics"
	"github.com/daxchain-io/evm-tools/internal/record"
)

// fileRun implements `evm-sink-file run`: load config, build the filter and the
// rotating writer, then read JSONL from stdin and append each record
// at-least-once until EOF or a signal.
func fileRun(cmd *cobra.Command, f *sinkFlags) error {
	cfg, err := f.decodeFile(cmd)
	if err != nil {
		return err
	}
	resolved, err := validateFile(cfg)
	if err != nil {
		return err
	}

	logger := slog.Default()

	// A sink resolves no chain, so the chain labels are empty/"unknown"; they stay
	// on the metric set for dashboard parity with the producers.
	m := metrics.NewFile(cfg.Chain, "")
	m.SetUp(true)

	healthBase := metrics.NewHealth(readyEmitBlockedThreshold, 0) // lag disabled for a sink.
	health := metrics.NewFileHealth(healthBase)
	// Start optimistically writable so an idle sink with no traffic isn't falsely
	// not-ready; a failed write flips it (there is no active disk probe).
	health.SetWritable(true)

	w, err := filesink.NewWriter(filesink.RotateConfig{
		Path:       resolved.Path,
		MaxSize:    resolved.MaxSize,
		MaxAge:     resolved.MaxAge,
		MaxBackups: resolved.MaxBackups,
		Compress:   resolved.Compress,
		Fsync:      resolved.Fsync,
		Logger:     logger,
		OnRotate:   m.IncRotation,
	})
	if err != nil {
		return err
	}

	mc := f.fileMetricsConfig(cmd, cfg)
	srv, err := metrics.NewServer(metrics.ServerOptions{
		Addr:           mc.Addr,
		MetricsEnabled: mc.Enabled,
		MetricsPath:    mc.Path,
		Registry:       m.Registry(),
		Health:         healthBase,
	})
	if err != nil {
		_ = w.Close()
		return err
	}
	go func() {
		if serveErr := srv.Serve(); serveErr != nil {
			logger.Error("metrics server stopped unexpectedly; marking process not-live", "error", serveErr)
			healthBase.SetLive(false)
		}
	}()
	logger.Info("health/metrics server listening", "addr", srv.Addr(), "metrics_enabled", mc.Enabled)
	logger.Info("file sink started",
		"path", resolved.Path,
		"max_size_mb", resolved.MaxSizeMB,
		"rotation_interval", resolved.RotationSummary,
		"max_backups", resolved.MaxBackups,
		"compress", resolved.Compress,
		"fsync", resolved.Fsync,
		"filters", resolved.FilterSummary,
	)

	reader := record.NewReader(cmd.InOrStdin())
	sink, err := filesink.New(filesink.Options{
		Reader:      reader,
		Writer:      w,
		Filter:      resolved.Filter,
		Metrics:     m,
		Health:      health,
		Logger:      logger,
		BackoffBase: resolved.BackoffBase,
		BackoffMax:  resolved.BackoffMax,
	})
	if err != nil {
		_ = w.Close()
		return err
	}

	m.SetWorkers(1)
	runErr := sink.Run(cmd.Context())

	m.SetWorkers(0)
	m.SetUp(false)

	// Graceful shutdown: flush and close the file, then stop the metrics server.
	if syncErr := w.Sync(); syncErr != nil {
		logger.Warn("final file sync", "error", syncErr)
	}
	if closeErr := w.Close(); closeErr != nil {
		logger.Warn("file close", "error", closeErr)
	}
	shutCtx, cancelShut := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancelShut()
	if shutErr := srv.Shutdown(shutCtx); shutErr != nil {
		logger.Warn("metrics server shutdown", "error", shutErr)
	}
	return runErr
}

// fileValidate implements `evm-sink-file validate`: load+decode config and
// validate the path, rotation settings, and filters — without creating the file
// or directory (validate performs no filesystem writes).
func fileValidate(cmd *cobra.Command, f *sinkFlags) error {
	cfg, err := f.decodeFile(cmd)
	if err != nil {
		return err
	}
	if _, err := validateFile(cfg); err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), "ok: config, output path, rotation settings, and filters validated")
	return nil
}

// resolvedFile is the validated, ready-to-run file sink configuration.
type resolvedFile struct {
	Path            string
	MaxSize         int64 // bytes
	MaxSizeMB       int
	MaxAge          time.Duration
	RotationSummary string
	MaxBackups      int
	Compress        bool
	Fsync           bool
	Filter          *filesink.Filter
	FilterSummary   string
	BackoffBase     time.Duration
	BackoffMax      time.Duration
}

// validateFile applies the cross-field invariants strict decoding cannot express
// and returns the resolved configuration so run can reuse it.
func validateFile(cfg *config.FileFull) (resolvedFile, error) {
	fc := cfg.File

	if strings.TrimSpace(fc.Path) == "" {
		return resolvedFile{}, fmt.Errorf("file.path is required (set [file].path or --path)")
	}
	if fc.MaxSizeMB < 0 {
		return resolvedFile{}, fmt.Errorf("file.max_size_mb must be >= 0 (got %d)", fc.MaxSizeMB)
	}
	if fc.MaxBackups < 0 {
		return resolvedFile{}, fmt.Errorf("file.max_backups must be >= 0 (got %d)", fc.MaxBackups)
	}

	// "" / "0" / "off" disables time-based rotation; otherwise a positive duration.
	maxAge, err := parseRotationInterval(fc.RotationInterval)
	if err != nil {
		return resolvedFile{}, err
	}
	backoffBase, err := parseDurationDefault(fc.BackoffBase, 500*time.Millisecond, "file.backoff_base")
	if err != nil {
		return resolvedFile{}, err
	}
	backoffMax, err := parseDurationDefault(fc.BackoffMax, 30*time.Second, "file.backoff_max")
	if err != nil {
		return resolvedFile{}, err
	}

	filter, summary := buildFileFilter(fc.Filters)

	rotationSummary := "off"
	if maxAge > 0 {
		rotationSummary = maxAge.String()
	}

	return resolvedFile{
		Path:            fc.Path,
		MaxSize:         int64(fc.MaxSizeMB) * 1024 * 1024,
		MaxSizeMB:       fc.MaxSizeMB,
		MaxAge:          maxAge,
		RotationSummary: rotationSummary,
		MaxBackups:      fc.MaxBackups,
		Compress:        fc.Compress,
		Fsync:           fc.Fsync,
		Filter:          filter,
		FilterSummary:   summary,
		BackoffBase:     backoffBase,
		BackoffMax:      backoffMax,
	}, nil
}

// parseRotationInterval parses file.rotation_interval. The documented disable
// spellings ("" / "0" / "0s" / "off" / "none" / "disabled") return 0 (no
// time-based rotation); anything else must be a strictly positive duration, so a
// negative or zero value (or a parse failure) is a fatal error rather than a
// silent disable — matching the max_size_mb / max_backups guards.
func parseRotationInterval(s string) (time.Duration, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "0", "0s", "off", "none", "disabled":
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("file.rotation_interval: invalid duration %q: %w", s, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("file.rotation_interval: duration must be positive, got %q", s)
	}
	return d, nil
}

// buildFileFilter resolves the [file.filters] config into a filesink Filter and a
// short summary string for the startup log.
func buildFileFilter(fc config.FileFilters) (*filesink.Filter, string) {
	filter := filesink.NewFilter(filesink.FilterOptions{
		IncludeTypes: fc.IncludeTypes,
		ExcludeTypes: fc.ExcludeTypes,
		IncludeNames: fc.IncludeNames,
		ExcludeNames: fc.ExcludeNames,
	})
	summary := "write all"
	if len(fc.IncludeTypes) > 0 || len(fc.ExcludeTypes) > 0 ||
		len(fc.IncludeNames) > 0 || len(fc.ExcludeNames) > 0 {
		summary = fmt.Sprintf("include_types=%d exclude_types=%d include_names=%d exclude_names=%d",
			len(fc.IncludeTypes), len(fc.ExcludeTypes), len(fc.IncludeNames), len(fc.ExcludeNames))
	}
	return filter, summary
}

// decodeFile loads and strict-decodes the evm-sink-file config.
func (f *sinkFlags) decodeFile(cmd *cobra.Command) (*config.FileFull, error) {
	loader, err := f.loadConfig(cmd)
	if err != nil {
		return nil, err
	}
	return loader.DecodeFile(f.allowExecEnabled())
}

// fileMetricsConfig resolves the metrics endpoint config for evm-sink-file.
func (f *sinkFlags) fileMetricsConfig(cmd *cobra.Command, cfg *config.FileFull) resolvedMetrics {
	cf := commandFlags{
		metricsChanged:     cmd.Flags().Changed("metrics"),
		metricsAddrChanged: cmd.Flags().Changed("metrics-addr"),
		metricsPathChanged: cmd.Flags().Changed("metrics-path"),
	}
	return f.resolveSinkMetrics(cf, cfg.Metrics, cfg.File.Metrics, ":9004")
}
