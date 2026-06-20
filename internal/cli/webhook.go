package cli

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/spf13/cobra"

	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/metrics"
	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/webhooksink"
)

// newWebhookPoster constructs the poster used by `run`. It is a package var so
// tests can substitute an in-memory fake and exercise the full run path with no
// real endpoint; production uses the real net/http-backed poster.
var newWebhookPoster = webhooksink.NewHTTPPoster

// webhookRun implements `evm-sink-webhook run`: load config, build the filter and
// the real HTTP poster, then read JSONL from stdin and forward each record
// at-least-once until EOF or a signal.
func webhookRun(cmd *cobra.Command, f *sinkFlags) error {
	cfg, err := f.decodeWebhook(cmd)
	if err != nil {
		return err
	}
	resolved, err := validateWebhook(cfg)
	if err != nil {
		return err
	}

	logger := slog.Default()

	poster, err := newWebhookPoster(resolved.Poster)
	if err != nil {
		return err
	}

	// A sink resolves no chain, so the chain labels are empty/"unknown"; they stay
	// on the metric set for dashboard parity with the producers.
	m := metrics.NewWebhook(cfg.Chain, "")
	m.SetUp(true)

	healthBase := metrics.NewHealth(readyEmitBlockedThreshold, 0) // lag disabled for a sink.
	health := metrics.NewWebhookHealth(healthBase)
	// With no active probe, start optimistically ready (like the producers) so an
	// idle webhook with no traffic isn't falsely not-ready; a failed POST flips
	// it. With an active probe, the immediate probe sets the real value.
	if resolved.ProbeInterval == 0 {
		health.SetEndpointReachable(true)
	}

	mc := f.webhookMetricsConfig(cmd, cfg)
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
	logger.Info("webhook sink started",
		"url", resolved.RedactedURL,
		"method", resolved.Poster.Method,
		"auth", resolved.Poster.AuthHeader != "",
		"filters", resolved.FilterSummary,
	)

	reader := record.NewReader(cmd.InOrStdin())
	sink, err := webhooksink.New(webhooksink.Options{
		Reader:        reader,
		Poster:        poster,
		Filter:        resolved.Filter,
		Metrics:       m,
		Health:        health,
		Logger:        logger,
		BackoffBase:   resolved.BackoffBase,
		BackoffMax:    resolved.BackoffMax,
		ProbeInterval: resolved.ProbeInterval,
	})
	if err != nil {
		return err
	}

	m.SetWorkers(1)
	runErr := sink.Run(cmd.Context())

	m.SetWorkers(0)
	m.SetUp(false)

	// Graceful shutdown: close the poster, then stop the metrics server.
	if closeErr := poster.Close(); closeErr != nil {
		logger.Warn("webhook poster close", "error", closeErr)
	}
	shutCtx, cancelShut := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancelShut()
	if shutErr := srv.Shutdown(shutCtx); shutErr != nil {
		logger.Warn("metrics server shutdown", "error", shutErr)
	}
	return runErr
}

// webhookValidate implements `evm-sink-webhook validate`: load+decode config,
// validate the filter and URL/auth material (building the poster validates the
// URL/method without any network I/O), without connecting.
func webhookValidate(cmd *cobra.Command, f *sinkFlags) error {
	cfg, err := f.decodeWebhook(cmd)
	if err != nil {
		return err
	}
	resolved, err := validateWebhook(cfg)
	if err != nil {
		return err
	}
	// Building the poster validates the URL and method without performing network
	// I/O.
	poster, err := webhooksink.NewHTTPPoster(resolved.Poster)
	if err != nil {
		return fmt.Errorf("webhook poster: %w", err)
	}
	_ = poster.Close()

	fmt.Fprintln(cmd.OutOrStdout(), "ok: config, webhook url/auth material, and filters validated")
	return nil
}

// resolvedWebhook is the validated, ready-to-run webhook sink configuration.
type resolvedWebhook struct {
	Poster        webhooksink.PosterConfig
	Filter        *webhooksink.Filter
	RedactedURL   string
	FilterSummary string
	BackoffBase   time.Duration
	BackoffMax    time.Duration
	ProbeInterval time.Duration
}

// validateWebhook applies the cross-field invariants strict decoding cannot
// express and returns the resolved configuration so run can reuse it.
func validateWebhook(cfg *config.WebhookFull) (resolvedWebhook, error) {
	wh := cfg.Webhook

	if wh.URL == "" {
		return resolvedWebhook{}, fmt.Errorf("webhook.url is required (set [webhook].url or --url)")
	}

	filter, summary, err := buildWebhookFilter(wh.Filters)
	if err != nil {
		return resolvedWebhook{}, err
	}

	timeout, err := parseDurationDefault(wh.Timeout, 10*time.Second, "webhook.timeout")
	if err != nil {
		return resolvedWebhook{}, err
	}
	backoffBase, err := parseDurationDefault(wh.BackoffBase, 500*time.Millisecond, "webhook.backoff_base")
	if err != nil {
		return resolvedWebhook{}, err
	}
	backoffMax, err := parseDurationDefault(wh.BackoffMax, 30*time.Second, "webhook.backoff_max")
	if err != nil {
		return resolvedWebhook{}, err
	}
	// The active probe runs only when a health URL is configured; without one,
	// readiness follows POST outcomes (with an optimistic start).
	probeInterval := time.Duration(0)
	if wh.HealthURL != "" {
		probeInterval, err = parseProbeInterval(wh.ReadinessProbeInterval, 15*time.Second, "webhook.readiness_probe_interval")
		if err != nil {
			return resolvedWebhook{}, err
		}
	}

	pc := webhooksink.PosterConfig{
		URL:        wh.URL,
		Method:     wh.Method,
		Headers:    wh.Headers,
		AuthHeader: wh.Auth.Header,
		AuthValue:  wh.Auth.Value,
		Timeout:    timeout,
		HealthURL:  wh.HealthURL,
	}

	return resolvedWebhook{
		Poster:        pc,
		Filter:        filter,
		RedactedURL:   webhooksink.RedactURL(wh.URL),
		FilterSummary: summary,
		BackoffBase:   backoffBase,
		BackoffMax:    backoffMax,
		ProbeInterval: probeInterval,
	}, nil
}

// buildWebhookFilter resolves the [webhook.filters] config into a webhooksink
// Filter and a short, secret-free summary string for the startup log.
func buildWebhookFilter(fc config.WebhookFilters) (*webhooksink.Filter, string, error) {
	opts := webhooksink.FilterOptions{
		IncludeTypes: fc.IncludeTypes,
		ExcludeTypes: fc.ExcludeTypes,
		IncludeNames: fc.IncludeNames,
		ExcludeNames: fc.ExcludeNames,
	}
	if fc.Field != nil {
		opts.Field = &webhooksink.FieldCondition{
			Field: fc.Field.Field,
			Op:    webhooksink.FieldOp(fc.Field.Op),
			Value: fc.Field.Value,
		}
	}
	filter, err := webhooksink.NewFilter(opts)
	if err != nil {
		return nil, "", err
	}

	summary := "forward all"
	if len(fc.IncludeTypes) > 0 || len(fc.ExcludeTypes) > 0 ||
		len(fc.IncludeNames) > 0 || len(fc.ExcludeNames) > 0 || fc.Field != nil {
		summary = fmt.Sprintf("include_types=%d exclude_types=%d include_names=%d exclude_names=%d field=%t",
			len(fc.IncludeTypes), len(fc.ExcludeTypes), len(fc.IncludeNames), len(fc.ExcludeNames), fc.Field != nil)
	}
	return filter, summary, nil
}

// decodeWebhook loads and strict-decodes the evm-sink-webhook config.
func (f *sinkFlags) decodeWebhook(cmd *cobra.Command) (*config.WebhookFull, error) {
	loader, err := f.loadConfig(cmd)
	if err != nil {
		return nil, err
	}
	return loader.DecodeWebhook(f.allowExecEnabled())
}

// webhookMetricsConfig resolves the metrics endpoint config for evm-sink-webhook.
func (f *sinkFlags) webhookMetricsConfig(cmd *cobra.Command, cfg *config.WebhookFull) resolvedMetrics {
	cf := commandFlags{
		metricsChanged:     cmd.Flags().Changed("metrics"),
		metricsAddrChanged: cmd.Flags().Changed("metrics-addr"),
		metricsPathChanged: cmd.Flags().Changed("metrics-path"),
	}
	return f.resolveSinkMetrics(cf, cfg.Metrics, cfg.Webhook.Metrics, ":9003")
}
