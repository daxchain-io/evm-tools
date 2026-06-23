package cli

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/kafkasink"
	"github.com/daxchain-io/evm-tools/internal/metrics"
	"github.com/daxchain-io/evm-tools/internal/record"
)

// newKafkaPublisher constructs the publisher used by `run`. It is a package var
// so tests can substitute an in-memory fake and exercise the full run path with
// no broker; production uses the real franz-go-backed publisher.
var newKafkaPublisher = kafkasink.NewKafkaPublisher

// kafkaRun implements `evm-sink-kafka run`: load config, build the router and
// the real franz-go publisher, then read JSONL from stdin and publish each
// record at-least-once (or idempotent) until EOF or a signal.
func kafkaRun(cmd *cobra.Command, f *sinkFlags) error {
	cfg, err := f.decodeKafka(cmd)
	if err != nil {
		return err
	}
	resolved, err := validateKafka(cfg)
	if err != nil {
		return err
	}

	logger := slog.Default()

	pub, err := newKafkaPublisher(resolved.Writer)
	if err != nil {
		return err
	}

	// A sink resolves no chain, so the chain labels are empty/"unknown"; they
	// stay on the metric set for dashboard parity with the producers.
	m := metrics.NewKafka(cfg.Chain, "")
	m.SetUp(true)

	healthBase := metrics.NewHealth(readyEmitBlockedThreshold, 0) // lag disabled for a sink.
	health := metrics.NewKafkaHealth(healthBase)

	mc := f.kafkaMetricsConfig(cmd, cfg)
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
	logger.Info("kafka sink started",
		"brokers", redactBrokers(resolved.Writer.Brokers),
		"default_topic", cfg.Kafka.Topic,
		"topic_overrides", len(cfg.Kafka.TopicByType),
		"partition_key", partitionKeyDesc(cfg.Kafka.PartitionKey),
		"delivery_mode", deliveryModeDesc(resolved.Writer.Idempotent),
		"sasl", resolved.Writer.SASLMechanism != "",
		"tls", resolved.Writer.TLSEnabled,
	)

	in, err := f.openInput(cmd, cfg.Input)
	if err != nil {
		_ = pub.Close()
		return err
	}
	defer func() { _ = in.Close() }()
	reader := record.NewReader(in)
	dlq, err := f.installDeadLetter(cmd, reader, string(ToolSinkKafka), cfg.DeadLetterFile, m, logger)
	if err != nil {
		_ = pub.Close()
		return err
	}
	if dlq != nil {
		defer func() { _ = dlq.Close() }()
	}
	sink, err := kafkasink.New(kafkasink.Options{
		Reader:        reader,
		Publisher:     pub,
		Router:        resolved.Router,
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

	// Graceful shutdown: flush+close the writer so any in-flight batch is
	// confirmed, then stop the metrics server.
	if closeErr := pub.Close(); closeErr != nil {
		logger.Warn("kafka writer close", "error", closeErr)
	}
	shutCtx, cancelShut := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancelShut()
	if shutErr := srv.Shutdown(shutCtx); shutErr != nil {
		logger.Warn("metrics server shutdown", "error", shutErr)
	}
	return runErr
}

// kafkaValidate implements `evm-sink-kafka validate`: load+decode config,
// validate the router and broker/auth material (building the publisher validates
// TLS/SASL material without any network I/O), without connecting.
func kafkaValidate(cmd *cobra.Command, f *sinkFlags) error {
	cfg, err := f.decodeKafka(cmd)
	if err != nil {
		return err
	}
	resolved, err := validateKafka(cfg)
	if err != nil {
		return err
	}
	// Building the publisher validates broker list, SASL mechanism, and TLS
	// material (file reads + keypair load) without performing network I/O.
	pub, err := kafkasink.NewKafkaPublisher(resolved.Writer)
	if err != nil {
		return fmt.Errorf("kafka publisher: %w", err)
	}
	_ = pub.Close()

	fmt.Fprintln(cmd.OutOrStdout(), "ok: config, broker/auth material, and topic routing validated")
	return nil
}

// resolvedKafka is the validated, ready-to-run kafka sink configuration.
type resolvedKafka struct {
	Router        *kafkasink.Router
	Writer        kafkasink.WriterConfig
	BackoffBase   time.Duration
	BackoffMax    time.Duration
	ProbeInterval time.Duration
}

// deliveryModeDesc renders the active producer mode for the startup log.
func deliveryModeDesc(idempotent bool) string {
	if idempotent {
		return "idempotent"
	}
	return "at-least-once"
}

// resolveDeliveryMode maps the kafka.delivery_mode knob to the idempotent-producer
// flag: "" / "at-least-once" / "plain" → false (the default at-least-once
// producer), "idempotent" → true. Both modes still publish with acks=all.
func resolveDeliveryMode(mode string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "at-least-once", "plain":
		return false, nil
	case "idempotent":
		return true, nil
	default:
		return false, fmt.Errorf("kafka.delivery_mode=%q is not supported (want \"at-least-once\" or \"idempotent\")", mode)
	}
}

// validateKafka applies the cross-field invariants strict decoding cannot
// express and returns the resolved configuration so run can reuse it.
func validateKafka(cfg *config.KafkaFull) (resolvedKafka, error) {
	k := cfg.Kafka

	brokers := normalizeBrokers(k.Brokers)
	if len(brokers) == 0 {
		return resolvedKafka{}, fmt.Errorf("kafka.brokers is required (set [kafka].brokers or --brokers)")
	}
	if k.Topic == "" && len(k.TopicByType) == 0 {
		return resolvedKafka{}, fmt.Errorf("a default kafka.topic or a [kafka].topic_by_type mapping is required")
	}

	router, err := kafkasink.NewRouter(k.Topic, k.TopicByType, kafkasink.PartitionKeyMode(k.PartitionKey))
	if err != nil {
		return resolvedKafka{}, err
	}

	if acks := strings.ToLower(strings.TrimSpace(k.RequiredAcks)); acks != "" && acks != "all" {
		return resolvedKafka{}, fmt.Errorf("kafka.required_acks=%q is not supported; at-least-once delivery requires \"all\"", k.RequiredAcks)
	}
	idempotent, err := resolveDeliveryMode(k.DeliveryMode)
	if err != nil {
		return resolvedKafka{}, err
	}

	backoffBase, err := parseDurationDefault(k.BackoffBase, 500*time.Millisecond, "kafka.backoff_base")
	if err != nil {
		return resolvedKafka{}, err
	}
	backoffMax, err := parseDurationDefault(k.BackoffMax, 30*time.Second, "kafka.backoff_max")
	if err != nil {
		return resolvedKafka{}, err
	}
	batchTimeout, err := parseDurationDefault(k.BatchTimeout, 200*time.Millisecond, "kafka.batch_timeout")
	if err != nil {
		return resolvedKafka{}, err
	}
	probeInterval, err := parseProbeInterval(k.ReadinessProbeInterval, "kafka.readiness_probe_interval")
	if err != nil {
		return resolvedKafka{}, err
	}

	// SASL must run over TLS; default TLS on when a mechanism is set unless the
	// operator explicitly turned it off (a deliberate, flagged decision).
	tlsEnabled := false
	if k.TLS.Enabled != nil {
		tlsEnabled = *k.TLS.Enabled
	} else if strings.TrimSpace(k.SASL.Mechanism) != "" {
		tlsEnabled = true
	}

	wc := kafkasink.WriterConfig{
		Brokers:               brokers,
		BatchTimeout:          batchTimeout,
		SASLMechanism:         k.SASL.Mechanism,
		SASLUsername:          k.SASL.Username,
		SASLPassword:          k.SASL.Password,
		TLSEnabled:            tlsEnabled,
		TLSCACert:             k.TLS.CACert,
		TLSClientCert:         k.TLS.ClientCert,
		TLSClientKey:          k.TLS.ClientKey,
		TLSServerName:         k.TLS.ServerName,
		TLSInsecureSkipVerify: k.TLS.InsecureSkipVerify,
		DialTimeout:           10 * time.Second,
		Topics:                topicSet(k.Topic, k.TopicByType),
		Idempotent:            idempotent,
	}

	return resolvedKafka{
		Router:        router,
		Writer:        wc,
		BackoffBase:   backoffBase,
		BackoffMax:    backoffMax,
		ProbeInterval: probeInterval,
	}, nil
}

// decodeKafka loads and strict-decodes the evm-sink-kafka config.
func (f *sinkFlags) decodeKafka(cmd *cobra.Command) (*config.KafkaFull, error) {
	loader, err := f.loadConfig(cmd)
	if err != nil {
		return nil, err
	}
	return loader.DecodeKafka(f.allowExecEnabled())
}

// kafkaMetricsConfig resolves the metrics endpoint config for evm-sink-kafka.
func (f *sinkFlags) kafkaMetricsConfig(cmd *cobra.Command, cfg *config.KafkaFull) resolvedMetrics {
	cf := commandFlags{
		metricsChanged:     cmd.Flags().Changed("metrics"),
		metricsAddrChanged: cmd.Flags().Changed("metrics-addr"),
		metricsPathChanged: cmd.Flags().Changed("metrics-path"),
	}
	return f.resolveSinkMetrics(cf, cfg.Metrics, cfg.Kafka.Metrics, ":9002")
}

// normalizeBrokers trims and splits broker entries, accepting both a TOML list
// and a single comma-separated string (from --brokers or an env override).
func normalizeBrokers(in []string) []string {
	out := make([]string, 0, len(in))
	for _, b := range in {
		for _, part := range strings.Split(b, ",") {
			if p := strings.TrimSpace(part); p != "" {
				out = append(out, p)
			}
		}
	}
	return out
}

// redactBrokers returns a log-safe broker summary: host:port pairs carry no
// secret, but we cap the logged count to keep the line bounded.
func redactBrokers(brokers []string) string {
	if len(brokers) <= 3 {
		return strings.Join(brokers, ",")
	}
	return fmt.Sprintf("%s,... (%d total)", strings.Join(brokers[:3], ","), len(brokers))
}

// partitionKeyDesc returns the effective partition-key mode for logging, falling
// back to the default when unset.
func partitionKeyDesc(mode string) string {
	if strings.TrimSpace(mode) == "" {
		return string(kafkasink.PartitionIdentity)
	}
	return mode
}

// parseDurationDefault parses a duration string, returning def when it is empty.
func parseDurationDefault(s string, def time.Duration, name string) (time.Duration, error) {
	if strings.TrimSpace(s) == "" {
		return def, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid duration %q: %w", name, s, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s: duration must be positive, got %q", name, s)
	}
	return d, nil
}

// parseProbeInterval parses the readiness-probe interval. Empty uses def;
// "0"/"0s"/"off"/"none"/"disabled" returns 0 (probe disabled); anything else
// must be a positive duration.
func parseProbeInterval(s, name string) (time.Duration, error) {
	// defaultProbeInterval is the readiness-probe cadence used by every sink when
	// the key is unset; the disable spellings turn the probe off.
	const defaultProbeInterval = 15 * time.Second
	t := strings.ToLower(strings.TrimSpace(s))
	switch t {
	case "":
		return defaultProbeInterval, nil
	case "0", "0s", "off", "none", "disabled":
		return 0, nil
	}
	d, err := time.ParseDuration(t)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid duration %q: %w", name, s, err)
	}
	if d <= 0 {
		return 0, nil
	}
	return d, nil
}

// parseDisableableDuration parses an optional duration that defaults to disabled:
// "" / "0" / "0s" / "off" / "none" / "disabled" return 0 (feature off); anything
// else must be a positive duration. Used for opt-in knobs such as the head-
// staleness threshold, the per-target read timeout, and the redis dedup TTL.
func parseDisableableDuration(s, name string) (time.Duration, error) {
	t := strings.ToLower(strings.TrimSpace(s))
	switch t {
	case "", "0", "0s", "off", "none", "disabled":
		return 0, nil
	}
	d, err := time.ParseDuration(t)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid duration %q: %w", name, s, err)
	}
	// A negative duration is never a legitimate "disable" spelling — it is a typo
	// for a positive value, and silently treating it as disabled would quietly turn
	// off the safety knob the operator meant to enable. Reject it (matching
	// parseRotationInterval / parseDurationDefault); 0 is reachable only via the
	// explicit disable spellings above.
	if d < 0 {
		return 0, fmt.Errorf("%s: duration must be positive or a disable spelling (\"\"/\"0\"/\"off\"), got %q", name, s)
	}
	return d, nil
}

// topicSet returns the deduped, non-empty set of topics the sink may write to:
// the default topic plus every per-type override. Used to scope the readiness
// probe's metadata request to exactly what the sink produces to.
func topicSet(defaultTopic string, byType map[string]string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(t string) {
		t = strings.TrimSpace(t)
		if t != "" && !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	add(defaultTopic)
	for _, t := range byType {
		add(t)
	}
	return out
}
