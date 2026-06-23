package cli

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/daxchain-io/evm-tools/internal/awssink"
	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/metrics"
	"github.com/daxchain-io/evm-tools/internal/record"
)

// resolvedAWS holds the parsed, validated AWS-sink runtime parameters shared by
// the SQS and SNS sinks.
type resolvedAWS struct {
	Client        awssink.ClientConfig
	FIFO          bool
	BackoffBase   time.Duration
	BackoffMax    time.Duration
	ProbeInterval time.Duration
}

// resolveAWSCommon parses the shared [aws_*] settings (durations, probe interval)
// without any network I/O, so it is safe for `validate`.
func resolveAWSCommon(c config.AWSCommon, fifo bool) (resolvedAWS, error) {
	bb, err := parseDurationDefault(c.BackoffBase, 500*time.Millisecond, "backoff_base")
	if err != nil {
		return resolvedAWS{}, err
	}
	bm, err := parseDurationDefault(c.BackoffMax, 30*time.Second, "backoff_max")
	if err != nil {
		return resolvedAWS{}, err
	}
	probe, err := parseProbeInterval(c.ReadinessProbeInterval, "readiness_probe_interval")
	if err != nil {
		return resolvedAWS{}, err
	}
	return resolvedAWS{
		Client:        awssink.ClientConfig{Region: c.Region, EndpointURL: c.EndpointURL},
		FIFO:          fifo,
		BackoffBase:   bb,
		BackoffMax:    bm,
		ProbeInterval: probe,
	}, nil
}

// awsSQSRun implements `evm-sink-aws-sqs run`.
func awsSQSRun(cmd *cobra.Command, f *sinkFlags) error {
	cfg, err := f.decodeAWSSQS(cmd)
	if err != nil {
		return err
	}
	r, err := validateAWSSQS(cfg)
	if err != nil {
		return err
	}
	awsCfg, err := awssink.LoadAWSConfig(cmd.Context(), r.Client)
	if err != nil {
		return err
	}
	pub, err := awssink.NewSQSPublisher(awsCfg, cfg.AWSSQS.QueueURL, r.FIFO)
	if err != nil {
		return err
	}
	return runAWSSink(cmd, f, "evm_sink_aws_sqs", ":9005", cfg.Chain, string(ToolSinkAWSSQS), cfg.Input, cfg.DeadLetterFile, cfg.Metrics, cfg.AWSSQS.Metrics, pub, r)
}

// awsSNSRun implements `evm-sink-aws-sns run`.
func awsSNSRun(cmd *cobra.Command, f *sinkFlags) error {
	cfg, err := f.decodeAWSSNS(cmd)
	if err != nil {
		return err
	}
	r, err := validateAWSSNS(cfg)
	if err != nil {
		return err
	}
	awsCfg, err := awssink.LoadAWSConfig(cmd.Context(), r.Client)
	if err != nil {
		return err
	}
	pub, err := awssink.NewSNSPublisher(awsCfg, cfg.AWSSNS.TopicARN, r.FIFO)
	if err != nil {
		return err
	}
	return runAWSSink(cmd, f, "evm_sink_aws_sns", ":9006", cfg.Chain, string(ToolSinkAWSSNS), cfg.Input, cfg.DeadLetterFile, cfg.Metrics, cfg.AWSSNS.Metrics, pub, r)
}

// runAWSSink wires metrics/health/server around the shared awssink loop and runs
// it until EOF or signal, then shuts down gracefully.
func runAWSSink(cmd *cobra.Command, f *sinkFlags, namespace, defaultAddr, chainName, sinkName, cfgInput, cfgDeadLetter string,
	sharedM, toolM config.MetricsConfig, pub awssink.Publisher, r resolvedAWS,
) error {
	logger := slog.Default()

	m := metrics.NewSinkMetrics(namespace, chainName, "")
	m.SetUp(true)

	healthBase := metrics.NewHealth(readyEmitBlockedThreshold, 0) // lag disabled for a sink.
	health := metrics.NewSinkHealth(healthBase)

	cf := commandFlags{
		metricsChanged:     cmd.Flags().Changed("metrics"),
		metricsAddrChanged: cmd.Flags().Changed("metrics-addr"),
		metricsPathChanged: cmd.Flags().Changed("metrics-path"),
	}
	mc := f.resolveSinkMetrics(cf, sharedM, toolM, defaultAddr)
	srv, err := metrics.NewServer(metrics.ServerOptions{
		Addr:           mc.Addr,
		MetricsEnabled: mc.Enabled,
		MetricsPath:    mc.Path,
		Registry:       m.Registry(),
		Health:         healthBase,
	})
	if err != nil {
		_ = pub.Close()
		return err
	}
	go func() {
		if serveErr := srv.Serve(); serveErr != nil {
			logger.Error("metrics server stopped unexpectedly; marking process not-live", "error", serveErr)
			healthBase.SetLive(false)
		}
	}()
	logger.Info("health/metrics server listening", "addr", srv.Addr(), "metrics_enabled", mc.Enabled)
	logger.Info("aws sink started", "target", pub.Target(), "fifo", r.FIFO,
		"readiness_probe", r.ProbeInterval > 0)

	in, err := f.openInput(cmd, cfgInput)
	if err != nil {
		_ = pub.Close()
		return err
	}
	defer func() { _ = in.Close() }()
	reader := record.NewReader(in)
	dlq, err := f.installDeadLetter(cmd, reader, sinkName, cfgDeadLetter, m, logger)
	if err != nil {
		_ = pub.Close()
		return err
	}
	if dlq != nil {
		defer func() { _ = dlq.Close() }()
	}
	sink, err := awssink.New(awssink.Options{
		Reader:        reader,
		Publisher:     pub,
		Metrics:       m,
		Health:        health,
		Logger:        logger,
		FIFO:          r.FIFO,
		BackoffBase:   r.BackoffBase,
		BackoffMax:    r.BackoffMax,
		ProbeInterval: r.ProbeInterval,
	})
	if err != nil {
		_ = pub.Close()
		return err
	}

	m.SetWorkers(1)
	runErr := sink.Run(cmd.Context())

	m.SetWorkers(0)
	m.SetUp(false)
	if closeErr := pub.Close(); closeErr != nil {
		logger.Warn("aws publisher close", "error", closeErr)
	}
	shutCtx, cancelShut := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancelShut()
	if shutErr := srv.Shutdown(shutCtx); shutErr != nil {
		logger.Warn("metrics server shutdown", "error", shutErr)
	}
	return runErr
}

// validateAWSSQS / validateAWSSNS check required fields and parse the shared
// settings without any network I/O (no credential resolution), so `validate`
// stays offline-safe.
func validateAWSSQS(cfg *config.AWSSQSFull) (resolvedAWS, error) {
	if strings.TrimSpace(cfg.AWSSQS.QueueURL) == "" {
		return resolvedAWS{}, fmt.Errorf("aws_sqs.queue_url is required (set [aws_sqs].queue_url or --queue-url)")
	}
	fifo := strings.HasSuffix(cfg.AWSSQS.QueueURL, ".fifo")
	r, err := resolveAWSCommon(cfg.AWSSQS.AWSCommon, fifo)
	if err != nil {
		return resolvedAWS{}, fmt.Errorf("aws_sqs: %w", err)
	}
	return r, nil
}

func validateAWSSNS(cfg *config.AWSSNSFull) (resolvedAWS, error) {
	if strings.TrimSpace(cfg.AWSSNS.TopicARN) == "" {
		return resolvedAWS{}, fmt.Errorf("aws_sns.topic_arn is required (set [aws_sns].topic_arn or --topic-arn)")
	}
	fifo := strings.HasSuffix(cfg.AWSSNS.TopicARN, ".fifo")
	r, err := resolveAWSCommon(cfg.AWSSNS.AWSCommon, fifo)
	if err != nil {
		return resolvedAWS{}, fmt.Errorf("aws_sns: %w", err)
	}
	return r, nil
}

// awsSQSValidate / awsSNSValidate implement the `validate` subcommand.
func awsSQSValidate(cmd *cobra.Command, f *sinkFlags) error {
	cfg, err := f.decodeAWSSQS(cmd)
	if err != nil {
		return err
	}
	if _, err := validateAWSSQS(cfg); err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), "ok: config and aws_sqs settings validated (credentials resolved at run time via the AWS default chain)")
	return nil
}

func awsSNSValidate(cmd *cobra.Command, f *sinkFlags) error {
	cfg, err := f.decodeAWSSNS(cmd)
	if err != nil {
		return err
	}
	if _, err := validateAWSSNS(cfg); err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), "ok: config and aws_sns settings validated (credentials resolved at run time via the AWS default chain)")
	return nil
}

func (f *sinkFlags) decodeAWSSQS(cmd *cobra.Command) (*config.AWSSQSFull, error) {
	loader, err := f.loadConfig(cmd)
	if err != nil {
		return nil, err
	}
	return loader.DecodeAWSSQS(f.allowExecEnabled())
}

func (f *sinkFlags) decodeAWSSNS(cmd *cobra.Command) (*config.AWSSNSFull, error) {
	loader, err := f.loadConfig(cmd)
	if err != nil {
		return nil, err
	}
	return loader.DecodeAWSSNS(f.allowExecEnabled())
}
