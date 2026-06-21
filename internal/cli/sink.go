package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/logging"
)

// SinkTool identifies which sink a command tree is for. Sinks read JSONL on
// stdin via the record contract and deliver it downstream; they share the
// shared [metrics]/[log] config and flags but not the producers' [rpc]/[chain]
// surface, so they get their own thin command tree rather than the
// producer-shaped one in cli.go.
type SinkTool string

// Supported sinks.
const (
	ToolSinkKafka    SinkTool = "evm-sink-kafka"
	ToolSinkWebhook  SinkTool = "evm-sink-webhook"
	ToolSinkFile     SinkTool = "evm-sink-file"
	ToolSinkAWSSQS   SinkTool = "evm-sink-aws-sqs"
	ToolSinkAWSSNS   SinkTool = "evm-sink-aws-sns"
	ToolSinkPostgres SinkTool = "evm-sink-postgres"
	ToolSinkRedis    SinkTool = "evm-sink-redis"
)

// sinkFlags holds the values bound to a sink's persistent flag set: the shared
// metrics/log/config flags plus the sink-specific flags. A sink has no RPC
// surface, so the --rpc-* flags are absent.
type sinkFlags struct {
	configFile string

	metricsEnabled bool
	metricsAddr    string
	metricsPath    string

	logLevel  string
	logFormat string

	allowExec bool

	// Kafka-specific.
	brokers []string
	topic   string

	// Webhook-specific.
	url string

	// File-specific.
	path string

	// AWS-specific.
	queueURL string
	topicARN string

	// Postgres-specific (dsn is secret -> config/env only, never a flag).
	table string

	// Redis-specific (url is secret -> config/env only, never a flag).
	stream string
}

// sinkShortDesc returns the one-line description for a sink.
func (t SinkTool) sinkShortDesc() string {
	switch t {
	case ToolSinkKafka:
		return "Publish JSONL records from stdin to Kafka topics (at-least-once)"
	case ToolSinkWebhook:
		return "Forward JSONL records from stdin to an HTTP endpoint (at-least-once, optional filters)"
	case ToolSinkFile:
		return "Append JSONL records from stdin to a rotating local file (at-least-once, optional filters)"
	case ToolSinkAWSSQS:
		return "Send JSONL records from stdin to an AWS SQS queue (at-least-once, FIFO-aware)"
	case ToolSinkAWSSNS:
		return "Publish JSONL records from stdin to an AWS SNS topic (at-least-once, FIFO-aware)"
	case ToolSinkPostgres:
		return "Insert JSONL records from stdin into a PostgreSQL table (idempotent, exactly-once-in-table)"
	case ToolSinkRedis:
		return "Append JSONL records from stdin to a Redis Stream (at-least-once, idempotent via dedup_key)"
	default:
		return "An evm-tools sink"
	}
}

// NewSinkRootCommand builds the command tree for a sink: run, validate, version.
// There is no `check rpc` — a sink has no RPC endpoint.
func NewSinkRootCommand(tool SinkTool) *cobra.Command {
	flags := &sinkFlags{}

	root := &cobra.Command{
		Use:           string(tool),
		Short:         tool.sinkShortDesc(),
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}

	bindSinkSharedFlags(root, flags)
	switch tool {
	case ToolSinkKafka:
		bindKafkaFlags(root, flags)
	case ToolSinkWebhook:
		bindWebhookFlags(root, flags)
	case ToolSinkFile:
		bindFileFlags(root, flags)
	case ToolSinkAWSSQS:
		bindAWSSQSFlags(root, flags)
	case ToolSinkAWSSNS:
		bindAWSSNSFlags(root, flags)
	case ToolSinkPostgres:
		bindPostgresFlags(root, flags)
	case ToolSinkRedis:
		bindRedisFlags(root, flags)
	}

	root.AddCommand(
		newSinkRunCommand(tool, flags),
		newSinkValidateCommand(tool, flags),
		newVersionCommand(),
	)
	return root
}

// bindSinkSharedFlags installs the shared (non-RPC) persistent flags every sink
// inherits.
func bindSinkSharedFlags(root *cobra.Command, f *sinkFlags) {
	pf := root.PersistentFlags()

	pf.StringVarP(&f.configFile, "config", "c", "", "path to the evm-tools TOML config file")

	pf.BoolVar(&f.metricsEnabled, "metrics", false, "enable the Prometheus metrics endpoint")
	pf.StringVar(&f.metricsAddr, "metrics-addr", "", "metrics bind address, e.g. :9002")
	pf.StringVar(&f.metricsPath, "metrics-path", "", "metrics route, e.g. /metrics")

	pf.StringVar(&f.logLevel, "log-level", "info", "log level: debug|info|warn|error")
	pf.StringVar(&f.logFormat, "log-format", "text", "log format: text|json")

	pf.BoolVar(&f.allowExec, "allow-exec", false, "allow config _cmd keys to execute (also EVM_TOOLS_ALLOW_EXEC=1)")
}

// bindKafkaFlags installs the evm-sink-kafka-specific flags.
func bindKafkaFlags(root *cobra.Command, f *sinkFlags) {
	pf := root.PersistentFlags()
	pf.StringSliceVar(&f.brokers, "brokers", nil, "comma-separated Kafka broker addresses, e.g. host1:9092,host2:9092")
	pf.StringVar(&f.topic, "topic", "", "default Kafka topic to publish records to")
}

// bindWebhookFlags installs the evm-sink-webhook-specific flags.
func bindWebhookFlags(root *cobra.Command, f *sinkFlags) {
	pf := root.PersistentFlags()
	pf.StringVar(&f.url, "url", "", "webhook URL to POST each record to, e.g. https://hooks.example.com/evm")
}

// bindFileFlags installs the evm-sink-file-specific flags.
func bindFileFlags(root *cobra.Command, f *sinkFlags) {
	pf := root.PersistentFlags()
	pf.StringVar(&f.path, "path", "", "output file path, e.g. /var/log/evm-tools/events.jsonl")
}

// bindAWSSQSFlags installs the evm-sink-aws-sqs-specific flags.
func bindAWSSQSFlags(root *cobra.Command, f *sinkFlags) {
	pf := root.PersistentFlags()
	pf.StringVar(&f.queueURL, "queue-url", "", "SQS queue URL (a .fifo URL enables FIFO ordering/dedup)")
}

// bindAWSSNSFlags installs the evm-sink-aws-sns-specific flags.
func bindAWSSNSFlags(root *cobra.Command, f *sinkFlags) {
	pf := root.PersistentFlags()
	pf.StringVar(&f.topicARN, "topic-arn", "", "SNS topic ARN (a .fifo topic enables FIFO ordering/dedup)")
}

// bindPostgresFlags installs the evm-sink-postgres-specific flags. The DSN is a
// secret and is intentionally NOT a flag (it would leak via the process argv);
// set it through [postgres].dsn / dsn_cmd / ${VAR}.
func bindPostgresFlags(root *cobra.Command, f *sinkFlags) {
	pf := root.PersistentFlags()
	pf.StringVar(&f.table, "table", "", "destination table (default evm_records; may be schema.table)")
}

// bindRedisFlags installs the evm-sink-redis-specific flags. The connection URL is
// a secret and is intentionally NOT a flag (it would leak via the process argv);
// set it through [redis].url / url_cmd / ${VAR}.
func bindRedisFlags(root *cobra.Command, f *sinkFlags) {
	pf := root.PersistentFlags()
	pf.StringVar(&f.stream, "stream", "", "destination Redis Stream key")
}

func newSinkRunCommand(tool SinkTool, f *sinkFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Read JSONL from stdin and deliver each record downstream (at-least-once)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.setupLogging(); err != nil {
				return err
			}
			// Derive a signal-aware context so SIGINT/SIGTERM trigger a clean
			// shutdown (stop reading, flush/close the writer, stop the server); a
			// second signal force-exits a wedged shutdown.
			ctx, stop := signalContext(cmd.Context())
			defer stop()
			cmd.SetContext(ctx)

			switch tool {
			case ToolSinkKafka:
				return kafkaRun(cmd, f)
			case ToolSinkWebhook:
				return webhookRun(cmd, f)
			case ToolSinkFile:
				return fileRun(cmd, f)
			case ToolSinkAWSSQS:
				return awsSQSRun(cmd, f)
			case ToolSinkAWSSNS:
				return awsSNSRun(cmd, f)
			case ToolSinkPostgres:
				return postgresRun(cmd, f)
			case ToolSinkRedis:
				return redisRun(cmd, f)
			default:
				return fmt.Errorf("unknown sink %q", tool)
			}
		},
	}
}

func newSinkValidateCommand(tool SinkTool, f *sinkFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate config (and destination/auth material) without connecting",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.setupLogging(); err != nil {
				return err
			}
			switch tool {
			case ToolSinkKafka:
				return kafkaValidate(cmd, f)
			case ToolSinkWebhook:
				return webhookValidate(cmd, f)
			case ToolSinkFile:
				return fileValidate(cmd, f)
			case ToolSinkAWSSQS:
				return awsSQSValidate(cmd, f)
			case ToolSinkAWSSNS:
				return awsSNSValidate(cmd, f)
			case ToolSinkPostgres:
				return postgresValidate(cmd, f)
			case ToolSinkRedis:
				return redisValidate(cmd, f)
			default:
				return fmt.Errorf("unknown sink %q", tool)
			}
		},
	}
}

// setupLogging configures the slog default logger from the sink flags.
func (f *sinkFlags) setupLogging() error {
	_, err := logging.Setup(f.logLevel, f.logFormat)
	return err
}

// allowExecEnabled resolves --allow-exec with the EVM_TOOLS_ALLOW_EXEC=1 env
// fallback, mirroring the producer path.
func (f *sinkFlags) allowExecEnabled() bool {
	if f.allowExec {
		return true
	}
	return os.Getenv("EVM_TOOLS_ALLOW_EXEC") == "1"
}

// loadConfig builds the config loader with the sink's flag bindings wired in.
func (f *sinkFlags) loadConfig(cmd *cobra.Command) (*config.Loader, error) {
	return config.New(config.Options{
		ConfigFile: f.configFile,
		Flags:      cmd.Flags(),
	})
}
