package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// EnvPrefix is the binding prefix for environment overrides, e.g.
// EVM_TOOLS_RPC_URL binds rpc.url.
const EnvPrefix = "EVM_TOOLS"

// searchPaths returns the directories searched, in order, for the config file
// when --config is not given; the first one containing an evm-tools config file
// wins. A home-directory ~/.evm-tools takes precedence, then the OS/XDG user
// config directory, then a host-level /etc location. It is computed per call (not
// a package var) so a HOME set after process start — common in containers — is
// honored.
func searchPaths() []string {
	return []string{
		filepath.Join(homeDir(), ".evm-tools"),
		filepath.Join(userConfigDir(), "evm-tools"),
		"/etc/evm-tools",
	}
}

// configBaseNames are the file stems searched within each directory, in
// preference order: config.toml is the primary name (so the discovered file is
// e.g. ~/.evm-tools/config.toml), and evm-tools.toml is accepted as a
// backward-compatible fallback. Within a directory config.toml wins; across
// directories the searchPaths() order wins, so a home/user config beats a
// host-level /etc config regardless of which filename each uses.
var configBaseNames = []string{"config", "evm-tools"}

// configExt is the config file extension (TOML).
const configExt = "toml"

// Loader assembles a *viper.Viper with the suite's precedence rules wired up.
// It is shared by both CLIs; each then strict-decodes its own subtree.
type Loader struct {
	v *viper.Viper
	// flagKeys is the set of dotted config keys overridden by a flag the user
	// actually changed. Together with the environment, it identifies values that
	// outrank the file — the only ones that short-circuit a _cmd or are exempt
	// from ${...} interpolation (which applies to file-sourced values only).
	flagKeys map[string]bool
}

// Options controls how the config is sourced.
type Options struct {
	// ConfigFile, when set, is loaded explicitly (from --config/-c). When
	// empty, the default search paths are used and a missing file is not fatal
	// (flags/env/defaults still apply).
	ConfigFile string
	// Flags, when non-nil, is the command's flag set; defined flags that the
	// user actually changed are bound at the top of the precedence order.
	Flags *pflag.FlagSet
}

// flagBindings maps a pflag name to the dotted config key it overrides. This is
// the explicit wiring the design calls for: env/flag binding to nested keys is
// never automatic.
var flagBindings = map[string]string{
	"chain":            "chain",
	"rpc-url":          "rpc.url",
	"rpc-client-cert":  "rpc.client_cert",
	"rpc-client-key":   "rpc.client_key",
	"rpc-ca-cert":      "rpc.ca_cert",
	"rpc-server-name":  "rpc.server_name",
	"rpc-require-mtls": "rpc.require_mtls",
	"log-level":        "log.level",
	"log-format":       "log.format",

	// evm-stream scalar flags (the additive --contract/--events/--native-transfers
	// are merged separately in applyStreamFlags, not bound here).
	"from-block":    "stream.from_block",
	"poll-interval": "stream.poll_interval",

	// evm-balance scalar cadence flags (the additive --native/--erc20 are merged
	// separately in applyBalanceFlags, not bound here).
	"interval":     "balance.interval",
	"every-blocks": "balance.every_blocks",

	// evm-sink-kafka flags.
	"brokers": "kafka.brokers",
	"topic":   "kafka.topic",

	// evm-sink-webhook flags.
	"url": "webhook.url",

	// evm-sink-file flags.
	"path": "file.path",

	// evm-sink-aws-sqs / evm-sink-aws-sns flags.
	"queue-url": "aws_sqs.queue_url",
	"topic-arn": "aws_sns.topic_arn",

	// evm-sink-postgres flag (dsn is secret -> config/env only, never argv).
	"table": "postgres.table",

	// evm-sink-redis flag (url is secret -> config/env only, never argv).
	"stream": "redis.stream",
}

// New builds a Loader, reading the config file (if any) and wiring env binding.
// It does not decode into a tool's struct — call DecodeStream or DecodeBalance.
func New(opts Options) (*Loader, error) {
	v := viper.New()
	v.SetConfigType("toml")
	setDefaults(v)

	// Environment binding: EVM_TOOLS_ prefix with a "." -> "_" key replacer so
	// nested keys such as rpc.url bind from EVM_TOOLS_RPC_URL.
	v.SetEnvPrefix(EnvPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	bindEnvKeys(v)

	if err := readConfigFile(v, opts.ConfigFile); err != nil {
		return nil, err
	}

	// Flags sit at the top of precedence. Bind only flags the user changed so
	// a flag's zero default never silently overrides file/env values.
	flagKeys := map[string]bool{}
	if opts.Flags != nil {
		if err := bindChangedFlags(v, opts.Flags, flagKeys); err != nil {
			return nil, err
		}
	}

	return &Loader{v: v, flagKeys: flagKeys}, nil
}

// readConfigFile loads an explicit file or the first discovered default file. A
// missing explicit file is fatal; a missing default file is not (flags/env/
// defaults still apply).
func readConfigFile(v *viper.Viper, explicit string) error {
	if explicit != "" {
		v.SetConfigFile(explicit)
		if err := v.ReadInConfig(); err != nil {
			return fmt.Errorf("read config %q: %w", explicit, err)
		}
		return nil
	}

	path, ok := findDefaultConfig()
	if !ok {
		// No default file is fine: flags/env/defaults still apply.
		return nil
	}
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("read config %q: %w", path, err)
	}
	return nil
}

// findDefaultConfig returns the first existing default config file. It walks the
// search directories in order and, within each, the preferred base names in
// order (config.toml before evm-tools.toml), so directory precedence dominates
// and config.toml wins a tie within one directory.
func findDefaultConfig() (string, bool) {
	for _, dir := range searchPaths() {
		for _, name := range configBaseNames {
			p := filepath.Join(dir, name+"."+configExt)
			if fi, err := os.Stat(p); err == nil && !fi.IsDir() {
				return p, true
			}
		}
	}
	return "", false
}

// bindEnvKeys explicitly binds the nested keys we want overridable from the
// environment. AutomaticEnv handles top-level keys, but nested keys are bound
// explicitly so the mapping is deliberate and documented.
func bindEnvKeys(v *viper.Viper) {
	keys := []string{
		"chain",
		"rpc.url", "rpc.client_cert", "rpc.client_key", "rpc.ca_cert", "rpc.server_name",
		"rpc.require_mtls",
		"metrics.enabled", "metrics.addr", "metrics.path",
		"log.level", "log.format",
		"stream.from_block", "stream.poll_interval", "stream.log_chunk_blocks",
		"stream.reorg_depth", "stream.head_staleness_threshold", "stream.checkpoint_file",
		"stream.metrics.enabled", "stream.metrics.addr", "stream.metrics.path",
		"balance.interval", "balance.every_blocks",
		"balance.max_concurrency", "balance.target_timeout", "balance.head_staleness_threshold",
		"balance.metrics.enabled", "balance.metrics.addr", "balance.metrics.path",
		"kafka.brokers", "kafka.topic", "kafka.partition_key", "kafka.required_acks",
		"kafka.readiness_probe_interval",
		"kafka.sasl.mechanism", "kafka.sasl.username", "kafka.sasl.password",
		"kafka.tls.enabled", "kafka.tls.ca_cert", "kafka.tls.client_cert",
		"kafka.tls.client_key", "kafka.tls.server_name",
		"kafka.metrics.enabled", "kafka.metrics.addr", "kafka.metrics.path",
		"webhook.url", "webhook.method", "webhook.timeout",
		"webhook.backoff_base", "webhook.backoff_max",
		"webhook.health_url", "webhook.readiness_probe_interval",
		"webhook.auth.header", "webhook.auth.value",
		"webhook.metrics.enabled", "webhook.metrics.addr", "webhook.metrics.path",
		"file.path", "file.max_size_mb", "file.rotation_interval", "file.max_backups",
		"file.compress", "file.fsync", "file.backoff_base", "file.backoff_max",
		"file.metrics.enabled", "file.metrics.addr", "file.metrics.path",
		"aws_sqs.queue_url", "aws_sqs.region", "aws_sqs.endpoint_url",
		"aws_sqs.backoff_base", "aws_sqs.backoff_max", "aws_sqs.readiness_probe_interval",
		"aws_sqs.metrics.enabled", "aws_sqs.metrics.addr", "aws_sqs.metrics.path",
		"aws_sns.topic_arn", "aws_sns.region", "aws_sns.endpoint_url",
		"aws_sns.backoff_base", "aws_sns.backoff_max", "aws_sns.readiness_probe_interval",
		"aws_sns.metrics.enabled", "aws_sns.metrics.addr", "aws_sns.metrics.path",
		"postgres.dsn", "postgres.table", "postgres.create_table",
		"postgres.backoff_base", "postgres.backoff_max", "postgres.readiness_probe_interval",
		"postgres.metrics.enabled", "postgres.metrics.addr", "postgres.metrics.path",
		"redis.url", "redis.stream", "redis.field", "redis.max_len",
		"redis.dedup", "redis.dedup_ttl",
		"redis.backoff_base", "redis.backoff_max", "redis.readiness_probe_interval",
		"redis.metrics.enabled", "redis.metrics.addr", "redis.metrics.path",
	}
	for _, k := range keys {
		// Error only occurs with an empty key; ignore safely.
		_ = v.BindEnv(k)
	}
}

// bindChangedFlags binds every flag the user explicitly set to its dotted key
// and records that key in flagKeys so later resolution can tell a flag override
// apart from a built-in default.
func bindChangedFlags(v *viper.Viper, fs *pflag.FlagSet, flagKeys map[string]bool) error {
	var bindErr error
	fs.Visit(func(f *pflag.Flag) {
		if bindErr != nil {
			return
		}
		key, ok := flagBindings[f.Name]
		if !ok {
			return
		}
		if err := v.BindPFlag(key, f); err != nil {
			bindErr = fmt.Errorf("bind flag --%s: %w", f.Name, err)
			return
		}
		flagKeys[key] = true
	})
	return bindErr
}

// setDefaults installs built-in defaults — the lowest precedence tier.
func setDefaults(v *viper.Viper) {
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "text")
	v.SetDefault("metrics.enabled", false)
	v.SetDefault("metrics.path", "/metrics")

	v.SetDefault("stream.poll_interval", "2s")
	v.SetDefault("stream.log_chunk_blocks", 2000)
	v.SetDefault("stream.from_block", "latest")
	v.SetDefault("stream.reorg_depth", 64)
	v.SetDefault("stream.metrics.path", "/metrics")
	v.SetDefault("stream.metrics.addr", ":9000")

	v.SetDefault("balance.metrics.path", "/metrics")
	v.SetDefault("balance.metrics.addr", ":9001")

	v.SetDefault("kafka.partition_key", "identity")
	v.SetDefault("kafka.required_acks", "all")
	v.SetDefault("kafka.backoff_base", "500ms")
	v.SetDefault("kafka.backoff_max", "30s")
	v.SetDefault("kafka.batch_timeout", "200ms")
	v.SetDefault("kafka.readiness_probe_interval", "15s")

	v.SetDefault("webhook.readiness_probe_interval", "15s")
	v.SetDefault("kafka.metrics.path", "/metrics")
	v.SetDefault("kafka.metrics.addr", ":9002")

	v.SetDefault("webhook.method", "POST")
	v.SetDefault("webhook.timeout", "10s")
	v.SetDefault("webhook.backoff_base", "500ms")
	v.SetDefault("webhook.backoff_max", "30s")
	v.SetDefault("webhook.metrics.path", "/metrics")
	v.SetDefault("webhook.metrics.addr", ":9003")

	v.SetDefault("file.backoff_base", "500ms")
	v.SetDefault("file.backoff_max", "30s")
	v.SetDefault("file.metrics.path", "/metrics")
	v.SetDefault("file.metrics.addr", ":9004")

	v.SetDefault("aws_sqs.backoff_base", "500ms")
	v.SetDefault("aws_sqs.backoff_max", "30s")
	v.SetDefault("aws_sqs.readiness_probe_interval", "15s")
	v.SetDefault("aws_sqs.metrics.path", "/metrics")
	v.SetDefault("aws_sqs.metrics.addr", ":9005")

	v.SetDefault("aws_sns.backoff_base", "500ms")
	v.SetDefault("aws_sns.backoff_max", "30s")
	v.SetDefault("aws_sns.readiness_probe_interval", "15s")
	v.SetDefault("aws_sns.metrics.path", "/metrics")
	v.SetDefault("aws_sns.metrics.addr", ":9006")

	v.SetDefault("postgres.table", "evm_records")
	v.SetDefault("postgres.backoff_base", "500ms")
	v.SetDefault("postgres.backoff_max", "30s")
	v.SetDefault("postgres.readiness_probe_interval", "15s")
	v.SetDefault("postgres.metrics.path", "/metrics")
	v.SetDefault("postgres.metrics.addr", ":9007")

	v.SetDefault("redis.field", "data")
	v.SetDefault("redis.dedup", true)
	v.SetDefault("redis.backoff_base", "500ms")
	v.SetDefault("redis.backoff_max", "30s")
	v.SetDefault("redis.readiness_probe_interval", "15s")
	v.SetDefault("redis.metrics.path", "/metrics")
	v.SetDefault("redis.metrics.addr", ":9008")
}

// userConfigDir returns the user-level config directory, defaulting to
// ~/.config when os.UserConfigDir fails.
func userConfigDir() string {
	if d, err := os.UserConfigDir(); err == nil {
		return d
	}
	return filepath.Join(os.Getenv("HOME"), ".config")
}

// homeDir returns the user's home directory, defaulting to $HOME when
// os.UserHomeDir fails.
func homeDir() string {
	if d, err := os.UserHomeDir(); err == nil {
		return d
	}
	return os.Getenv("HOME")
}
