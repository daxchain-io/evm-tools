package config

import (
	"errors"
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

// Default config file locations searched when --config is not given. A
// user-level workstation config takes precedence over a host-level one.
var defaultSearchPaths = []string{
	filepath.Join(userConfigDir(), "evm-tools"),
	"/etc/evm-tools",
}

// configBaseName is the file stem searched for (e.g. evm-tools.toml).
const configBaseName = "evm-tools"

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
	"rpc-url":         "rpc.url",
	"rpc-client-cert": "rpc.client_cert",
	"rpc-client-key":  "rpc.client_key",
	"rpc-ca-cert":     "rpc.ca_cert",
	"rpc-server-name": "rpc.server_name",
	"log-level":       "log.level",
	"log-format":      "log.format",

	// evm-sink-kafka flags.
	"brokers": "kafka.brokers",
	"topic":   "kafka.topic",
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

// readConfigFile loads an explicit file or searches the default paths. A
// missing explicit file is fatal; a missing default file is not.
func readConfigFile(v *viper.Viper, explicit string) error {
	if explicit != "" {
		v.SetConfigFile(explicit)
		if err := v.ReadInConfig(); err != nil {
			return fmt.Errorf("read config %q: %w", explicit, err)
		}
		return nil
	}

	v.SetConfigName(configBaseName)
	for _, p := range defaultSearchPaths {
		v.AddConfigPath(p)
	}
	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if errors.As(err, &notFound) {
			// No default file is fine: flags/env/defaults still apply.
			return nil
		}
		return fmt.Errorf("read config: %w", err)
	}
	return nil
}

// bindEnvKeys explicitly binds the nested keys we want overridable from the
// environment. AutomaticEnv handles top-level keys, but nested keys are bound
// explicitly so the mapping is deliberate and documented.
func bindEnvKeys(v *viper.Viper) {
	keys := []string{
		"chain",
		"rpc.url", "rpc.client_cert", "rpc.client_key", "rpc.ca_cert", "rpc.server_name",
		"metrics.enabled", "metrics.addr", "metrics.path",
		"log.level", "log.format",
		"stream.metrics.enabled", "stream.metrics.addr", "stream.metrics.path",
		"balance.metrics.enabled", "balance.metrics.addr", "balance.metrics.path",
		"kafka.brokers", "kafka.topic", "kafka.partition_key", "kafka.required_acks",
		"kafka.sasl.mechanism", "kafka.sasl.username", "kafka.sasl.password",
		"kafka.tls.enabled", "kafka.tls.ca_cert", "kafka.tls.client_cert",
		"kafka.tls.client_key", "kafka.tls.server_name",
		"kafka.metrics.enabled", "kafka.metrics.addr", "kafka.metrics.path",
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
	v.SetDefault("stream.metrics.path", "/metrics")
	v.SetDefault("stream.metrics.addr", ":9000")

	v.SetDefault("balance.metrics.path", "/metrics")
	v.SetDefault("balance.metrics.addr", ":9001")

	v.SetDefault("kafka.partition_key", "identity")
	v.SetDefault("kafka.required_acks", "all")
	v.SetDefault("kafka.backoff_base", "500ms")
	v.SetDefault("kafka.backoff_max", "30s")
	v.SetDefault("kafka.batch_timeout", "200ms")
	v.SetDefault("kafka.metrics.path", "/metrics")
	v.SetDefault("kafka.metrics.addr", ":9002")
}

// userConfigDir returns the user-level config directory, defaulting to
// ~/.config when os.UserConfigDir fails.
func userConfigDir() string {
	if d, err := os.UserConfigDir(); err == nil {
		return d
	}
	return filepath.Join(os.Getenv("HOME"), ".config")
}
