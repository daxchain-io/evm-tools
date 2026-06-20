// Package config defines the typed configuration for every tool in the suite
// and the loader that assembles it from flags, environment variables, a TOML
// file, and built-in defaults.
//
// The shared chain/RPC/metrics settings live at the top level; each tool owns a
// namespaced section ([stream] for evm-stream, [balance] for evm-balance). A
// tool decodes only the shared keys plus its own subtree, so sibling-tool
// sections are ignored rather than rejected and one file serves the whole suite.
//
// Precedence (highest wins): flags > env (EVM_TOOLS_ prefix) > TOML > defaults.
//
// Value interpolation (${VAR}, ${VAR:-default}, $$) and command execution
// (_cmd keys, gated by AllowExec) are resolved during decoding — see resolve.go
// and docs/design.md "Value interpolation".
package config

// MetricsConfig is the shared/per-tool Prometheus endpoint configuration.
// Tool-specific sections ([stream.metrics], [balance.metrics]) override the
// shared [metrics] defaults.
//
// Enabled is a *bool so an unset tool-specific section (nil) is distinguishable
// from one that is present and explicitly false. That distinction lets a
// tool-specific [stream.metrics]/[balance.metrics] truly *override* the shared
// [metrics] section — including disabling an endpoint the shared section enabled
// — rather than only being able to turn it on. See [MetricsConfig.IsEnabled].
type MetricsConfig struct {
	Enabled *bool  `mapstructure:"enabled"`
	Addr    string `mapstructure:"addr"`
	Path    string `mapstructure:"path"`
}

// IsEnabled reports whether the section explicitly enables the endpoint. An
// unset section (nil) reports false; callers that need to distinguish unset from
// explicitly-false read the Enabled pointer directly.
func (m MetricsConfig) IsEnabled() bool {
	return m.Enabled != nil && *m.Enabled
}

// RPCConfig holds the shared HTTPS RPC transport settings. The same transport
// serves normal runs, balance polling, backfills, and health checks. HTTPS uses
// server-authenticated TLS by default; a client cert/key upgrades to mutual TLS
// and RequireMTLS makes a missing client cert fail fast for private endpoints.
type RPCConfig struct {
	URL        string `mapstructure:"url"`
	ClientCert string `mapstructure:"client_cert"`
	ClientKey  string `mapstructure:"client_key"`
	CACert     string `mapstructure:"ca_cert"`
	ServerName string `mapstructure:"server_name"`
	// RequireMTLS demands a client certificate/key for HTTPS endpoints (off by
	// default). Set it for private nodes that mandate client authentication.
	RequireMTLS bool `mapstructure:"require_mtls"`
}

// LogConfig controls the slog diagnostics emitted on stderr.
type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// Shared is the top-level configuration common to every tool.
type Shared struct {
	Chain   string        `mapstructure:"chain"`
	RPC     RPCConfig     `mapstructure:"rpc"`
	Metrics MetricsConfig `mapstructure:"metrics"`
	Log     LogConfig     `mapstructure:"log"`
	// AllowExec gates _cmd config keys. It is sourced from --allow-exec or
	// EVM_TOOLS_ALLOW_EXEC and is not a TOML key itself.
	AllowExec bool `mapstructure:"-"`
}

// StreamContract is one configured [[stream.contracts]] entry.
type StreamContract struct {
	Name       string            `mapstructure:"name"`
	Address    string            `mapstructure:"address"`
	Events     []string          `mapstructure:"events"`
	ABI        string            `mapstructure:"abi"`
	ABIFile    string            `mapstructure:"abi_file"`
	Signatures map[string]string `mapstructure:"signatures"`
}

// NativeTransfersConfig is the [stream.native_transfers] section.
type NativeTransfersConfig struct {
	Enabled         bool     `mapstructure:"enabled"`
	IncludeInternal bool     `mapstructure:"include_internal"`
	From            []string `mapstructure:"from"`
	To              []string `mapstructure:"to"`
}

// StreamConfig is the [stream] section for evm-stream.
type StreamConfig struct {
	FromBlock       string                `mapstructure:"from_block"`
	PollInterval    string                `mapstructure:"poll_interval"`
	LogChunkBlocks  int                   `mapstructure:"log_chunk_blocks"`
	Metrics         MetricsConfig         `mapstructure:"metrics"`
	Contracts       []StreamContract      `mapstructure:"contracts"`
	NativeTransfers NativeTransfersConfig `mapstructure:"native_transfers"`
}

// BalanceNative is one configured [[balance.native]] entry.
type BalanceNative struct {
	Name    string `mapstructure:"name"`
	Address string `mapstructure:"address"`
}

// BalanceERC20 is one configured [[balance.erc20]] entry.
type BalanceERC20 struct {
	Name     string `mapstructure:"name"`
	Token    string `mapstructure:"token"`
	Address  string `mapstructure:"address"`
	Decimals *int   `mapstructure:"decimals"`
}

// BalanceContract is one configured [[balance.contracts]] entry.
type BalanceContract struct {
	Name                      string `mapstructure:"name"`
	Address                   string `mapstructure:"address"`
	NativeBalance             bool   `mapstructure:"native_balance"`
	TokenSupply               bool   `mapstructure:"token_supply"`
	TransferCountWindowBlocks int    `mapstructure:"transfer_count_window_blocks"`
	Decimals                  *int   `mapstructure:"decimals"`
}

// BalanceERC721Balances is one configured [[balance.erc721_balances]] entry.
type BalanceERC721Balances struct {
	Name  string `mapstructure:"name"`
	Token string `mapstructure:"token"`
	Owner string `mapstructure:"owner"`
	Mode  string `mapstructure:"mode"`
}

// BalanceERC721Ownership is one configured [[balance.erc721_ownership]] entry.
type BalanceERC721Ownership struct {
	Name    string `mapstructure:"name"`
	Token   string `mapstructure:"token"`
	TokenID string `mapstructure:"token_id"`
}

// BalanceConfig is the [balance] section for evm-balance. Sampling cadence is
// either Interval or EveryBlocks; exactly one must be set (validated later).
type BalanceConfig struct {
	Interval        string                   `mapstructure:"interval"`
	EveryBlocks     int                      `mapstructure:"every_blocks"`
	Metrics         MetricsConfig            `mapstructure:"metrics"`
	Native          []BalanceNative          `mapstructure:"native"`
	ERC20           []BalanceERC20           `mapstructure:"erc20"`
	Contracts       []BalanceContract        `mapstructure:"contracts"`
	ERC721Balances  []BalanceERC721Balances  `mapstructure:"erc721_balances"`
	ERC721Ownership []BalanceERC721Ownership `mapstructure:"erc721_ownership"`
}

// KafkaSASLConfig holds the optional SASL authentication for the Kafka sink.
// Mechanism is one of "plain", "scram-sha-256", or "scram-sha-512" (empty
// disables SASL). The Password is a secret and is sourced through the shared
// env-interpolation / _cmd machinery (password_cmd) so it never lands in the
// file; it is never logged.
type KafkaSASLConfig struct {
	Mechanism string `mapstructure:"mechanism"`
	Username  string `mapstructure:"username"`
	Password  string `mapstructure:"password"`
}

// KafkaTLSConfig holds the TLS settings for the Kafka connection. SASL must run
// over TLS, so Enabled defaults on when a SASL mechanism is set (enforced by the
// resolver). CACert/ClientCert/ClientKey are file paths; ServerName overrides
// the SNI/verification name; InsecureSkipVerify is a deliberate, dev-only escape
// hatch.
type KafkaTLSConfig struct {
	Enabled            *bool  `mapstructure:"enabled"`
	CACert             string `mapstructure:"ca_cert"`
	ClientCert         string `mapstructure:"client_cert"`
	ClientKey          string `mapstructure:"client_key"`
	ServerName         string `mapstructure:"server_name"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
}

// KafkaConfig is the [kafka] section for evm-sink-kafka. Brokers and Topic are
// the minimum; TopicByType maps a record type to an override topic, and
// PartitionKey selects how the partition key is derived (default: the record's
// documented dedup identity, preserving per-key ordering).
type KafkaConfig struct {
	Brokers []string `mapstructure:"brokers"`
	Topic   string   `mapstructure:"topic"`
	// TopicByType maps a record type (e.g. "event", "balance_sample") to a topic
	// that overrides Topic for records of that type.
	TopicByType map[string]string `mapstructure:"topic_by_type"`
	// PartitionKey selects the partition-key strategy: "identity" (default — the
	// record's dedup identity, so per-key ordering holds), "dedup" (the full
	// dedup key including the sample disambiguator), or "none" (no key —
	// round-robin partitioning, no ordering guarantee).
	PartitionKey string `mapstructure:"partition_key"`
	// RequiredAcks selects the publish acknowledgement level. Only "all" is
	// supported for the at-least-once contract; it is the default. Surfaced so a
	// future relaxation is a config change, and so a wrong value fails fast.
	RequiredAcks string `mapstructure:"required_acks"`
	// BackoffBase / BackoffMax bound the blocking exponential-backoff retry on a
	// transient publish failure. Strings so a duration like "500ms" / "30s"
	// parses; empty falls back to built-in defaults.
	BackoffBase string `mapstructure:"backoff_base"`
	BackoffMax  string `mapstructure:"backoff_max"`
	// BatchTimeout bounds how long the writer waits to fill a batch before
	// flushing; kept small so a low-volume stream still confirms promptly.
	BatchTimeout string `mapstructure:"batch_timeout"`
	// ReadinessProbeInterval is how often an active broker-reachability probe
	// refreshes /readyz while idle (no records flowing). A duration like "15s"
	// (the default); "0" or "off" disables it, after which readiness follows
	// publish outcomes only.
	ReadinessProbeInterval string `mapstructure:"readiness_probe_interval"`

	SASL    KafkaSASLConfig `mapstructure:"sasl"`
	TLS     KafkaTLSConfig  `mapstructure:"tls"`
	Metrics MetricsConfig   `mapstructure:"metrics"`
}

// WebhookAuthConfig holds the optional auth header for the webhook sink. Header
// names the request header (e.g. "Authorization"); Value is the secret payload
// (e.g. "Bearer <token>") and is sourced through the shared env-interpolation /
// _cmd machinery (value_cmd) so it never lands in the file and is never logged.
type WebhookAuthConfig struct {
	Header string `mapstructure:"header"`
	Value  string `mapstructure:"value"`
}

// WebhookFieldCondition is the single simple field condition the webhook sink
// supports — NOT a full rule DSL. It compares one named field inside a record's
// data payload against a value with a comparison operator. Op is "eq", "gt", or
// "lt"; Field is the data-field name (e.g. "balance"); Value is the comparand.
// Numeric fields (the contract's string-encoded amounts and JSON numbers)
// compare numerically for gt/lt; eq falls back to a string compare when the
// operands are not both numeric.
type WebhookFieldCondition struct {
	Field string `mapstructure:"field"`
	Op    string `mapstructure:"op"`
	Value string `mapstructure:"value"`
}

// WebhookFilters scopes which records are forwarded. By default every record is
// POSTed; these optional filters narrow that. IncludeTypes/IncludeNames are
// allowlists (when non-empty, a record must match to be forwarded);
// ExcludeTypes/ExcludeNames are denylists (a match drops the record). Field is
// the single optional field condition. All configured filters must pass for a
// record to be forwarded.
type WebhookFilters struct {
	IncludeTypes []string               `mapstructure:"include_types"`
	ExcludeTypes []string               `mapstructure:"exclude_types"`
	IncludeNames []string               `mapstructure:"include_names"`
	ExcludeNames []string               `mapstructure:"exclude_names"`
	Field        *WebhookFieldCondition `mapstructure:"field"`
}

// WebhookConfig is the [webhook] section for evm-sink-webhook. URL is the
// minimum; the sink POSTs each record's verbatim JSONL payload as
// application/json. Method defaults to POST. Headers are static request headers;
// Auth carries the optional secret auth header. Filters optionally scope which
// records are forwarded.
type WebhookConfig struct {
	URL    string `mapstructure:"url"`
	Method string `mapstructure:"method"`
	// Headers are static, non-secret request headers added to every request.
	Headers map[string]string `mapstructure:"headers"`
	// Timeout bounds a single HTTP request; empty falls back to a built-in
	// default. A string so a duration like "10s" parses.
	Timeout string `mapstructure:"timeout"`
	// BackoffBase / BackoffMax bound the blocking exponential-backoff retry on a
	// transient POST failure (network/timeout/5xx). Strings so "500ms" / "30s"
	// parse; empty falls back to built-in defaults.
	BackoffBase string `mapstructure:"backoff_base"`
	BackoffMax  string `mapstructure:"backoff_max"`
	// HealthURL, when set, is GET-probed by the active readiness probe to confirm
	// the endpoint is reachable while idle. Empty disables the active probe;
	// readiness then follows POST outcomes and starts optimistically ready.
	HealthURL string `mapstructure:"health_url"`
	// ReadinessProbeInterval is how often the active probe runs when HealthURL is
	// set. A duration like "15s" (the default); "0"/"off" disables it.
	ReadinessProbeInterval string `mapstructure:"readiness_probe_interval"`

	Auth    WebhookAuthConfig `mapstructure:"auth"`
	Filters WebhookFilters    `mapstructure:"filters"`
	Metrics MetricsConfig     `mapstructure:"metrics"`
}

// StreamFull is the fully decoded configuration for evm-stream: shared keys
// plus the [stream] subtree.
type StreamFull struct {
	Shared
	Stream StreamConfig
}

// KafkaFull is the fully decoded configuration for evm-sink-kafka: shared keys
// plus the [kafka] subtree. Sinks read stdin JSONL, not RPC, so [rpc]/[chain]
// are not required — only the shared [metrics]/[log] plus [kafka].
type KafkaFull struct {
	Shared
	Kafka KafkaConfig
}

// WebhookFull is the fully decoded configuration for evm-sink-webhook: shared
// keys plus the [webhook] subtree. Like the kafka sink it reads stdin JSONL, not
// RPC, so only the shared [metrics]/[log] plus [webhook] are required.
type WebhookFull struct {
	Shared
	Webhook WebhookConfig
}

// BalanceFull is the fully decoded configuration for evm-balance: shared keys
// plus the [balance] subtree.
type BalanceFull struct {
	Shared
	Balance BalanceConfig
}
