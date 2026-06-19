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

// RPCConfig holds the shared HTTPS+mTLS RPC transport settings. The same
// transport serves normal runs, balance polling, backfills, and health checks.
type RPCConfig struct {
	URL        string `mapstructure:"url"`
	ClientCert string `mapstructure:"client_cert"`
	ClientKey  string `mapstructure:"client_key"`
	CACert     string `mapstructure:"ca_cert"`
	ServerName string `mapstructure:"server_name"`
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

// StreamFull is the fully decoded configuration for evm-stream: shared keys
// plus the [stream] subtree.
type StreamFull struct {
	Shared
	Stream StreamConfig
}

// BalanceFull is the fully decoded configuration for evm-balance: shared keys
// plus the [balance] subtree.
type BalanceFull struct {
	Shared
	Balance BalanceConfig
}
