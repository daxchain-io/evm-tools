// Package cli builds the shared Cobra command trees for the producer CLIs.
//
// Both evm-stream and evm-balance expose the same command surface — run,
// validate, check rpc, version — over the same shared flag set (--config,
// --rpc-*, --metrics*, --log-level/-format, --allow-exec). This package
// constructs that tree parameterized by tool so the two cmd/ entrypoints stay
// thin and identical in shape.
//
// The run/validate/check paths are fully implemented for both producers:
// evm-stream landed in M1 and evm-balance in M2. version and --help are
// likewise functional.
package cli

import (
	"github.com/spf13/cobra"
)

// Tool identifies which producer a command tree is for. It selects the binary
// name, the config subtree decoded, and the short/long help text.
type Tool string

// Supported tools.
const (
	ToolStream  Tool = "evm-stream"
	ToolBalance Tool = "evm-balance"
)

// sharedFlags holds the values bound to the persistent flag set. They are
// resolved into the typed config by the config loader (flags win over env/file).
type sharedFlags struct {
	configFile string

	chain string

	// output is the record transport spec: "" / "-" / "stdout" (default) write
	// JSONL to stdout; "unix:/path" listens on a Unix-domain socket. Resolved with
	// config (top-level [output]) by outputSpec.
	output string
	// blockUntilConsumer applies to a "unix:" output: wait for a consumer before
	// emitting and block (lossless) while none are connected. Default true.
	blockUntilConsumer bool

	rpcURL         string
	rpcClientCert  string
	rpcClientKey   string
	rpcCACert      string
	rpcServerName  string
	rpcRequireMTLS bool

	metricsEnabled bool
	metricsAddr    string
	metricsPath    string

	logLevel  string
	logFormat string

	allowExec bool

	// evm-stream-only: let the producer run config-free. streamContracts are
	// contract addresses to watch (each resolved against the built-in standard
	// ABIs using streamEvents); streamNativeTransfers enables native ETH transfer
	// monitoring. These three merge on top of any config file via applyStreamFlags.
	// streamFromBlock / streamPollInterval override stream.from_block /
	// stream.poll_interval and bind through flagBindings like the other scalars, so
	// backfill height and head-poll cadence are reachable without a config file too.
	streamContracts       []string
	streamEvents          []string
	streamNativeTransfers bool
	streamFromBlock       string
	streamPollInterval    string

	// evm-balance-only: let the poller run config-free. balanceNative / balanceERC20
	// are additive targets merged via applyBalanceFlags (--erc20 is "token:holder");
	// balanceInterval / balanceEveryBlocks set the sampling cadence (interval XOR
	// every_blocks) and bind through flagBindings like the other scalars.
	balanceNative      []string
	balanceERC20       []string
	balanceInterval    string
	balanceEveryBlocks int
}

// shortDesc returns the one-line description for a tool.
func (t Tool) shortDesc() string {
	switch t {
	case ToolStream:
		return "Stream live EVM contract events and native transfers as JSONL"
	case ToolBalance:
		return "Poll EVM balances and contract state and emit JSONL samples"
	default:
		return "An evm-tools producer"
	}
}

// NewRootCommand builds the full command tree for the given tool.
func NewRootCommand(tool Tool) *cobra.Command {
	flags := &sharedFlags{}

	root := &cobra.Command{
		Use:           string(tool),
		Short:         tool.shortDesc(),
		SilenceUsage:  true,
		SilenceErrors: true,
		// Subcommands carry the behavior; the bare root just prints help.
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}

	bindSharedFlags(root, flags)
	switch tool {
	case ToolStream:
		bindStreamFlags(root, flags)
	case ToolBalance:
		bindBalanceFlags(root, flags)
	}

	root.AddCommand(
		newRunCommand(tool, flags),
		newValidateCommand(tool, flags),
		newCheckCommand(tool, flags),
		newVersionCommand(),
	)

	return root
}

// bindSharedFlags installs the shared persistent flag set on the root command
// so every subcommand inherits it.
func bindSharedFlags(root *cobra.Command, f *sharedFlags) {
	pf := root.PersistentFlags()

	pf.StringVarP(&f.configFile, "config", "c", "", "path to the evm-tools TOML config file")

	pf.StringVar(&f.chain, "chain", "", `chain label for records/metrics (default: derived from the resolved chain id, e.g. "ethereum"; the chain id always comes from RPC)`)

	pf.StringVar(&f.output, "output", "", `record destination: "-"/"stdout" (default) or "unix:/path" to listen for a sink to connect`)

	pf.BoolVar(&f.blockUntilConsumer, "block-until-consumer", true, `for a "unix:" --output: wait for a consumer and block (lossless) while none are connected; =false drops records with no consumer (fire-and-forget)`)

	pf.StringVar(&f.rpcURL, "rpc-url", "", "full EVM RPC endpoint URL (including port when needed)")
	pf.StringVar(&f.rpcClientCert, "rpc-client-cert", "", "path to the mTLS client certificate")
	pf.StringVar(&f.rpcClientKey, "rpc-client-key", "", "path to the mTLS client private key")
	pf.StringVar(&f.rpcCACert, "rpc-ca-cert", "", "path to a custom CA certificate bundle")
	pf.StringVar(&f.rpcServerName, "rpc-server-name", "", "optional TLS server name override")
	pf.BoolVar(&f.rpcRequireMTLS, "rpc-require-mtls", false, "require an mTLS client cert/key for HTTPS (off by default; public providers need none)")

	pf.BoolVar(&f.metricsEnabled, "metrics", false, "enable the Prometheus metrics endpoint")
	pf.StringVar(&f.metricsAddr, "metrics-addr", "", "metrics bind address, e.g. :9000")
	pf.StringVar(&f.metricsPath, "metrics-path", "", "metrics route, e.g. /metrics")

	pf.StringVar(&f.logLevel, "log-level", "info", "log level: debug|info|warn|error")
	pf.StringVar(&f.logFormat, "log-format", "text", "log format: text|json")

	pf.BoolVar(&f.allowExec, "allow-exec", false, "allow config _cmd keys to execute (also EVM_TOOLS_ALLOW_EXEC=1)")
}

// bindStreamFlags installs the evm-stream-only flags that let it run config-free:
// point it at one or more contracts (resolved against the built-in standard ABIs)
// and/or enable native transfers. They merge on top of any config file, so they
// add to — rather than replace — configured contracts.
func bindStreamFlags(root *cobra.Command, f *sharedFlags) {
	pf := root.PersistentFlags()
	pf.StringArrayVar(&f.streamContracts, "contract", nil, "contract address to watch (repeatable); resolves --events against the built-in ERC-20/721/1155 ABIs")
	pf.StringSliceVar(&f.streamEvents, "events", nil, "comma-separated event names for --contract addresses (default: Transfer)")
	pf.BoolVar(&f.streamNativeTransfers, "native-transfers", false, "emit native ETH transfers (enable without any config file)")
	pf.StringVar(&f.streamFromBlock, "from-block", "", `start block: "latest" (new activity only) or a block number to backfill from (default: latest; a checkpoint cursor still wins)`)
	pf.StringVar(&f.streamPollInterval, "poll-interval", "", "head-poll cadence, e.g. 2s (default: 2s)")
}

// bindBalanceFlags installs the evm-balance-only flags that let it run config-free:
// --native / --erc20 name targets to poll (merged on top of any config file by
// applyBalanceFlags, so they add to — rather than replace — configured targets),
// and --interval / --every-blocks set the sampling cadence (exactly one).
func bindBalanceFlags(root *cobra.Command, f *sharedFlags) {
	pf := root.PersistentFlags()
	pf.StringArrayVar(&f.balanceNative, "native", nil, "report the native balance of this address (repeatable)")
	pf.StringArrayVar(&f.balanceERC20, "erc20", nil, `report an ERC-20 balance as "token:holder" (repeatable)`)
	pf.StringVar(&f.balanceInterval, "interval", "", "sampling cadence, e.g. 30s (set this or --every-blocks)")
	pf.IntVar(&f.balanceEveryBlocks, "every-blocks", 0, "sample every N new blocks instead of on a time interval")
}

// outputSpec resolves the producer's record-destination spec with flag-over-config
// precedence: an explicit --output wins, otherwise the top-level [output] config
// value (which Viper already resolves from env > file > default). An empty result
// means stdout. It is not in flagBindings because --output maps to the same key
// for both producers, so the precedence is applied here rather than via Viper.
func (f *sharedFlags) outputSpec(cmd *cobra.Command, cfgOutput string) string {
	if cmd.Flags().Changed("output") {
		return f.output
	}
	return cfgOutput
}
