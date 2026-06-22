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
	if tool == ToolStream {
		bindStreamFlags(root, flags)
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

	pf.StringVar(&f.chain, "chain", "", "chain label for records/metrics (the chain id is always resolved from RPC)")

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
