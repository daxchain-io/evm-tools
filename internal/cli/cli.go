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

	rpcURL        string
	rpcClientCert string
	rpcClientKey  string
	rpcCACert     string
	rpcServerName string

	metricsEnabled bool
	metricsAddr    string
	metricsPath    string

	logLevel  string
	logFormat string

	allowExec bool
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

	pf.StringVar(&f.rpcURL, "rpc-url", "", "full EVM RPC endpoint URL (including port when needed)")
	pf.StringVar(&f.rpcClientCert, "rpc-client-cert", "", "path to the mTLS client certificate")
	pf.StringVar(&f.rpcClientKey, "rpc-client-key", "", "path to the mTLS client private key")
	pf.StringVar(&f.rpcCACert, "rpc-ca-cert", "", "path to a custom CA certificate bundle")
	pf.StringVar(&f.rpcServerName, "rpc-server-name", "", "optional TLS server name override")

	pf.BoolVar(&f.metricsEnabled, "metrics", false, "enable the Prometheus metrics endpoint")
	pf.StringVar(&f.metricsAddr, "metrics-addr", "", "metrics bind address, e.g. :9000")
	pf.StringVar(&f.metricsPath, "metrics-path", "", "metrics route, e.g. /metrics")

	pf.StringVar(&f.logLevel, "log-level", "info", "log level: debug|info|warn|error")
	pf.StringVar(&f.logFormat, "log-format", "text", "log format: text|json")

	pf.BoolVar(&f.allowExec, "allow-exec", false, "allow config _cmd keys to execute (also EVM_TOOLS_ALLOW_EXEC=1)")
}
