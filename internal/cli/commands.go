package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/logging"
)

// errNotImplemented is the clear, uniform error the scaffolded run/validate/
// check paths return until M1/M2 fill them in.
func errNotImplemented(tool Tool, what string) error {
	return fmt.Errorf("%s: %s is not implemented yet (scaffolded in M0; see docs/plan.md)", tool, what)
}

// allowExec resolves the effective --allow-exec value, honoring the
// EVM_TOOLS_ALLOW_EXEC=1 environment fallback.
func (f *sharedFlags) allowExecEnabled() bool {
	if f.allowExec {
		return true
	}
	return os.Getenv("EVM_TOOLS_ALLOW_EXEC") == "1"
}

// setupLogging configures the slog default logger from the shared flags. It is
// called by every working subcommand before doing anything else.
func (f *sharedFlags) setupLogging() error {
	_, err := logging.Setup(f.logLevel, f.logFormat)
	return err
}

// loadConfig builds the config loader with flag bindings wired in. The caller
// then decodes the tool-specific subtree.
func (f *sharedFlags) loadConfig(cmd *cobra.Command) (*config.Loader, error) {
	return config.New(config.Options{
		ConfigFile: f.configFile,
		Flags:      cmd.Flags(),
	})
}

// decodeFor loads and strict-decodes the config for the given tool, returning
// an error rather than the typed struct (M0 callers only need it to succeed).
// In M1/M2 this is where validate inspects the decoded config.
func (f *sharedFlags) decodeFor(cmd *cobra.Command, tool Tool) error {
	loader, err := f.loadConfig(cmd)
	if err != nil {
		return err
	}
	switch tool {
	case ToolStream:
		_, err = loader.DecodeStream(f.allowExecEnabled())
	case ToolBalance:
		_, err = loader.DecodeBalance(f.allowExecEnabled())
	default:
		return fmt.Errorf("unknown tool %q", tool)
	}
	return err
}

func newRunCommand(tool Tool, f *sharedFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Run the producer, emitting JSONL records to stdout",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.setupLogging(); err != nil {
				return err
			}
			if err := f.decodeFor(cmd, tool); err != nil {
				return err
			}
			return errNotImplemented(tool, "run")
		},
	}
}

func newValidateCommand(tool Tool, f *sharedFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate config (and, later, mTLS material and ABI resolution) without connecting",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.setupLogging(); err != nil {
				return err
			}
			// Config loading + strict decode already works in M0, so a typo in
			// the tool's own section fails here. mTLS material and ABI
			// resolution checks arrive with the transport in M1.
			if err := f.decodeFor(cmd, tool); err != nil {
				return err
			}
			return errNotImplemented(tool, "validate (mTLS/ABI checks)")
		},
	}
}

func newCheckCommand(tool Tool, f *sharedFlags) *cobra.Command {
	check := &cobra.Command{
		Use:   "check",
		Short: "Health checks for the configured RPC endpoint",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	check.AddCommand(&cobra.Command{
		Use:   "rpc",
		Short: "One-shot RPC reachability check (exit 0 on success)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.setupLogging(); err != nil {
				return err
			}
			if err := f.decodeFor(cmd, tool); err != nil {
				return err
			}
			return errNotImplemented(tool, "check rpc")
		},
	})
	return check
}
