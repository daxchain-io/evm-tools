package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/logging"
)

// signalContext returns a context cancelled on the first SIGINT/SIGTERM (which
// drives graceful shutdown) and forces an immediate os.Exit(1) on a SECOND
// signal — an escape hatch so a wedged graceful shutdown can be stopped without
// resorting to SIGKILL. The returned stop releases the handler; on a clean
// completion (no signal) the watcher goroutine exits via ctx cancellation
// without leaking.
func signalContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-ctx.Done():
			// Cancelled without a signal (clean completion); stop listening.
		case <-sigCh:
			cancel() // first signal: begin graceful shutdown
			<-sigCh  // second signal: hard exit
			os.Exit(1)
		}
	}()
	return ctx, func() {
		signal.Stop(sigCh)
		cancel()
	}
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

func newRunCommand(tool Tool, f *sharedFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Run the producer, emitting JSONL records to stdout",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := f.setupLogging(); err != nil {
				return err
			}
			// Ignore SIGPIPE so a write to a broken stdout (a dead downstream sink)
			// returns EPIPE to the record writer — which the run loop treats as a
			// terminal "downstream gone" condition with a clean non-signal exit —
			// instead of the default disposition killing the producer with a signal
			// and bypassing graceful shutdown / the final flush.
			signal.Ignore(syscall.SIGPIPE)
			// Derive a signal-aware context so SIGINT/SIGTERM trigger a clean
			// shutdown (finish the in-flight line, flush, stop the server); a
			// second signal force-exits a wedged shutdown.
			ctx, stop := signalContext(cmd.Context())
			defer stop()
			cmd.SetContext(ctx)

			switch tool {
			case ToolStream:
				return streamRun(cmd, f)
			case ToolBalance:
				return balanceRun(cmd, f)
			default:
				return fmt.Errorf("unknown tool %q", tool)
			}
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
			switch tool {
			case ToolStream:
				return streamValidate(cmd, f)
			case ToolBalance:
				return balanceValidate(cmd, f)
			default:
				return fmt.Errorf("unknown tool %q", tool)
			}
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
			switch tool {
			case ToolStream:
				return streamCheckRPC(cmd, f)
			case ToolBalance:
				return balanceCheckRPC(cmd, f)
			default:
				return fmt.Errorf("unknown tool %q", tool)
			}
		},
	})
	return check
}
