package cli

import (
	"context"
	"fmt"
	"log/slog"
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

// watchReload calls reload on every SIGHUP until ctx is cancelled. Notifying on
// SIGHUP also overrides its default (process-terminating) disposition, so an
// operator can `kill -HUP` a running tool to re-read config without killing it.
// The returned stop releases the handler; the watcher goroutine exits when ctx is
// cancelled (the run context, cancelled on shutdown).
func watchReload(ctx context.Context, reload func()) func() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
				reload()
			}
		}
	}()
	return func() { signal.Stop(ch) }
}

// resolvedLog resolves log.level/log.format through the full config pipeline —
// flag > env > file > default precedence plus ${VAR}/_cmd interpolation — so
// startup and reload apply exactly the values every other config consumer sees.
// configFile is the explicit --config path ("" = default search).
func resolvedLog(cmd *cobra.Command, configFile string, allowExec bool) (level, format string, err error) {
	loader, err := config.New(config.Options{ConfigFile: configFile, Flags: cmd.Flags()})
	if err != nil {
		return "", "", err
	}
	sh, err := loader.DecodeShared(allowExec)
	if err != nil {
		return "", "", err
	}
	return sh.Log.Level, sh.Log.Format, nil
}

// reloadLogging re-reads the config (same precedence + interpolation as startup)
// and live-applies the resolved log level/format. This handles the log settings
// for every tool; producers additionally hot-reload their watched contract/target
// set via their own SIGHUP watcher (see reloadStreamWatchSet / reloadBalanceTargets).
// Connection-level and structural changes still require a restart (gapless when a
// resume cursor is configured). A bad or unreadable config is logged and the
// running configuration is kept.
func reloadLogging(cmd *cobra.Command, configFile string, allowExec bool) {
	level, format, err := resolvedLog(cmd, configFile, allowExec)
	if err != nil {
		slog.Warn("config reload failed; keeping running configuration", "error", err.Error())
		return
	}
	if rerr := logging.Reload(level, format); rerr != nil {
		slog.Warn("config reload: invalid log settings; keeping running configuration", "error", rerr.Error())
		return
	}
	slog.Info("config reloaded (SIGHUP)",
		"log_level", level, "log_format", format,
		"note", "log level/format applied; producers also reload their watched set; other changes need a restart")
}

// allowExec resolves the effective --allow-exec value, honoring the
// EVM_TOOLS_ALLOW_EXEC=1 environment fallback.
func (f *sharedFlags) allowExecEnabled() bool {
	if f.allowExec {
		return true
	}
	return os.Getenv("EVM_TOOLS_ALLOW_EXEC") == "1"
}

// setupLogging configures the slog default logger, honoring log.level/log.format
// from the config file (with full precedence + interpolation) so a file-only
// deployment logs at its configured level immediately — not just after a SIGHUP.
// On a config/parse error it falls back to the flag/default values; the real
// config error then surfaces from the subcommand itself.
func (f *sharedFlags) setupLogging(cmd *cobra.Command) error {
	level, format := f.logLevel, f.logFormat
	if l, fm, err := resolvedLog(cmd, f.configFile, f.allowExecEnabled()); err == nil {
		level, format = l, fm
	}
	_, err := logging.Setup(level, format)
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
			if err := f.setupLogging(cmd); err != nil {
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
			// SIGHUP re-reads config and live-applies the log level/format.
			defer watchReload(ctx, func() { reloadLogging(cmd, f.configFile, f.allowExecEnabled()) })()
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
			if err := f.setupLogging(cmd); err != nil {
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
			if err := f.setupLogging(cmd); err != nil {
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
