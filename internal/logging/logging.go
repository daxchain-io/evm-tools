// Package logging configures the standard-library slog logger used for
// diagnostics. Logs are the process's normal output: records below Error go to
// stdout and Error (and above) go to stderr — the conventional Unix split. The
// JSONL record stream never shares stdout; it travels over the record transport
// (internal/transport), so logs own stdout cleanly. Configuring this once here
// keeps every binary behaving identically.
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
)

// Format selects the slog handler output format.
type Format string

// Supported log formats.
const (
	FormatText Format = "text"
	FormatJSON Format = "json"
)

// ParseLevel maps a case-insensitive level name to an slog.Level. The default
// (and the value for an empty string) is info.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("invalid log level %q (want debug|info|warn|error)", s)
	}
}

// ParseFormat maps a case-insensitive format name to a Format. The default
// (and the value for an empty string) is text.
func ParseFormat(s string) (Format, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "text":
		return FormatText, nil
	case "json":
		return FormatJSON, nil
	default:
		return FormatText, fmt.Errorf("invalid log format %q (want text|json)", s)
	}
}

// handlerFor builds a single slog.Handler over w with the given options/format.
func handlerFor(w io.Writer, opts *slog.HandlerOptions, format Format) slog.Handler {
	if format == FormatJSON {
		return slog.NewJSONHandler(w, opts)
	}
	return slog.NewTextHandler(w, opts)
}

// New builds an *slog.Logger that writes every level to w with the given level
// and format. It is the single-stream constructor used by tests and by callers
// that deliberately want all logs on one writer.
func New(w io.Writer, level slog.Level, format Format) *slog.Logger {
	return slog.New(handlerFor(w, &slog.HandlerOptions{Level: level}, format))
}

// splitHandler routes a record below LevelError to out (stdout) and a record at
// LevelError or above to err (stderr) — the conventional stream split. Both
// sub-handlers share the same level, so a level change applies to both at once.
type splitHandler struct {
	out slog.Handler // records below Error (stdout)
	err slog.Handler // records at Error and above (stderr)
}

func (h splitHandler) Enabled(ctx context.Context, l slog.Level) bool {
	if l >= slog.LevelError {
		return h.err.Enabled(ctx, l)
	}
	return h.out.Enabled(ctx, l)
}

func (h splitHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level >= slog.LevelError {
		return h.err.Handle(ctx, r)
	}
	return h.out.Handle(ctx, r)
}

func (h splitHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return splitHandler{out: h.out.WithAttrs(attrs), err: h.err.WithAttrs(attrs)}
}

func (h splitHandler) WithGroup(name string) slog.Handler {
	return splitHandler{out: h.out.WithGroup(name), err: h.err.WithGroup(name)}
}

// Live-reload state for the default logger. The level is held in a shared
// slog.LevelVar so a runtime level change takes effect immediately without
// rebuilding the handler; a format change rebuilds and re-installs the default
// handler under the mutex. The split destinations are process-global vars so a
// test can redirect them; production uses stdout (sub-error) and stderr (error).
var (
	curMu     sync.Mutex
	curLevel            = new(slog.LevelVar)
	curFormat           = FormatText
	outWriter io.Writer = os.Stdout // records below Error
	errWriter io.Writer = os.Stderr // records at Error and above
)

// RouteAllToStderr sends every log level to stderr instead of the default
// stdout/stderr split. evm-sink-stdout calls this before [Setup] because it
// writes the JSONL record stream to stdout, so its diagnostics must stay off
// stdout. The change is picked up by Setup and survives a [Reload].
func RouteAllToStderr() {
	curMu.Lock()
	defer curMu.Unlock()
	outWriter = os.Stderr
}

// Setup parses the level/format strings, builds the split logger backed by a
// reloadable level, installs it as the slog default, and returns it. A parse
// error is returned without mutating the default logger. After Setup, [Reload]
// can change the level/format at runtime (e.g. on SIGHUP).
func Setup(level, format string) (*slog.Logger, error) {
	lvl, err := ParseLevel(level)
	if err != nil {
		return nil, err
	}
	fmtVal, err := ParseFormat(format)
	if err != nil {
		return nil, err
	}
	curMu.Lock()
	defer curMu.Unlock()
	curLevel.Set(lvl)
	curFormat = fmtVal
	logger := newLeveled(curLevel, fmtVal)
	slog.SetDefault(logger)
	return logger, nil
}

// Reload re-applies the level and format to the default logger at runtime. A
// level change takes effect immediately via the shared LevelVar; a format change
// rebuilds and re-installs the default handler. On a parse error it returns the
// error without mutating the logger, so a bad reload leaves the running config
// intact.
func Reload(level, format string) error {
	lvl, err := ParseLevel(level)
	if err != nil {
		return err
	}
	fmtVal, err := ParseFormat(format)
	if err != nil {
		return err
	}
	curMu.Lock()
	defer curMu.Unlock()
	curLevel.Set(lvl)
	if fmtVal != curFormat {
		slog.SetDefault(newLeveled(curLevel, fmtVal))
		curFormat = fmtVal
	}
	return nil
}

// newLeveled builds the split logger whose sub-handlers read their level from lv,
// so the level can change live without rebuilding the handler.
func newLeveled(lv *slog.LevelVar, format Format) *slog.Logger {
	opts := &slog.HandlerOptions{Level: lv}
	return slog.New(splitHandler{
		out: handlerFor(outWriter, opts, format),
		err: handlerFor(errWriter, opts, format),
	})
}
