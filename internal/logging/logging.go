// Package logging configures the standard-library slog logger used for
// human-readable diagnostics. Per the design, stdout is reserved for the JSONL
// data contract, so all logs go to stderr. Configuring this once here keeps
// every binary behaving identically.
package logging

import (
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

// New builds an *slog.Logger that writes to w (use os.Stderr in production)
// with the given level and format.
func New(w io.Writer, level slog.Level, format Format) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if format == FormatJSON {
		h = slog.NewJSONHandler(w, opts)
	} else {
		h = slog.NewTextHandler(w, opts)
	}
	return slog.New(h)
}

// Live-reload state for the default logger. The level is held in a shared
// slog.LevelVar so a runtime level change takes effect immediately without
// rebuilding the handler; a format change rebuilds and re-installs the default
// handler under the mutex. These are process-global, mirroring slog's own
// global default logger.
var (
	curMu     sync.Mutex
	curLevel            = new(slog.LevelVar)
	curFormat           = FormatText
	curWriter io.Writer = os.Stderr
)

// Setup parses the level/format strings, builds a stderr logger backed by a
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
	logger := newLeveled(curWriter, curLevel, fmtVal)
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
		slog.SetDefault(newLeveled(curWriter, curLevel, fmtVal))
		curFormat = fmtVal
	}
	return nil
}

// newLeveled builds a logger whose handler reads its level from lv, so the level
// can change live without rebuilding the handler.
func newLeveled(w io.Writer, lv *slog.LevelVar, format Format) *slog.Logger {
	opts := &slog.HandlerOptions{Level: lv}
	var h slog.Handler
	if format == FormatJSON {
		h = slog.NewJSONHandler(w, opts)
	} else {
		h = slog.NewTextHandler(w, opts)
	}
	return slog.New(h)
}
