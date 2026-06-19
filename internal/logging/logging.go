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

// Setup parses the level/format strings, builds a stderr logger, installs it as
// the slog default, and returns it. A parse error is returned without mutating
// the default logger.
func Setup(level, format string) (*slog.Logger, error) {
	lvl, err := ParseLevel(level)
	if err != nil {
		return nil, err
	}
	fmtVal, err := ParseFormat(format)
	if err != nil {
		return nil, err
	}
	logger := New(os.Stderr, lvl, fmtVal)
	slog.SetDefault(logger)
	return logger, nil
}
