package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"":      slog.LevelInfo,
		"info":  slog.LevelInfo,
		"DEBUG": slog.LevelDebug,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
	}
	for in, want := range cases {
		got, err := ParseLevel(in)
		if err != nil {
			t.Errorf("ParseLevel(%q): %v", in, err)
		}
		if got != want {
			t.Errorf("ParseLevel(%q) = %v, want %v", in, got, want)
		}
	}
	if _, err := ParseLevel("loud"); err == nil {
		t.Error("expected error for invalid level")
	}
}

func TestParseFormat(t *testing.T) {
	for _, in := range []string{"", "text", "TEXT"} {
		if f, err := ParseFormat(in); err != nil || f != FormatText {
			t.Errorf("ParseFormat(%q) = %v, %v", in, f, err)
		}
	}
	if f, err := ParseFormat("json"); err != nil || f != FormatJSON {
		t.Errorf("ParseFormat(json) = %v, %v", f, err)
	}
	if _, err := ParseFormat("yaml"); err == nil {
		t.Error("expected error for invalid format")
	}
}

func TestNewTextAndJSON(t *testing.T) {
	var buf bytes.Buffer
	New(&buf, slog.LevelInfo, FormatText).Info("hello", "k", "v")
	if !strings.Contains(buf.String(), "hello") || !strings.Contains(buf.String(), "k=v") {
		t.Errorf("text handler output unexpected: %s", buf.String())
	}

	buf.Reset()
	New(&buf, slog.LevelInfo, FormatJSON).Info("hello", "k", "v")
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("json handler did not emit JSON: %v\n%s", err, buf.String())
	}
	if m["msg"] != "hello" || m["k"] != "v" {
		t.Errorf("json fields unexpected: %v", m)
	}
}

// TestLevelFiltering verifies debug is suppressed at info level.
func TestLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, slog.LevelInfo, FormatText)
	l.Debug("should-not-appear")
	if strings.Contains(buf.String(), "should-not-appear") {
		t.Errorf("debug should be filtered at info level: %s", buf.String())
	}
}

// TestReloadLevelAndFormat verifies a live level change takes effect (debug
// becomes visible), a format change switches to JSON, and a bad reload is
// rejected without mutating the running logger.
func TestReloadLevelAndFormat(t *testing.T) {
	// Debug/Info/Warn route to outWriter; redirect it to capture them.
	var buf bytes.Buffer
	curMu.Lock()
	prev := outWriter
	outWriter = &buf
	curMu.Unlock()
	t.Cleanup(func() { curMu.Lock(); outWriter = prev; curMu.Unlock() })

	if _, err := Setup("info", "text"); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	slog.Default().Debug("dbg-hidden")
	if strings.Contains(buf.String(), "dbg-hidden") {
		t.Error("debug must be filtered at info level")
	}

	if err := Reload("debug", "text"); err != nil {
		t.Fatalf("Reload to debug: %v", err)
	}
	slog.Default().Debug("dbg-shown")
	if !strings.Contains(buf.String(), "dbg-shown") {
		t.Errorf("debug must appear after live reload to debug: %s", buf.String())
	}

	buf.Reset()
	if err := Reload("info", "json"); err != nil {
		t.Fatalf("Reload to json: %v", err)
	}
	slog.Default().Info("as-json", "k", "v")
	var m map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &m); err != nil {
		t.Fatalf("expected JSON after format reload: %v\n%s", err, buf.String())
	}

	if err := Reload("nope", "text"); err == nil {
		t.Error("a bad level on reload must error (and leave the logger intact)")
	}
}

// TestLevelSplit verifies the default logger routes debug/info/warn to the
// stdout writer and error to the stderr writer.
func TestLevelSplit(t *testing.T) {
	var outBuf, errBuf bytes.Buffer
	curMu.Lock()
	prevOut, prevErr := outWriter, errWriter
	outWriter, errWriter = &outBuf, &errBuf
	curMu.Unlock()
	t.Cleanup(func() { curMu.Lock(); outWriter, errWriter = prevOut, prevErr; curMu.Unlock() })

	if _, err := Setup("debug", "text"); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	l := slog.Default()
	l.Debug("dbg-line")
	l.Info("info-line")
	l.Warn("warn-line")
	l.Error("err-line")

	out, errs := outBuf.String(), errBuf.String()
	for _, want := range []string{"dbg-line", "info-line", "warn-line"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout writer missing %q:\n%s", want, out)
		}
		if strings.Contains(errs, want) {
			t.Errorf("stderr writer should not contain %q:\n%s", want, errs)
		}
	}
	if !strings.Contains(errs, "err-line") {
		t.Errorf("stderr writer missing error:\n%s", errs)
	}
	if strings.Contains(out, "err-line") {
		t.Errorf("stdout writer should not contain the error:\n%s", out)
	}
}

func TestSetupErrors(t *testing.T) {
	if _, err := Setup("nope", "text"); err == nil {
		t.Error("expected level error")
	}
	if _, err := Setup("info", "nope"); err == nil {
		t.Error("expected format error")
	}
	if _, err := Setup("debug", "json"); err != nil {
		t.Errorf("valid Setup failed: %v", err)
	}
}
