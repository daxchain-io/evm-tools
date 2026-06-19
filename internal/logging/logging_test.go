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
