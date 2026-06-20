package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeFileConfig writes a TOML config to a temp file and returns its path.
func writeFileConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "evm-tools.toml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return p
}

func fileRecord(typ, name string) string {
	return `{"schema_version":1,"type":"` + typ + `","name":"` + name + `","data":{}}`
}

func TestFileSinkVersion(t *testing.T) {
	out, err := runSink(context.Background(), t, ToolSinkFile, "", "version")
	if err != nil {
		t.Fatalf("version: %v", err)
	}
	for _, want := range []string{"version:", "commit:", "date:", "go:"} {
		if !strings.Contains(out, want) {
			t.Errorf("version output missing %q:\n%s", want, out)
		}
	}
}

func TestFileSinkHelpListsFlags(t *testing.T) {
	out, err := runSink(context.Background(), t, ToolSinkFile, "", "--help")
	if err != nil {
		t.Fatalf("--help: %v", err)
	}
	for _, want := range []string{"run", "validate", "version", "--path", "--metrics", "--config"} {
		if !strings.Contains(out, want) {
			t.Errorf("--help missing %q:\n%s", want, out)
		}
	}
	// A file sink has no other sinks' flags and no RPC surface.
	for _, absent := range []string{"--url", "--brokers", "--rpc-url"} {
		if strings.Contains(out, absent) {
			t.Errorf("file sink should not expose %q:\n%s", absent, out)
		}
	}
}

func TestFileSinkValidateOK(t *testing.T) {
	out := filepath.Join(t.TempDir(), "events.jsonl")
	cfg := writeFileConfig(t, `
[file]
path = "`+out+`"
max_size_mb = 10
rotation_interval = "24h"
max_backups = 5
compress = true
`)
	got, err := runSink(context.Background(), t, ToolSinkFile, "", "validate", "-c", cfg)
	if err != nil {
		t.Fatalf("validate: %v\n%s", err, got)
	}
	if !strings.Contains(got, "ok:") {
		t.Errorf("validate output = %q, want ok:", got)
	}
	// validate must not create the file.
	if _, statErr := os.Stat(out); statErr == nil {
		t.Errorf("validate should not create the output file")
	}
}

func TestFileSinkValidateMissingPath(t *testing.T) {
	cfg := writeFileConfig(t, "[file]\nmax_size_mb = 10\n")
	_, err := runSink(context.Background(), t, ToolSinkFile, "", "validate", "-c", cfg)
	if err == nil {
		t.Fatal("expected validate to fail when file.path is unset")
	}
	if !strings.Contains(err.Error(), "file.path is required") {
		t.Errorf("error = %v, want file.path is required", err)
	}
}

func TestFileSinkValidateBadRotationInterval(t *testing.T) {
	cfg := writeFileConfig(t, "[file]\npath = \"/tmp/x.jsonl\"\nrotation_interval = \"nope\"\n")
	_, err := runSink(context.Background(), t, ToolSinkFile, "", "validate", "-c", cfg)
	if err == nil {
		t.Fatal("expected validate to fail on a bad rotation_interval")
	}
}

func TestFileSinkValidateNegativeRotationInterval(t *testing.T) {
	// A negative duration must be rejected, not silently treated as "disabled".
	cfg := writeFileConfig(t, "[file]\npath = \"/tmp/x.jsonl\"\nrotation_interval = \"-5m\"\n")
	_, err := runSink(context.Background(), t, ToolSinkFile, "", "validate", "-c", cfg)
	if err == nil {
		t.Fatal("expected validate to reject a negative rotation_interval")
	}
}

func TestFileSinkValidateRotationDisableSpellings(t *testing.T) {
	// "off"/"0" are the documented disable spellings and must validate cleanly.
	for _, v := range []string{"off", "0", "0s", ""} {
		body := "[file]\npath = \"/tmp/x.jsonl\"\n"
		if v != "" {
			body += "rotation_interval = \"" + v + "\"\n"
		}
		if _, err := runSink(context.Background(), t, ToolSinkFile, "", "validate", "-c", writeFileConfig(t, body)); err != nil {
			t.Errorf("rotation_interval=%q should validate (disabled), got %v", v, err)
		}
	}
}

func TestFileSinkRunWritesAndFilters(t *testing.T) {
	out := filepath.Join(t.TempDir(), "events.jsonl")
	cfg := writeFileConfig(t, `
[file]
path = "`+out+`"
[file.filters]
include_types = ["event"]
`)
	stdin := strings.Join([]string{
		fileRecord("event", "USDT"),
		fileRecord("native_transfer", "ETH"),
		fileRecord("event", "USDC"),
	}, "\n") + "\n"

	// --metrics-addr :0 binds an ephemeral port so the test never conflicts.
	_, err := runSink(context.Background(), t, ToolSinkFile, stdin, "run", "-c", cfg, "--metrics-addr", ":0")
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("wrote %d lines, want 2 (events only):\n%s", len(lines), b)
	}
	for _, l := range lines {
		if !strings.Contains(l, `"type":"event"`) {
			t.Errorf("non-event line written: %s", l)
		}
	}
}
