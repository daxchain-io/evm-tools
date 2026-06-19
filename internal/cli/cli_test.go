package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// run executes a root command for the given tool with args, capturing stdout
// and returning the error.
func run(t *testing.T, tool Tool, args ...string) (string, error) {
	t.Helper()
	root := NewRootCommand(tool)
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), err
}

func TestVersionText(t *testing.T) {
	for _, tool := range []Tool{ToolStream, ToolBalance} {
		out, err := run(t, tool, "version")
		if err != nil {
			t.Fatalf("%s version: %v", tool, err)
		}
		for _, want := range []string{"version:", "commit:", "date:", "go:"} {
			if !strings.Contains(out, want) {
				t.Errorf("%s version output missing %q:\n%s", tool, want, out)
			}
		}
	}
}

func TestVersionJSON(t *testing.T) {
	out, err := run(t, ToolStream, "version", "--json")
	if err != nil {
		t.Fatalf("version --json: %v", err)
	}
	var info struct {
		Version   string `json:"version"`
		Commit    string `json:"commit"`
		Date      string `json:"date"`
		GoVersion string `json:"go_version"`
	}
	if err := json.Unmarshal([]byte(out), &info); err != nil {
		t.Fatalf("version --json not valid JSON: %v\n%s", err, out)
	}
	if info.Version == "" || info.GoVersion == "" {
		t.Errorf("version --json missing fields: %+v", info)
	}
}

func TestHelpListsAllCommands(t *testing.T) {
	out, err := run(t, ToolStream, "--help")
	if err != nil {
		t.Fatalf("--help: %v", err)
	}
	for _, want := range []string{"run", "validate", "check", "version"} {
		if !strings.Contains(out, want) {
			t.Errorf("--help missing command %q:\n%s", want, out)
		}
	}
}

func TestHelpListsSharedFlags(t *testing.T) {
	out, err := run(t, ToolBalance, "--help")
	if err != nil {
		t.Fatalf("--help: %v", err)
	}
	for _, want := range []string{
		"--config", "--rpc-url", "--rpc-client-cert", "--rpc-client-key",
		"--rpc-ca-cert", "--rpc-server-name", "--metrics", "--metrics-addr",
		"--metrics-path", "--log-level", "--log-format", "--allow-exec",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("--help missing flag %q:\n%s", want, out)
		}
	}
}

// TestRunNotImplemented verifies the still-scaffolded balance run path returns a
// clear not-implemented error (after successfully loading defaults config).
// evm-stream's run path is implemented in M1, so this exercises evm-balance,
// which lands in M2.
func TestRunNotImplemented(t *testing.T) {
	_, err := run(t, ToolBalance, "run", "--config", writeTempConfig(t))
	if err == nil {
		t.Fatal("expected not-implemented error from run")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("error should be clearly not-implemented, got: %v", err)
	}
}

func TestCheckRPCNotImplemented(t *testing.T) {
	_, err := run(t, ToolBalance, "check", "rpc", "--config", writeTempConfig(t))
	if err == nil {
		t.Fatal("expected not-implemented error from check rpc")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("error should be clearly not-implemented, got: %v", err)
	}
}

// TestBadLogLevelFails verifies a bad --log-level surfaces a clear error before
// any work.
func TestBadLogLevelFails(t *testing.T) {
	_, err := run(t, ToolStream, "run", "--log-level", "loud", "--config", writeTempConfig(t))
	if err == nil || !strings.Contains(err.Error(), "log level") {
		t.Fatalf("expected log level error, got: %v", err)
	}
}
