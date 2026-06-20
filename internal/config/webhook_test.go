package config

import (
	"strings"
	"testing"
)

const sampleWebhookConfig = `
chain = "my-chain"

[metrics]
enabled = false

[log]
level = "info"

[webhook]
url = "https://hooks.internal.example.com/evm"
method = "POST"
timeout = "5s"

[webhook.headers]
X-Source = "evm-tools"

[webhook.auth]
header = "Authorization"

[webhook.filters]
include_types = ["balance_change", "event"]
exclude_names = ["noisy"]

[webhook.filters.field]
field = "balance"
op = "gt"
value = "1000"

[webhook.metrics]
enabled = true
addr = ":9003"
`

func TestDecodeWebhook(t *testing.T) {
	p := writeConfig(t, sampleWebhookConfig)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeWebhook(false)
	if err != nil {
		t.Fatalf("DecodeWebhook: %v", err)
	}
	if cfg.Chain != "my-chain" {
		t.Errorf("chain = %q", cfg.Chain)
	}
	if cfg.Webhook.URL != "https://hooks.internal.example.com/evm" {
		t.Errorf("url = %q", cfg.Webhook.URL)
	}
	if cfg.Webhook.Method != "POST" {
		t.Errorf("method = %q", cfg.Webhook.Method)
	}
	if cfg.Webhook.Timeout != "5s" {
		t.Errorf("timeout = %q", cfg.Webhook.Timeout)
	}
	// Viper lowercases all config keys, including arbitrary map keys, so a header
	// name decodes lowercased. HTTP header names are case-insensitive and Go's
	// http.Header.Set canonicalizes on send, so this is functionally harmless.
	if cfg.Webhook.Headers["x-source"] != "evm-tools" {
		t.Errorf("headers = %v", cfg.Webhook.Headers)
	}
	if cfg.Webhook.Auth.Header != "Authorization" {
		t.Errorf("auth.header = %q", cfg.Webhook.Auth.Header)
	}
	if len(cfg.Webhook.Filters.IncludeTypes) != 2 || cfg.Webhook.Filters.IncludeTypes[0] != "balance_change" {
		t.Errorf("filters.include_types = %v", cfg.Webhook.Filters.IncludeTypes)
	}
	if len(cfg.Webhook.Filters.ExcludeNames) != 1 || cfg.Webhook.Filters.ExcludeNames[0] != "noisy" {
		t.Errorf("filters.exclude_names = %v", cfg.Webhook.Filters.ExcludeNames)
	}
	if cfg.Webhook.Filters.Field == nil {
		t.Fatalf("filters.field should be set")
	}
	if cfg.Webhook.Filters.Field.Field != "balance" || cfg.Webhook.Filters.Field.Op != "gt" || cfg.Webhook.Filters.Field.Value != "1000" {
		t.Errorf("filters.field = %+v", cfg.Webhook.Filters.Field)
	}
	if !cfg.Webhook.Metrics.IsEnabled() || cfg.Webhook.Metrics.Addr != ":9003" {
		t.Errorf("webhook.metrics = %+v", cfg.Webhook.Metrics)
	}
}

// TestWebhookDefaultsApply verifies the built-in defaults fill method, timeout,
// backoff, and the metrics endpoint defaults.
func TestWebhookDefaultsApply(t *testing.T) {
	p := writeConfig(t, `
[webhook]
url = "https://h/evm"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeWebhook(false)
	if err != nil {
		t.Fatalf("DecodeWebhook: %v", err)
	}
	if cfg.Webhook.Method != "POST" {
		t.Errorf("method default = %q, want POST", cfg.Webhook.Method)
	}
	if cfg.Webhook.Timeout != "10s" {
		t.Errorf("timeout default = %q, want 10s", cfg.Webhook.Timeout)
	}
	if cfg.Webhook.BackoffBase != "500ms" || cfg.Webhook.BackoffMax != "30s" {
		t.Errorf("backoff defaults = %q/%q", cfg.Webhook.BackoffBase, cfg.Webhook.BackoffMax)
	}
	if cfg.Webhook.Metrics.Addr != ":9003" {
		t.Errorf("webhook.metrics.addr default = %q, want :9003", cfg.Webhook.Metrics.Addr)
	}
}

// TestWebhookSiblingSectionsIgnored verifies a sink ignores producer/other-sink
// sections rather than rejecting them, so one shared file serves the whole suite.
func TestWebhookSiblingSectionsIgnored(t *testing.T) {
	p := writeConfig(t, sampleConfig+`
[kafka]
brokers = ["b:9092"]
topic = "t"

[webhook]
url = "https://h/evm"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeWebhook(false)
	if err != nil {
		t.Fatalf("DecodeWebhook should ignore sibling sections: %v", err)
	}
	if cfg.Webhook.URL != "https://h/evm" {
		t.Errorf("url = %q", cfg.Webhook.URL)
	}
}

// TestWebhookUnknownKeyFatal verifies a typo in the [webhook] section is a fatal
// strict-decode error.
func TestWebhookUnknownKeyFatal(t *testing.T) {
	p := writeConfig(t, `
[webhook]
url = "https://h/evm"
urll = "typo"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := l.DecodeWebhook(false); err == nil {
		t.Fatal("expected a strict-decode error for an unknown [webhook] key")
	}
}

// TestWebhookEnvOverride verifies EVM_TOOLS_WEBHOOK_URL overrides the file value.
func TestWebhookEnvOverride(t *testing.T) {
	t.Setenv("EVM_TOOLS_WEBHOOK_URL", "https://from-env/evm")
	p := writeConfig(t, `
[webhook]
url = "https://from-file/evm"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeWebhook(false)
	if err != nil {
		t.Fatalf("DecodeWebhook: %v", err)
	}
	if cfg.Webhook.URL != "https://from-env/evm" {
		t.Errorf("url = %q, want from-env (env should override file)", cfg.Webhook.URL)
	}
}

// TestWebhookAuthViaCmd verifies the auth header value can be sourced through the
// shared _cmd machinery (never hardcoded in the file).
func TestWebhookAuthViaCmd(t *testing.T) {
	p := writeConfig(t, `
[webhook]
url = "https://h/evm"

[webhook.auth]
header = "Authorization"
value_cmd = "printf 'Bearer secret-from-vault'"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeWebhook(true) // allowExec
	if err != nil {
		t.Fatalf("DecodeWebhook(allowExec): %v", err)
	}
	if cfg.Webhook.Auth.Value != "Bearer secret-from-vault" {
		t.Errorf("auth.value = %q, want Bearer secret-from-vault", cfg.Webhook.Auth.Value)
	}
}

// TestWebhookAuthCmdDisabledFatal verifies a value_cmd while exec is disabled is
// fatal (not a silent skip), so a secret is never quietly missing.
func TestWebhookAuthCmdDisabledFatal(t *testing.T) {
	p := writeConfig(t, `
[webhook]
url = "https://h/evm"

[webhook.auth]
header = "Authorization"
value_cmd = "printf x"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = l.DecodeWebhook(false)
	if err == nil || !strings.Contains(err.Error(), "command execution") {
		t.Fatalf("expected a disabled-exec error, got: %v", err)
	}
}
