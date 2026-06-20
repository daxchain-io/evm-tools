package config

import (
	"testing"
)

const sampleFileConfig = `
chain = "my-chain"

[metrics]
enabled = false

[log]
level = "info"

[file]
path = "/var/log/evm-tools/events.jsonl"
max_size_mb = 100
rotation_interval = "24h"
max_backups = 7
compress = true
fsync = true
backoff_base = "250ms"

[file.filters]
include_types = ["event", "native_transfer"]
exclude_names = ["noisy"]

[file.metrics]
enabled = true
addr = ":9004"
`

func TestDecodeFile(t *testing.T) {
	p := writeConfig(t, sampleFileConfig)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeFile(false)
	if err != nil {
		t.Fatalf("DecodeFile: %v", err)
	}
	if cfg.Chain != "my-chain" {
		t.Errorf("chain = %q", cfg.Chain)
	}
	if cfg.File.Path != "/var/log/evm-tools/events.jsonl" {
		t.Errorf("path = %q", cfg.File.Path)
	}
	if cfg.File.MaxSizeMB != 100 {
		t.Errorf("max_size_mb = %d", cfg.File.MaxSizeMB)
	}
	if cfg.File.RotationInterval != "24h" {
		t.Errorf("rotation_interval = %q", cfg.File.RotationInterval)
	}
	if cfg.File.MaxBackups != 7 {
		t.Errorf("max_backups = %d", cfg.File.MaxBackups)
	}
	if !cfg.File.Compress {
		t.Errorf("compress = %v, want true", cfg.File.Compress)
	}
	if !cfg.File.Fsync {
		t.Errorf("fsync = %v, want true", cfg.File.Fsync)
	}
	if cfg.File.BackoffBase != "250ms" {
		t.Errorf("backoff_base = %q", cfg.File.BackoffBase)
	}
	if len(cfg.File.Filters.IncludeTypes) != 2 || cfg.File.Filters.IncludeTypes[0] != "event" {
		t.Errorf("filters.include_types = %v", cfg.File.Filters.IncludeTypes)
	}
	if len(cfg.File.Filters.ExcludeNames) != 1 || cfg.File.Filters.ExcludeNames[0] != "noisy" {
		t.Errorf("filters.exclude_names = %v", cfg.File.Filters.ExcludeNames)
	}
	if !cfg.File.Metrics.IsEnabled() || cfg.File.Metrics.Addr != ":9004" {
		t.Errorf("file.metrics = %+v", cfg.File.Metrics)
	}
}

// TestFileDefaultsApply verifies the built-in defaults fill backoff and the
// metrics endpoint.
func TestFileDefaultsApply(t *testing.T) {
	p := writeConfig(t, `
[file]
path = "/tmp/events.jsonl"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeFile(false)
	if err != nil {
		t.Fatalf("DecodeFile: %v", err)
	}
	if cfg.File.BackoffBase != "500ms" || cfg.File.BackoffMax != "30s" {
		t.Errorf("backoff defaults = %q/%q", cfg.File.BackoffBase, cfg.File.BackoffMax)
	}
	if cfg.File.Metrics.Addr != ":9004" {
		t.Errorf("file.metrics.addr default = %q, want :9004", cfg.File.Metrics.Addr)
	}
}

// TestFileSiblingSectionsIgnored verifies a sink ignores producer/other-sink
// sections rather than rejecting them, so one shared file serves the whole suite.
func TestFileSiblingSectionsIgnored(t *testing.T) {
	p := writeConfig(t, sampleConfig+`
[kafka]
brokers = ["b:9092"]
topic = "t"

[webhook]
url = "https://h/evm"

[file]
path = "/tmp/events.jsonl"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeFile(false)
	if err != nil {
		t.Fatalf("DecodeFile should ignore sibling sections: %v", err)
	}
	if cfg.File.Path != "/tmp/events.jsonl" {
		t.Errorf("path = %q", cfg.File.Path)
	}
}

// TestFileUnknownKeyFatal verifies a typo in the [file] section (or its filters)
// is a fatal strict-decode error.
func TestFileUnknownKeyFatal(t *testing.T) {
	p := writeConfig(t, `
[file]
path = "/tmp/events.jsonl"
maxsize_mb = 10
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := l.DecodeFile(false); err == nil {
		t.Fatal("expected a strict-decode error for an unknown [file] key")
	}
}

// TestFileEnvOverride verifies EVM_TOOLS_FILE_PATH overrides the file value.
func TestFileEnvOverride(t *testing.T) {
	t.Setenv("EVM_TOOLS_FILE_PATH", "/from/env.jsonl")
	p := writeConfig(t, `
[file]
path = "/from/file.jsonl"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeFile(false)
	if err != nil {
		t.Fatalf("DecodeFile: %v", err)
	}
	if cfg.File.Path != "/from/env.jsonl" {
		t.Errorf("path = %q, want from-env (env should override file)", cfg.File.Path)
	}
}
