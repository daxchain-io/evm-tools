package config

import (
	"strings"
	"testing"
)

const sampleKafkaConfig = `
chain = "my-chain"

[metrics]
enabled = false

[log]
level = "info"

[kafka]
brokers = ["broker1:9092", "broker2:9092"]
topic = "evm.events"
partition_key = "identity"

[kafka.topic_by_type]
event = "evm.events"
balance_sample = "evm.balances"

[kafka.sasl]
mechanism = "scram-sha-256"
username = "evm-tools"

[kafka.tls]
enabled = true
ca_cert = "/certs/kafka-ca.crt"

[kafka.metrics]
enabled = true
addr = ":9002"
`

func TestDecodeKafka(t *testing.T) {
	p := writeConfig(t, sampleKafkaConfig)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeKafka(false)
	if err != nil {
		t.Fatalf("DecodeKafka: %v", err)
	}
	if cfg.Chain != "my-chain" {
		t.Errorf("chain = %q", cfg.Chain)
	}
	if len(cfg.Kafka.Brokers) != 2 || cfg.Kafka.Brokers[0] != "broker1:9092" {
		t.Errorf("brokers = %v", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.Topic != "evm.events" {
		t.Errorf("topic = %q", cfg.Kafka.Topic)
	}
	if cfg.Kafka.PartitionKey != "identity" {
		t.Errorf("partition_key = %q", cfg.Kafka.PartitionKey)
	}
	if cfg.Kafka.TopicByType["balance_sample"] != "evm.balances" {
		t.Errorf("topic_by_type = %v", cfg.Kafka.TopicByType)
	}
	if cfg.Kafka.SASL.Mechanism != "scram-sha-256" || cfg.Kafka.SASL.Username != "evm-tools" {
		t.Errorf("sasl = %+v", cfg.Kafka.SASL)
	}
	if cfg.Kafka.TLS.Enabled == nil || !*cfg.Kafka.TLS.Enabled || cfg.Kafka.TLS.CACert != "/certs/kafka-ca.crt" {
		t.Errorf("tls = %+v", cfg.Kafka.TLS)
	}
	if !cfg.Kafka.Metrics.IsEnabled() || cfg.Kafka.Metrics.Addr != ":9002" {
		t.Errorf("kafka.metrics = %+v", cfg.Kafka.Metrics)
	}
}

// TestKafkaDefaultsApply verifies the built-in defaults fill required_acks,
// partition_key, backoff, and the metrics endpoint defaults.
func TestKafkaDefaultsApply(t *testing.T) {
	p := writeConfig(t, `
[kafka]
brokers = ["b:9092"]
topic = "t"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeKafka(false)
	if err != nil {
		t.Fatalf("DecodeKafka: %v", err)
	}
	if cfg.Kafka.PartitionKey != "identity" {
		t.Errorf("partition_key default = %q, want identity", cfg.Kafka.PartitionKey)
	}
	if cfg.Kafka.RequiredAcks != "all" {
		t.Errorf("required_acks default = %q, want all", cfg.Kafka.RequiredAcks)
	}
	if cfg.Kafka.BackoffBase != "500ms" || cfg.Kafka.BackoffMax != "30s" {
		t.Errorf("backoff defaults = %q/%q", cfg.Kafka.BackoffBase, cfg.Kafka.BackoffMax)
	}
	if cfg.Kafka.Metrics.Addr != ":9002" {
		t.Errorf("kafka.metrics.addr default = %q, want :9002", cfg.Kafka.Metrics.Addr)
	}
}

// TestKafkaSiblingSectionsIgnored verifies a sink ignores producer sections
// ([stream]/[balance]/[rpc]) rather than rejecting them, so one shared file
// serves the whole suite.
func TestKafkaSiblingSectionsIgnored(t *testing.T) {
	p := writeConfig(t, sampleConfig+`
[kafka]
brokers = ["b:9092"]
topic = "t"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeKafka(false)
	if err != nil {
		t.Fatalf("DecodeKafka should ignore sibling sections: %v", err)
	}
	if cfg.Kafka.Topic != "t" {
		t.Errorf("topic = %q", cfg.Kafka.Topic)
	}
}

// TestKafkaUnknownKeyFatal verifies a typo in the [kafka] section is a fatal
// strict-decode error.
func TestKafkaUnknownKeyFatal(t *testing.T) {
	p := writeConfig(t, `
[kafka]
brokers = ["b:9092"]
toppic = "typo"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := l.DecodeKafka(false); err == nil {
		t.Fatal("expected a strict-decode error for an unknown [kafka] key")
	}
}

// TestKafkaEnvOverride verifies EVM_TOOLS_KAFKA_TOPIC overrides the file value.
func TestKafkaEnvOverride(t *testing.T) {
	t.Setenv("EVM_TOOLS_KAFKA_TOPIC", "from-env")
	p := writeConfig(t, `
[kafka]
brokers = ["b:9092"]
topic = "from-file"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeKafka(false)
	if err != nil {
		t.Fatalf("DecodeKafka: %v", err)
	}
	if cfg.Kafka.Topic != "from-env" {
		t.Errorf("topic = %q, want from-env (env should override file)", cfg.Kafka.Topic)
	}
}

// TestKafkaPasswordViaCmd verifies the SASL password can be sourced through the
// shared _cmd machinery (never hardcoded in the file).
func TestKafkaPasswordViaCmd(t *testing.T) {
	p := writeConfig(t, `
[kafka]
brokers = ["b:9092"]
topic = "t"

[kafka.sasl]
mechanism = "plain"
username = "u"
password_cmd = "printf secret-from-vault"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg, err := l.DecodeKafka(true) // allowExec
	if err != nil {
		t.Fatalf("DecodeKafka(allowExec): %v", err)
	}
	if cfg.Kafka.SASL.Password != "secret-from-vault" {
		t.Errorf("password = %q, want secret-from-vault", cfg.Kafka.SASL.Password)
	}
}

// TestKafkaPasswordCmdDisabledFatal verifies a password_cmd while exec is
// disabled is fatal (not a silent skip), so a secret is never quietly missing.
func TestKafkaPasswordCmdDisabledFatal(t *testing.T) {
	p := writeConfig(t, `
[kafka]
brokers = ["b:9092"]
topic = "t"

[kafka.sasl]
mechanism = "plain"
username = "u"
password_cmd = "printf x"
`)
	l, err := New(Options{ConfigFile: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = l.DecodeKafka(false)
	if err == nil || !strings.Contains(err.Error(), "command execution") {
		t.Fatalf("expected a disabled-exec error, got: %v", err)
	}
}
