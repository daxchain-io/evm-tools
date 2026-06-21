package config

import (
	"fmt"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

// sharedKeys are the top-level keys a tool decodes regardless of which tool it
// is. Sibling-tool sections (the other tool's namespace) are excluded so they
// are ignored rather than rejected by strict decoding.
var sharedKeys = map[string]bool{
	"chain":   true,
	"rpc":     true,
	"metrics": true,
	"log":     true,
}

// streamTarget is the decode shape for evm-stream: shared keys squashed onto
// the top level plus the [stream] subtree.
type streamTarget struct {
	Shared `mapstructure:",squash"`
	Stream StreamConfig `mapstructure:"stream"`
}

// balanceTarget is the decode shape for evm-balance.
type balanceTarget struct {
	Shared  `mapstructure:",squash"`
	Balance BalanceConfig `mapstructure:"balance"`
}

// kafkaTarget is the decode shape for evm-sink-kafka: shared keys squashed onto
// the top level plus the [kafka] subtree.
type kafkaTarget struct {
	Shared `mapstructure:",squash"`
	Kafka  KafkaConfig `mapstructure:"kafka"`
}

// webhookTarget is the decode shape for evm-sink-webhook: shared keys squashed
// onto the top level plus the [webhook] subtree.
type webhookTarget struct {
	Shared  `mapstructure:",squash"`
	Webhook WebhookConfig `mapstructure:"webhook"`
}

// fileTarget is the decode shape for evm-sink-file: shared keys squashed onto
// the top level plus the [file] subtree.
type fileTarget struct {
	Shared `mapstructure:",squash"`
	File   FileConfig `mapstructure:"file"`
}

// awsSQSTarget / awsSNSTarget are the decode shapes for the AWS sinks.
type awsSQSTarget struct {
	Shared `mapstructure:",squash"`
	AWSSQS AWSSQSConfig `mapstructure:"aws_sqs"`
}

type awsSNSTarget struct {
	Shared `mapstructure:",squash"`
	AWSSNS AWSSNSConfig `mapstructure:"aws_sns"`
}

// postgresTarget is the decode shape for evm-sink-postgres.
type postgresTarget struct {
	Shared   `mapstructure:",squash"`
	Postgres PostgresConfig `mapstructure:"postgres"`
}

// redisTarget is the decode shape for evm-sink-redis.
type redisTarget struct {
	Shared `mapstructure:",squash"`
	Redis  RedisConfig `mapstructure:"redis"`
}

// sharedTarget decodes only the shared top-level keys (chain/rpc/metrics/log).
type sharedTarget struct {
	Shared `mapstructure:",squash"`
}

// DecodeShared strict-decodes only the shared keys (no tool subtree), applying the
// same precedence and ${VAR}/_cmd interpolation as the per-tool decoders. It is
// used to resolve cross-tool settings such as [log] consistently at startup and on
// reload, so every path sees the same final values.
func (l *Loader) DecodeShared(allowExec bool) (*Shared, error) {
	var t sharedTarget
	if err := l.strictDecode("", &t, allowExec); err != nil {
		return nil, err
	}
	t.AllowExec = allowExec
	return &t.Shared, nil
}

// DecodeStream strict-decodes the shared keys plus the [stream] subtree into a
// StreamFull. Unknown keys within those sections are a fatal error; the
// [balance] section is ignored.
func (l *Loader) DecodeStream(allowExec bool) (*StreamFull, error) {
	var t streamTarget
	if err := l.strictDecode("stream", &t, allowExec); err != nil {
		return nil, err
	}
	t.AllowExec = allowExec
	return &StreamFull{Shared: t.Shared, Stream: t.Stream}, nil
}

// DecodeBalance strict-decodes the shared keys plus the [balance] subtree into
// a BalanceFull. Unknown keys within those sections are a fatal error; the
// [stream] section is ignored.
func (l *Loader) DecodeBalance(allowExec bool) (*BalanceFull, error) {
	var t balanceTarget
	if err := l.strictDecode("balance", &t, allowExec); err != nil {
		return nil, err
	}
	t.AllowExec = allowExec
	return &BalanceFull{Shared: t.Shared, Balance: t.Balance}, nil
}

// DecodeKafka strict-decodes the shared keys plus the [kafka] subtree into a
// KafkaFull. Unknown keys within those sections are a fatal error; sibling-tool
// sections ([stream]/[balance]) are ignored. A sink needs the shared
// [metrics]/[log] plus its own [kafka] section, not [rpc]/[chain] — those, if
// present in a shared file, decode harmlessly into the shared struct.
func (l *Loader) DecodeKafka(allowExec bool) (*KafkaFull, error) {
	var t kafkaTarget
	if err := l.strictDecode("kafka", &t, allowExec); err != nil {
		return nil, err
	}
	t.AllowExec = allowExec
	return &KafkaFull{Shared: t.Shared, Kafka: t.Kafka}, nil
}

// DecodeWebhook strict-decodes the shared keys plus the [webhook] subtree into a
// WebhookFull. Unknown keys within those sections are a fatal error; sibling-tool
// sections ([stream]/[balance]/[kafka]) are ignored. A sink needs the shared
// [metrics]/[log] plus its own [webhook] section, not [rpc]/[chain].
func (l *Loader) DecodeWebhook(allowExec bool) (*WebhookFull, error) {
	var t webhookTarget
	if err := l.strictDecode("webhook", &t, allowExec); err != nil {
		return nil, err
	}
	t.AllowExec = allowExec
	return &WebhookFull{Shared: t.Shared, Webhook: t.Webhook}, nil
}

// DecodeFile strict-decodes the shared keys plus the [file] subtree into a
// FileFull. Unknown keys within those sections are a fatal error; sibling-tool
// sections ([stream]/[balance]/[kafka]/[webhook]) are ignored. A sink needs the
// shared [metrics]/[log] plus its own [file] section, not [rpc]/[chain].
func (l *Loader) DecodeFile(allowExec bool) (*FileFull, error) {
	var t fileTarget
	if err := l.strictDecode("file", &t, allowExec); err != nil {
		return nil, err
	}
	t.AllowExec = allowExec
	return &FileFull{Shared: t.Shared, File: t.File}, nil
}

// DecodeAWSSQS strict-decodes the shared keys plus the [aws_sqs] subtree.
func (l *Loader) DecodeAWSSQS(allowExec bool) (*AWSSQSFull, error) {
	var t awsSQSTarget
	if err := l.strictDecode("aws_sqs", &t, allowExec); err != nil {
		return nil, err
	}
	t.AllowExec = allowExec
	return &AWSSQSFull{Shared: t.Shared, AWSSQS: t.AWSSQS}, nil
}

// DecodeAWSSNS strict-decodes the shared keys plus the [aws_sns] subtree.
func (l *Loader) DecodeAWSSNS(allowExec bool) (*AWSSNSFull, error) {
	var t awsSNSTarget
	if err := l.strictDecode("aws_sns", &t, allowExec); err != nil {
		return nil, err
	}
	t.AllowExec = allowExec
	return &AWSSNSFull{Shared: t.Shared, AWSSNS: t.AWSSNS}, nil
}

// DecodePostgres strict-decodes the shared keys plus the [postgres] subtree.
func (l *Loader) DecodePostgres(allowExec bool) (*PostgresFull, error) {
	var t postgresTarget
	if err := l.strictDecode("postgres", &t, allowExec); err != nil {
		return nil, err
	}
	t.AllowExec = allowExec
	return &PostgresFull{Shared: t.Shared, Postgres: t.Postgres}, nil
}

// DecodeRedis strict-decodes the shared keys plus the [redis] subtree.
func (l *Loader) DecodeRedis(allowExec bool) (*RedisFull, error) {
	var t redisTarget
	if err := l.strictDecode("redis", &t, allowExec); err != nil {
		return nil, err
	}
	t.AllowExec = allowExec
	return &RedisFull{Shared: t.Shared, Redis: t.Redis}, nil
}

// strictDecode builds a settings map containing only the shared keys and the
// named tool subtree, then decodes it with ErrorUnused so a typo in the tool's
// own section (or a shared section) fails fast. Sibling-tool sections never
// enter the map, so they are silently ignored as required.
func (l *Loader) strictDecode(toolKey string, target any, allowExec bool) error {
	all := l.v.AllSettings()
	subset := make(map[string]any, len(all))
	for k, val := range all {
		if sharedKeys[k] || k == toolKey {
			subset[k] = val
		}
	}

	// Resolve ${...} interpolation and _cmd execution on the subset before
	// strict decoding, so consumers see only final values.
	if err := l.resolve(subset, allowExec); err != nil {
		return err
	}

	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:           target,
		ErrorUnused:      true, // typos in our own subtree are fatal
		WeaklyTypedInput: true, // TOML/env give us strings for some scalars
		TagName:          "mapstructure",
	})
	if err != nil {
		return fmt.Errorf("build decoder: %w", err)
	}
	if err := dec.Decode(subset); err != nil {
		return fmt.Errorf("decode %s config: %w", toolKey, err)
	}
	return nil
}

// Viper exposes the underlying *viper.Viper for callers that need raw access
// (e.g. tests). Most code should use the typed decoders.
func (l *Loader) Viper() *viper.Viper {
	return l.v
}
