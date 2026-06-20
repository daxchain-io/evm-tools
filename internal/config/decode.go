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
