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

// DecodeStream strict-decodes the shared keys plus the [stream] subtree into a
// StreamFull. Unknown keys within those sections are a fatal error; the
// [balance] section is ignored.
func (l *Loader) DecodeStream(allowExec bool) (*StreamFull, error) {
	var t streamTarget
	if err := l.strictDecode("stream", &t); err != nil {
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
	if err := l.strictDecode("balance", &t); err != nil {
		return nil, err
	}
	t.AllowExec = allowExec
	return &BalanceFull{Shared: t.Shared, Balance: t.Balance}, nil
}

// strictDecode builds a settings map containing only the shared keys and the
// named tool subtree, then decodes it with ErrorUnused so a typo in the tool's
// own section (or a shared section) fails fast. Sibling-tool sections never
// enter the map, so they are silently ignored as required.
func (l *Loader) strictDecode(toolKey string, target any) error {
	all := l.v.AllSettings()
	subset := make(map[string]any, len(all))
	for k, val := range all {
		if sharedKeys[k] || k == toolKey {
			subset[k] = val
		}
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
