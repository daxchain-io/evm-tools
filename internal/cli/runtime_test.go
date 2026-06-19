package cli

import (
	"testing"

	"github.com/daxchain-io/evm-tools/internal/config"
)

func boolPtr(b bool) *bool { return &b }

// TestResolveMetricsPrecedence exercises the design's precedence:
// flags > tool-specific [<tool>.metrics] > shared [metrics] > defaults.
// The load-bearing case is shared-enabled + tool-disabled: a tool-specific
// section must be able to *override* (disable) an endpoint the shared section
// enabled, not only enable one. That is why MetricsConfig.Enabled is a *bool.
func TestResolveMetricsPrecedence(t *testing.T) {
	const defAddr = ":9000"

	tests := []struct {
		name        string
		shared      config.MetricsConfig
		tool        config.MetricsConfig
		cmd         commandFlags
		flags       sharedFlags
		wantEnabled bool
		wantAddr    string
		wantPath    string
	}{
		{
			name:        "defaults only",
			wantEnabled: false,
			wantAddr:    defAddr,
			wantPath:    "/metrics",
		},
		{
			name:        "shared enables, tool unset -> enabled",
			shared:      config.MetricsConfig{Enabled: boolPtr(true)},
			wantEnabled: true,
			wantAddr:    defAddr,
			wantPath:    "/metrics",
		},
		{
			name:        "shared disabled, tool enables -> enabled (canonical)",
			shared:      config.MetricsConfig{Enabled: boolPtr(false)},
			tool:        config.MetricsConfig{Enabled: boolPtr(true), Addr: ":9100"},
			wantEnabled: true,
			wantAddr:    ":9100",
			wantPath:    "/metrics",
		},
		{
			// The bug this fix targets: with OR semantics this could not disable.
			name:        "shared enabled, tool disables -> disabled",
			shared:      config.MetricsConfig{Enabled: boolPtr(true)},
			tool:        config.MetricsConfig{Enabled: boolPtr(false)},
			wantEnabled: false,
			wantAddr:    defAddr,
			wantPath:    "/metrics",
		},
		{
			name:        "flag disables over tool+shared enabled",
			shared:      config.MetricsConfig{Enabled: boolPtr(true)},
			tool:        config.MetricsConfig{Enabled: boolPtr(true)},
			cmd:         commandFlags{metricsChanged: true},
			flags:       sharedFlags{metricsEnabled: false},
			wantEnabled: false,
			wantAddr:    defAddr,
			wantPath:    "/metrics",
		},
		{
			name:        "flag enables over tool+shared disabled",
			shared:      config.MetricsConfig{Enabled: boolPtr(false)},
			tool:        config.MetricsConfig{Enabled: boolPtr(false)},
			cmd:         commandFlags{metricsChanged: true},
			flags:       sharedFlags{metricsEnabled: true},
			wantEnabled: true,
			wantAddr:    defAddr,
			wantPath:    "/metrics",
		},
		{
			name:        "addr/path precedence: flag > tool > shared",
			shared:      config.MetricsConfig{Addr: ":1", Path: "/s"},
			tool:        config.MetricsConfig{Addr: ":2", Path: "/t"},
			cmd:         commandFlags{metricsAddrChanged: true},
			flags:       sharedFlags{metricsAddr: ":3"},
			wantEnabled: false,
			wantAddr:    ":3",
			wantPath:    "/t",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := tc.flags
			got := f.resolveMetrics(tc.cmd, tc.shared, tc.tool, defAddr)
			if got.Enabled != tc.wantEnabled {
				t.Errorf("Enabled = %v, want %v", got.Enabled, tc.wantEnabled)
			}
			if got.Addr != tc.wantAddr {
				t.Errorf("Addr = %q, want %q", got.Addr, tc.wantAddr)
			}
			if got.Path != tc.wantPath {
				t.Errorf("Path = %q, want %q", got.Path, tc.wantPath)
			}
		})
	}
}
