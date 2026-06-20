package cli

import (
	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// rpcTLSFromConfig maps the shared [rpc] config onto the transport's TLS config.
func rpcTLSFromConfig(c config.RPCConfig) rpc.TLSConfig {
	return rpc.TLSConfig{
		ClientCert:  c.ClientCert,
		ClientKey:   c.ClientKey,
		CACert:      c.CACert,
		ServerName:  c.ServerName,
		RequireMTLS: c.RequireMTLS,
	}
}

// resolvedMetrics is the metrics endpoint configuration after applying the
// design's precedence: flags > tool-specific [<tool>.metrics] > shared
// [metrics] > built-in defaults.
type resolvedMetrics struct {
	Enabled bool
	Addr    string
	Path    string
}

// resolveMetrics folds the shared and tool-specific metrics config plus the
// command-line flags into one resolved endpoint config. The flags win only when
// the user actually set them.
func (f *sharedFlags) resolveMetrics(cmd commandFlags, shared, tool config.MetricsConfig, defaultAddr string) resolvedMetrics {
	out := resolvedMetrics{
		Enabled: shared.IsEnabled(),
		Addr:    shared.Addr,
		Path:    shared.Path,
	}
	// Tool-specific section overrides shared. A *bool lets an explicit
	// tool-specific value win in either direction: it can disable an endpoint
	// the shared section enabled, not only enable one. An unset (nil) section
	// leaves the shared value in place.
	if tool.Enabled != nil {
		out.Enabled = *tool.Enabled
	}
	if tool.Addr != "" {
		out.Addr = tool.Addr
	}
	if tool.Path != "" {
		out.Path = tool.Path
	}
	// Built-in defaults fill gaps.
	if out.Addr == "" {
		out.Addr = defaultAddr
	}
	if out.Path == "" {
		out.Path = "/metrics"
	}
	// Flags win when explicitly set.
	if cmd.metricsChanged {
		out.Enabled = f.metricsEnabled
	}
	if cmd.metricsAddrChanged {
		out.Addr = f.metricsAddr
	}
	if cmd.metricsPathChanged {
		out.Path = f.metricsPath
	}
	return out
}

// commandFlags records which metrics flags the user explicitly changed, since
// the metrics enable/addr/path precedence depends on it.
type commandFlags struct {
	metricsChanged     bool
	metricsAddrChanged bool
	metricsPathChanged bool
}

// resolveSinkMetrics is the sinkFlags analogue of resolveMetrics: it folds the
// shared and tool-specific metrics config plus the command-line flags into one
// resolved endpoint config for a sink (which has no RPC surface but the same
// metrics precedence: flags > [<sink>.metrics] > [metrics] > defaults).
func (f *sinkFlags) resolveSinkMetrics(cmd commandFlags, shared, tool config.MetricsConfig, defaultAddr string) resolvedMetrics {
	out := resolvedMetrics{
		Enabled: shared.IsEnabled(),
		Addr:    shared.Addr,
		Path:    shared.Path,
	}
	if tool.Enabled != nil {
		out.Enabled = *tool.Enabled
	}
	if tool.Addr != "" {
		out.Addr = tool.Addr
	}
	if tool.Path != "" {
		out.Path = tool.Path
	}
	if out.Addr == "" {
		out.Addr = defaultAddr
	}
	if out.Path == "" {
		out.Path = "/metrics"
	}
	if cmd.metricsChanged {
		out.Enabled = f.metricsEnabled
	}
	if cmd.metricsAddrChanged {
		out.Addr = f.metricsAddr
	}
	if cmd.metricsPathChanged {
		out.Path = f.metricsPath
	}
	return out
}
