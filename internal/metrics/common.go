package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/daxchain-io/evm-tools/internal/buildinfo"
)

// registerCommon registers the standard cross-tool metrics on a tool's private
// registry: the Go runtime collector (go_goroutines, go_memstats_*, go_gc_*),
// the process collector (process_resident_memory_bytes, process_cpu_seconds_total,
// process_open_fds, …), and a build_info gauge. Because each suite uses its own
// registry rather than the default one, these standard series are otherwise
// absent — so an OOM, goroutine/FD leak, GC pressure, or CPU saturation would be
// invisible from the endpoint. namespace is the per-tool metric prefix (e.g.
// "evm_stream"); base carries the shared blockchain/chain_id const labels so the
// build_info series lines up with the rest of the suite on a dashboard.
//
// The process collector reports nothing on platforms without /proc (e.g. macOS);
// it is silent there and fully populated in Linux containers — the production
// target. Called once per constructor, so the collectors register exactly once
// on each fresh registry.
func registerCommon(reg *prometheus.Registry, namespace string, base prometheus.Labels) {
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	bi := buildinfo.Get()
	labels := make(prometheus.Labels, len(base)+3)
	for k, v := range base {
		labels[k] = v
	}
	labels["version"] = bi.Version
	labels["commit"] = bi.Commit
	labels["go_version"] = bi.GoVersion

	info := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        namespace + "_build_info",
		Help:        "Build metadata of the running binary; constant 1 with version/commit/go_version in labels.",
		ConstLabels: labels,
	})
	reg.MustRegister(info)
	info.Set(1)
}
