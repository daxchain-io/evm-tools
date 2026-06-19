// Package metrics will hold the Prometheus registry, the HTTP server that
// exposes it, and the /healthz + /readyz endpoints shared by the tools.
//
// The registry, metric sets, and health server land in milestone M1 (see
// docs/plan.md). This file scaffolds the package so the layout is complete.
package metrics

import "errors"

// ErrNotImplemented is returned by metrics entry points that are scaffolded but
// not yet built. It is replaced by the real registry/server in M1.
var ErrNotImplemented = errors.New("metrics: not implemented")
