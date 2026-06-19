// Package rpc will provide the shared HTTPS+mTLS JSON-RPC transport and client
// used by every tool in the suite (normal runs, balance polling, backfills, and
// health checks).
//
// The mTLS transport, JSON-RPC client, fail-fast certificate validation, and
// URL redaction land in milestone M1 (see docs/plan.md). This file currently
// holds only the shared error sentinel so callers can reference the package.
package rpc

import "errors"

// ErrNotImplemented is returned by RPC entry points that are scaffolded but not
// yet built. It is replaced by real transport logic in M1.
var ErrNotImplemented = errors.New("rpc: not implemented")
