// Package stream holds the evm-stream core logic: event resolution and
// decoding, the HTTP poll loop, chunked eth_getLogs backfill, native transfer
// detection, and lossless emission through internal/record.
//
// The real monitoring logic lands in milestone M1 (see docs/plan.md). This file
// scaffolds the package so the layout is complete.
package stream

import "errors"

// ErrNotImplemented is returned by stream entry points that are scaffolded but
// not yet built. It is replaced by the real run loop in M1.
var ErrNotImplemented = errors.New("stream: not implemented")
