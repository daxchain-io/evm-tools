// Package balance holds the evm-balance core logic: native/ERC-20/ERC-721
// balance polling, contract-state observation, decimals resolution, sampling
// cadence, change detection, and emission through internal/record.
//
// The real polling logic lands in milestone M2 (see docs/plan.md). This file
// scaffolds the package so the layout is complete.
package balance

import "errors"

// ErrNotImplemented is returned by balance entry points that are scaffolded but
// not yet built. It is replaced by the real poll loop in M2.
var ErrNotImplemented = errors.New("balance: not implemented")
