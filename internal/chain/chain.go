// Package chain will hold chain metadata and block/header helpers (chain ID
// resolution, block lookups) shared by the producers.
//
// The real helpers land in milestone M1 (see docs/plan.md); this file scaffolds
// the package so the layout is complete.
package chain

import "errors"

// ErrNotImplemented is returned by chain helpers that are scaffolded but not yet
// built. It is replaced by real logic in M1.
var ErrNotImplemented = errors.New("chain: not implemented")
