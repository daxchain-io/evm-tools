package balance

import "context"

// watchSet is a re-decoded target configuration staged for a hot reload. Cadence
// and connection-level settings are intentionally not reloadable (a ticker /
// client change needs a restart); only the watched target lists are swapped.
type watchSet struct {
	native    []NativeTarget
	erc20     []ERC20Target
	contracts []ContractTarget
	erc721Bal []ERC721BalanceTarget
	erc721Own []ERC721OwnershipTarget
}

// QueueReload stages a re-decoded target set to be applied at the top of the next
// sampling iteration. It is safe to call from the SIGHUP handler goroutine: it
// only stores the staged set under reloadMu; the Run goroutine performs the swap.
// A second reload before the first is consumed supersedes it (last one wins).
func (p *Poller) QueueReload(
	native []NativeTarget, erc20 []ERC20Target, contracts []ContractTarget,
	erc721Bal []ERC721BalanceTarget, erc721Own []ERC721OwnershipTarget,
) {
	p.reloadMu.Lock()
	p.pendingWatch = &watchSet{
		native: native, erc20: erc20, contracts: contracts,
		erc721Bal: erc721Bal, erc721Own: erc721Own,
	}
	p.reloadMu.Unlock()
}

// applyPendingReload swaps in a staged target set (if any) at a safe point on the
// Run goroutine, between sampling ticks: it resets the gauge series and clears the
// change-detection state for targets the reload removed, swaps the target lists,
// re-resolves decimals for any newly added ERC-20/contract targets, and counts
// the reload. A reload never changes the cadence or the RPC connection.
func (p *Poller) applyPendingReload(ctx context.Context) {
	p.reloadMu.Lock()
	ws := p.pendingWatch
	p.pendingWatch = nil
	p.reloadMu.Unlock()
	if ws == nil {
		return
	}

	p.resetRemoved(ws)

	p.opts.Native = ws.native
	p.opts.ERC20 = ws.erc20
	p.opts.Contracts = ws.contracts
	p.opts.ERC721Balances = ws.erc721Bal
	p.opts.ERC721Ownership = ws.erc721Own

	// Resolve decimals for any newly added ERC-20/contract targets (a no-op for
	// those that already carry a configured override).
	p.resolveDecimals(ctx)

	p.opts.Metrics.IncConfigReload()
	p.log.Info("config reloaded (SIGHUP): watched target set updated",
		"native", len(ws.native), "erc20", len(ws.erc20), "contracts", len(ws.contracts),
		"erc721_balances", len(ws.erc721Bal), "erc721_ownership", len(ws.erc721Own),
		"note", "cadence/connection changes still require a restart")
}

// resetRemoved drops the gauge series and change-detection entries for every
// target present in the running set but absent from the staged set, so a removed
// target leaves no stale metric or change baseline behind. Targets are diffed by
// configured name within each type (the metric/change-detection identity).
func (p *Poller) resetRemoved(ws *watchSet) {
	p.mu.Lock()
	defer p.mu.Unlock()

	newNative := names(len(ws.native), func(i int) string { return ws.native[i].Name })
	for _, t := range p.opts.Native {
		if !newNative[t.Name] {
			p.opts.Metrics.ResetAccountSeries(t.Name, lower(t.Address))
			delete(p.prior, "native:"+t.Name)
		}
	}

	newERC20 := names(len(ws.erc20), func(i int) string { return ws.erc20[i].Name })
	for _, t := range p.opts.ERC20 {
		if !newERC20[t.Name] {
			p.opts.Metrics.ResetAccountTokenSeries(t.Name, lower(t.Address), t.Name, lower(t.Token))
			delete(p.prior, "erc20:"+t.Name)
		}
	}

	newContracts := names(len(ws.contracts), func(i int) string { return ws.contracts[i].Name })
	newContractWindow := make(map[string]uint64, len(ws.contracts))
	for _, t := range ws.contracts {
		newContractWindow[t.Name] = t.TransferCountWindowBlocks
	}
	for _, t := range p.opts.Contracts {
		if !newContracts[t.Name] {
			p.opts.Metrics.ResetContractSeries(t.Name, lower(t.Address))
			delete(p.prior, "contract-native:"+t.Name)
			delete(p.prior, "contract-supply:"+t.Name)
			delete(p.prior, "contract-transfers:"+t.Name)
			continue
		}
		// A surviving contract whose transfer-count window changed would otherwise
		// strand its old window_blocks label variant as a never-updated series; drop
		// the contract's series so the next sample re-creates it under the current
		// window. (The balance/supply gauges re-populate on that same tick.)
		if nw := newContractWindow[t.Name]; nw != t.TransferCountWindowBlocks {
			p.opts.Metrics.ResetContractSeries(t.Name, lower(t.Address))
		}
	}

	newERC721Bal := names(len(ws.erc721Bal), func(i int) string { return ws.erc721Bal[i].Name })
	for _, t := range p.opts.ERC721Balances {
		if !newERC721Bal[t.Name] {
			p.opts.Metrics.ResetAccountTokenSeries(t.Name, lower(t.Owner), t.Name, lower(t.Token))
			delete(p.prior, "erc721-balance:"+t.Name)
		}
	}

	newERC721Own := names(len(ws.erc721Own), func(i int) string { return ws.erc721Own[i].Name })
	for _, t := range p.opts.ERC721Ownership {
		if !newERC721Own[t.Name] {
			delete(p.priorOwner, "erc721-ownership:"+t.Name)
		}
	}
}

// names builds a set of the configured names in a target slice via an indexed
// accessor (the slices have no common element type).
func names(n int, name func(i int) string) map[string]bool {
	set := make(map[string]bool, n)
	for i := range n {
		set[name(i)] = true
	}
	return set
}
