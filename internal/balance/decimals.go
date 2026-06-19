package balance

import (
	"context"

	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// resolveDecimals resolves and caches token decimals once at startup, per
// design.md ("Token decimals"): for each ERC-20 holder and each contract with
// token_supply enabled, an explicit config override wins; otherwise eth_call
// decimals() is consulted. A token that omits decimals() (the call errors or
// returns empty) and has no override is left unresolved — only raw values are
// emitted for it, with a single stderr warning. The result is written back onto
// the target's Decimals field so the sample path needs no per-tick lookup.
//
// decimals() is read at the latest block; it is effectively immutable, so it is
// resolved once and cached for the run.
func (p *Poller) resolveDecimals(ctx context.Context) {
	// Cache per token address so two entries for the same token resolve once.
	cache := map[string]*int{}

	resolve := func(token string, override *int) *int {
		if override != nil {
			return override
		}
		key := lower(token)
		if d, ok := cache[key]; ok {
			return d
		}
		d := p.fetchDecimals(ctx, token)
		cache[key] = d
		return d
	}

	for i := range p.opts.ERC20 {
		t := &p.opts.ERC20[i]
		d := resolve(t.Token, t.Decimals)
		if d == nil && t.Decimals == nil {
			p.log.Warn("token does not implement decimals() and no override set; emitting raw values only",
				"name", t.Name, "token", lower(t.Token))
		}
		t.Decimals = d
	}

	for i := range p.opts.Contracts {
		t := &p.opts.Contracts[i]
		if !t.TokenSupply {
			continue
		}
		d := resolve(t.Address, t.Decimals)
		if d == nil && t.Decimals == nil {
			p.log.Warn("contract does not implement decimals() and no override set; emitting raw total_supply only",
				"name", t.Name, "contract", lower(t.Address))
		}
		t.Decimals = d
	}
}

// fetchDecimals calls decimals() on a token. A revert/empty result or any RPC
// error yields nil (treated as "not implemented"); the caller warns once.
func (p *Poller) fetchDecimals(ctx context.Context, token string) *int {
	raw, err := p.opts.Client.Call(ctx, rpc.CallMsg{To: token, Data: callDataDecimals()}, "latest")
	if err != nil {
		p.log.Debug("decimals() call failed; treating as unset",
			"token", lower(token), "error_type", string(rpc.Classify(err)))
		return nil
	}
	d, err := decodeUint8(raw)
	if err != nil {
		p.log.Debug("decimals() returned an undecodable value; treating as unset",
			"token", lower(token))
		return nil
	}
	return &d
}
