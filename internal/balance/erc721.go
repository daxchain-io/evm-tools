package balance

import (
	"context"
	"fmt"

	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// readERC721Balance reads an ERC-721 holder's token count via balanceOf(owner)
// and returns a balance_sample (kind erc721) reading carrying the count; its
// applyChange yields a balance_change when the count moves. ERC-721 records carry
// counts, not decimals, so no decimals resolution applies (see docs/design.md,
// "Token decimals").
func (p *Poller) readERC721Balance(ctx context.Context, t ERC721BalanceTarget, head uint64, tag, ts, blockHash string) (reading, error) {
	raw, err := p.opts.Client.Call(ctx, rpc.CallMsg{To: t.Token, Data: callDataBalanceOf(t.Owner)}, tag)
	if err != nil {
		return reading{}, fmt.Errorf("erc721 balanceOf %q: %w", t.Name, err)
	}
	count, err := decodeUint256(raw)
	if err != nil {
		return reading{}, fmt.Errorf("erc721 balanceOf %q: %w", t.Name, err)
	}

	// Reuse the exporter-aligned token-balance gauge: an ERC-721 count is a raw
	// token balance with no decimals, so the raw gauge carries the count.
	p.opts.Metrics.SetAccountTokenBalanceRaw(t.Name, lower(t.Owner), t.Name, lower(t.Token), count)

	data := record.BalanceData{
		Kind:    record.KindERC721,
		Address: t.Owner,
		Token:   t.Token,
		Count:   count.String(),
	}
	sample := p.buildEnv(record.TypeBalanceSample, t.Name, head, blockHash, ts, data)
	key := "erc721-balance:" + t.Name
	apply := func() (record.Envelope, bool, func()) {
		commit := func() { p.commitValue(key, count) }
		moved, prev := p.peekChanged(key, count)
		if !moved {
			return record.Envelope{}, false, commit
		}
		cd := data
		cd.PreviousCount = prev.String()
		return p.buildEnv(record.TypeBalanceChange, t.Name, head, blockHash, ts, cd), true, commit
	}
	return reading{sample: sample, applyChange: apply}, nil
}

// readERC721Ownership reads ownerOf(tokenID) on an ERC-721 token and returns an
// ownership_sample reading carrying the current owner; its applyChange yields an
// ownership_change (with the previous owner) when ownership moves. The configured
// token ID is carried verbatim in the record.
func (p *Poller) readERC721Ownership(ctx context.Context, t ERC721OwnershipTarget, head uint64, tag, ts, blockHash string) (reading, error) {
	raw, err := p.opts.Client.Call(ctx, rpc.CallMsg{To: t.Token, Data: callDataOwnerOf(t.TokenID)}, tag)
	if err != nil {
		return reading{}, fmt.Errorf("erc721 ownerOf %q: %w", t.Name, err)
	}
	owner, err := decodeAddress(raw)
	if err != nil {
		return reading{}, fmt.Errorf("erc721 ownerOf %q: %w", t.Name, err)
	}

	data := record.OwnershipData{
		Kind:    record.KindERC721,
		Token:   t.Token,
		TokenID: t.TokenID,
		Owner:   owner,
	}
	sample := p.buildEnv(record.TypeOwnershipSample, t.Name, head, blockHash, ts, data)
	key := "erc721-ownership:" + t.Name
	apply := func() (record.Envelope, bool, func()) {
		commit := func() { p.commitOwner(key, owner) }
		moved, prev := p.peekOwnerChanged(key, owner)
		if !moved {
			return record.Envelope{}, false, commit
		}
		cd := data
		cd.PreviousOwner = prev
		return p.buildEnv(record.TypeOwnershipChange, t.Name, head, blockHash, ts, cd), true, commit
	}
	return reading{sample: sample, applyChange: apply}, nil
}
