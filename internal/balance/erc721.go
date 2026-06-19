package balance

import (
	"context"
	"fmt"

	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// sampleERC721Balance reads an ERC-721 holder's token count via balanceOf(owner)
// and emits a balance_sample (kind erc721) carrying the count, plus a
// balance_change when the count moves. ERC-721 records carry counts, not
// decimals, so no decimals resolution applies (see docs/design.md, "Token
// decimals").
func (p *Poller) sampleERC721Balance(ctx context.Context, t ERC721BalanceTarget, head uint64, tag, ts, blockHash string) error {
	raw, err := p.opts.Client.Call(ctx, rpc.CallMsg{To: t.Token, Data: callDataBalanceOf(t.Owner)}, tag)
	if err != nil {
		return fmt.Errorf("erc721 balanceOf %q: %w", t.Name, err)
	}
	count, err := decodeUint256(raw)
	if err != nil {
		return fmt.Errorf("erc721 balanceOf %q: %w", t.Name, err)
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
	if err := p.emit(record.TypeBalanceSample, t.Name, head, blockHash, ts, data); err != nil {
		return err
	}

	moved, prev := p.changed("erc721-balance:"+t.Name, count)
	if moved {
		cd := data
		cd.PreviousCount = prev.String()
		if err := p.emit(record.TypeBalanceChange, t.Name, head, blockHash, ts, cd); err != nil {
			return err
		}
	}
	return nil
}

// sampleERC721Ownership reads ownerOf(tokenID) on an ERC-721 token and emits an
// ownership_sample carrying the current owner, plus an ownership_change (with the
// previous owner) when ownership moves. The configured token ID is carried
// verbatim in the record.
func (p *Poller) sampleERC721Ownership(ctx context.Context, t ERC721OwnershipTarget, head uint64, tag, ts, blockHash string) error {
	raw, err := p.opts.Client.Call(ctx, rpc.CallMsg{To: t.Token, Data: callDataOwnerOf(t.TokenID)}, tag)
	if err != nil {
		return fmt.Errorf("erc721 ownerOf %q: %w", t.Name, err)
	}
	owner, err := decodeAddress(raw)
	if err != nil {
		return fmt.Errorf("erc721 ownerOf %q: %w", t.Name, err)
	}

	data := record.OwnershipData{
		Kind:    record.KindERC721,
		Token:   t.Token,
		TokenID: t.TokenID,
		Owner:   owner,
	}
	if err := p.emit(record.TypeOwnershipSample, t.Name, head, blockHash, ts, data); err != nil {
		return err
	}

	moved, prev := p.ownerChanged("erc721-ownership:"+t.Name, owner)
	if moved {
		cd := data
		cd.PreviousOwner = prev
		if err := p.emit(record.TypeOwnershipChange, t.Name, head, blockHash, ts, cd); err != nil {
			return err
		}
	}
	return nil
}
