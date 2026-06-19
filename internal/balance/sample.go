package balance

import (
	"context"
	"fmt"
	"math/big"

	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// sampleNative reads an account's native (wei) balance and emits a
// balance_sample (kind native), plus a balance_change when it moved.
func (p *Poller) sampleNative(ctx context.Context, t NativeTarget, head uint64, tag, ts, blockHash string) error {
	wei, err := p.opts.Client.BalanceAt(ctx, t.Address, tag)
	if err != nil {
		return fmt.Errorf("native balance %q: %w", t.Name, err)
	}

	p.opts.Metrics.SetAccountBalanceWei(t.Name, lower(t.Address), wei)
	p.opts.Metrics.SetAccountBalanceEth(t.Name, lower(t.Address), bigToFloat(wei, 18))

	data := record.BalanceData{
		Kind:       record.KindNative,
		Address:    t.Address,
		BalanceWei: wei.String(),
		Balance:    weiToEther(wei),
	}
	if err := p.emit(record.TypeBalanceSample, t.Name, head, blockHash, ts, data); err != nil {
		return err
	}

	moved, prev := p.changed("native:"+t.Name, wei)
	if moved {
		cd := data
		cd.PreviousWei = prev.String()
		if err := p.emit(record.TypeBalanceChange, t.Name, head, blockHash, ts, cd); err != nil {
			return err
		}
	}
	return nil
}

// sampleERC20 reads an ERC-20 holder balance via balanceOf and emits a
// balance_sample (kind erc20), plus a balance_change when it moved. Human-
// readable Balance/decimals are emitted only when decimals are known.
func (p *Poller) sampleERC20(ctx context.Context, t ERC20Target, head uint64, tag, ts, blockHash string) error {
	raw, err := p.opts.Client.Call(ctx, rpc.CallMsg{To: t.Token, Data: callDataBalanceOf(t.Address)}, tag)
	if err != nil {
		return fmt.Errorf("erc20 balanceOf %q: %w", t.Name, err)
	}
	val, err := decodeUint256(raw)
	if err != nil {
		return fmt.Errorf("erc20 balanceOf %q: %w", t.Name, err)
	}

	p.opts.Metrics.SetAccountTokenBalanceRaw(t.Name, lower(t.Address), t.Name, lower(t.Token), val)

	data := record.BalanceData{
		Kind:       record.KindERC20,
		Address:    t.Address,
		Token:      t.Token,
		BalanceRaw: val.String(),
	}
	if t.Decimals != nil {
		d := *t.Decimals
		data.Decimals = &d
		data.Balance = formatUnits(val, d)
		p.opts.Metrics.SetAccountTokenBalance(t.Name, lower(t.Address), t.Name, lower(t.Token), bigToFloat(val, d))
	}

	if err := p.emit(record.TypeBalanceSample, t.Name, head, blockHash, ts, data); err != nil {
		return err
	}

	moved, prev := p.changed("erc20:"+t.Name, val)
	if moved {
		cd := data
		cd.PreviousRaw = prev.String()
		if err := p.emit(record.TypeBalanceChange, t.Name, head, blockHash, ts, cd); err != nil {
			return err
		}
	}
	return nil
}

// sampleContract reads the configured fields of a contract and emits a
// contract_sample (and contract_change on movement) per field.
func (p *Poller) sampleContract(ctx context.Context, t ContractTarget, head uint64, tag, ts, blockHash string) error {
	if t.NativeBalance {
		if err := p.sampleContractNative(ctx, t, head, tag, ts, blockHash); err != nil {
			return err
		}
	}
	if t.TokenSupply {
		if err := p.sampleContractSupply(ctx, t, head, tag, ts, blockHash); err != nil {
			return err
		}
	}
	if t.TransferCountWindowBlocks > 0 {
		if err := p.sampleContractTransferCount(ctx, t, head, ts, blockHash); err != nil {
			return err
		}
	}
	return nil
}

func (p *Poller) sampleContractNative(ctx context.Context, t ContractTarget, head uint64, tag, ts, blockHash string) error {
	wei, err := p.opts.Client.BalanceAt(ctx, t.Address, tag)
	if err != nil {
		return fmt.Errorf("contract native balance %q: %w", t.Name, err)
	}
	p.opts.Metrics.SetContractBalanceWei(t.Name, lower(t.Address), wei)
	p.opts.Metrics.SetContractBalanceEth(t.Name, lower(t.Address), bigToFloat(wei, 18))

	data := record.ContractData{
		Address:    t.Address,
		Field:      record.FieldNativeBalance,
		BalanceWei: wei.String(),
		Balance:    weiToEther(wei),
	}
	if err := p.emit(record.TypeContractSample, t.Name, head, blockHash, ts, data); err != nil {
		return err
	}
	moved, prev := p.changed("contract-native:"+t.Name, wei)
	if moved {
		cd := data
		cd.PreviousWei = prev.String()
		if err := p.emit(record.TypeContractChange, t.Name, head, blockHash, ts, cd); err != nil {
			return err
		}
	}
	return nil
}

func (p *Poller) sampleContractSupply(ctx context.Context, t ContractTarget, head uint64, tag, ts, blockHash string) error {
	raw, err := p.opts.Client.Call(ctx, rpc.CallMsg{To: t.Address, Data: callDataTotalSupply()}, tag)
	if err != nil {
		return fmt.Errorf("contract totalSupply %q: %w", t.Name, err)
	}
	val, err := decodeUint256(raw)
	if err != nil {
		return fmt.Errorf("contract totalSupply %q: %w", t.Name, err)
	}

	data := record.ContractData{
		Address:        t.Address,
		Field:          record.FieldTokenTotalSupply,
		TotalSupplyRaw: val.String(),
	}
	if t.Decimals != nil {
		d := *t.Decimals
		data.Decimals = &d
		data.TotalSupply = formatUnits(val, d)
		p.opts.Metrics.SetContractTokenTotalSupply(t.Name, lower(t.Address), bigToFloat(val, d))
	} else {
		// No decimals: still expose the raw supply as the gauge value.
		p.opts.Metrics.SetContractTokenTotalSupply(t.Name, lower(t.Address), bigToFloat(val, 0))
	}

	if err := p.emit(record.TypeContractSample, t.Name, head, blockHash, ts, data); err != nil {
		return err
	}
	moved, prev := p.changed("contract-supply:"+t.Name, val)
	if moved {
		cd := data
		cd.PreviousTotalSupply = prev.String()
		if err := p.emit(record.TypeContractChange, t.Name, head, blockHash, ts, cd); err != nil {
			return err
		}
	}
	return nil
}

// sampleContractTransferCount counts Transfer logs emitted by the contract over
// the trailing window [head-window+1, head] and emits a contract_sample with
// field transfer_count. window_blocks is the configured window width.
func (p *Poller) sampleContractTransferCount(ctx context.Context, t ContractTarget, head uint64, ts, blockHash string) error {
	window := t.TransferCountWindowBlocks
	var from uint64
	if head+1 > window {
		from = head - window + 1
	}
	logs, err := p.opts.Client.GetLogs(ctx, rpc.LogFilter{
		FromBlock: from,
		ToBlock:   head,
		Addresses: []string{lower(t.Address)},
		Topics:    []any{transferTopic0},
	})
	if err != nil {
		return fmt.Errorf("contract transfer_count %q: %w", t.Name, err)
	}
	count := big.NewInt(int64(len(logs)))
	wb := int(window)

	p.opts.Metrics.SetContractTransferCount(t.Name, lower(t.Address), float64(len(logs)))

	data := record.ContractData{
		Address:      t.Address,
		Field:        record.FieldTransferCount,
		Count:        count.String(),
		WindowBlocks: &wb,
	}
	if err := p.emit(record.TypeContractSample, t.Name, head, blockHash, ts, data); err != nil {
		return err
	}
	moved, prev := p.changed("contract-transfers:"+t.Name, count)
	if moved {
		cd := data
		cd.PreviousCount = prev.String()
		if err := p.emit(record.TypeContractChange, t.Name, head, blockHash, ts, cd); err != nil {
			return err
		}
	}
	return nil
}

// emit builds and emits one record envelope, counting it against the sample/
// change record metrics. Sampled records carry block_hash for provenance.
func (p *Poller) emit(typ record.Type, name string, head uint64, blockHash, ts string, data any) error {
	env := record.Envelope{
		Type:        typ,
		Tool:        record.ToolBalance,
		Name:        name,
		Chain:       p.opts.ChainName,
		ChainID:     p.opts.ChainID,
		BlockNumber: head,
		BlockHash:   blockHash,
		Timestamp:   ts,
		Data:        data,
	}
	if err := p.opts.Emitter.Emit(env); err != nil {
		return err
	}
	switch typ {
	case record.TypeBalanceChange, record.TypeContractChange, record.TypeOwnershipChange:
		p.opts.Metrics.IncChangeRecord()
	default:
		p.opts.Metrics.IncSampleRecord()
	}
	return nil
}

// bigToFloat converts an integer scaled by 10^decimals to a float64 for gauge
// reporting. Precision is lossy for very large values, which is acceptable for a
// metric (the exact value lives in the JSONL record); the wei gauge uses
// decimals 0 to report the raw integer as a float.
func bigToFloat(v *big.Int, decimals int) float64 {
	f := new(big.Float).SetInt(v)
	if decimals > 0 {
		scale := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil))
		f.Quo(f, scale)
	}
	out, _ := f.Float64()
	return out
}
