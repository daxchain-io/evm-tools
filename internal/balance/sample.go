package balance

import (
	"context"
	"fmt"
	"math/big"

	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// readNative reads an account's native (wei) balance and returns a balance_sample
// (kind native) reading; its applyChange yields a balance_change when the balance
// moved since the last tick.
func (p *Poller) readNative(ctx context.Context, t NativeTarget, head uint64, tag, ts, blockHash string) (reading, error) {
	wei, err := p.opts.Client.BalanceAt(ctx, t.Address, tag)
	if err != nil {
		return reading{}, fmt.Errorf("native balance %q: %w", t.Name, err)
	}

	p.opts.Metrics.SetAccountBalanceWei(t.Name, lower(t.Address), wei)
	p.opts.Metrics.SetAccountBalanceEth(t.Name, lower(t.Address), bigToFloat(wei, 18))

	data := record.BalanceData{
		Kind:       record.KindNative,
		Address:    t.Address,
		BalanceWei: wei.String(),
		Balance:    weiToEther(wei),
	}
	sample := p.buildEnv(record.TypeBalanceSample, t.Name, head, blockHash, ts, data)
	key := "native:" + t.Name
	apply := func() (record.Envelope, bool, func()) {
		commit := func() { p.commitValue(key, wei) }
		moved, prev := p.peekChanged(key, wei)
		if !moved {
			return record.Envelope{}, false, commit
		}
		cd := data
		cd.PreviousWei = prev.String()
		return p.buildEnv(record.TypeBalanceChange, t.Name, head, blockHash, ts, cd), true, commit
	}
	return reading{sample: sample, applyChange: apply}, nil
}

// readERC20 reads an ERC-20 holder balance via balanceOf and returns a
// balance_sample (kind erc20) reading; its applyChange yields a balance_change
// when it moved. Human-readable Balance/decimals are populated only when decimals
// are known.
func (p *Poller) readERC20(ctx context.Context, t ERC20Target, head uint64, tag, ts, blockHash string) (reading, error) {
	raw, err := p.opts.Client.Call(ctx, rpc.CallMsg{To: t.Token, Data: callDataBalanceOf(t.Address)}, tag)
	if err != nil {
		return reading{}, fmt.Errorf("erc20 balanceOf %q: %w", t.Name, err)
	}
	val, err := decodeUint256(raw)
	if err != nil {
		return reading{}, fmt.Errorf("erc20 balanceOf %q: %w", t.Name, err)
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

	sample := p.buildEnv(record.TypeBalanceSample, t.Name, head, blockHash, ts, data)
	key := "erc20:" + t.Name
	apply := func() (record.Envelope, bool, func()) {
		commit := func() { p.commitValue(key, val) }
		moved, prev := p.peekChanged(key, val)
		if !moved {
			return record.Envelope{}, false, commit
		}
		cd := data
		cd.PreviousRaw = prev.String()
		return p.buildEnv(record.TypeBalanceChange, t.Name, head, blockHash, ts, cd), true, commit
	}
	return reading{sample: sample, applyChange: apply}, nil
}

// readContract reads the configured fields of a contract and returns one reading
// per active field (contract_sample, with a contract_change applied on movement).
func (p *Poller) readContract(ctx context.Context, t ContractTarget, head uint64, tag, ts, blockHash string) ([]reading, error) {
	var out []reading
	if t.NativeBalance {
		r, err := p.readContractNative(ctx, t, head, tag, ts, blockHash)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if t.TokenSupply {
		r, err := p.readContractSupply(ctx, t, head, tag, ts, blockHash)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if t.TransferCountWindowBlocks > 0 {
		r, err := p.readContractTransferCount(ctx, t, head, ts, blockHash)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

func (p *Poller) readContractNative(ctx context.Context, t ContractTarget, head uint64, tag, ts, blockHash string) (reading, error) {
	wei, err := p.opts.Client.BalanceAt(ctx, t.Address, tag)
	if err != nil {
		return reading{}, fmt.Errorf("contract native balance %q: %w", t.Name, err)
	}
	p.opts.Metrics.SetContractBalanceWei(t.Name, lower(t.Address), wei)
	p.opts.Metrics.SetContractBalanceEth(t.Name, lower(t.Address), bigToFloat(wei, 18))

	data := record.ContractData{
		Address:    t.Address,
		Field:      record.FieldNativeBalance,
		BalanceWei: wei.String(),
		Balance:    weiToEther(wei),
	}
	sample := p.buildEnv(record.TypeContractSample, t.Name, head, blockHash, ts, data)
	key := "contract-native:" + t.Name
	apply := func() (record.Envelope, bool, func()) {
		commit := func() { p.commitValue(key, wei) }
		moved, prev := p.peekChanged(key, wei)
		if !moved {
			return record.Envelope{}, false, commit
		}
		cd := data
		cd.PreviousWei = prev.String()
		return p.buildEnv(record.TypeContractChange, t.Name, head, blockHash, ts, cd), true, commit
	}
	return reading{sample: sample, applyChange: apply}, nil
}

func (p *Poller) readContractSupply(ctx context.Context, t ContractTarget, head uint64, tag, ts, blockHash string) (reading, error) {
	raw, err := p.opts.Client.Call(ctx, rpc.CallMsg{To: t.Address, Data: callDataTotalSupply()}, tag)
	if err != nil {
		return reading{}, fmt.Errorf("contract totalSupply %q: %w", t.Name, err)
	}
	val, err := decodeUint256(raw)
	if err != nil {
		return reading{}, fmt.Errorf("contract totalSupply %q: %w", t.Name, err)
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

	sample := p.buildEnv(record.TypeContractSample, t.Name, head, blockHash, ts, data)
	key := "contract-supply:" + t.Name
	apply := func() (record.Envelope, bool, func()) {
		commit := func() { p.commitValue(key, val) }
		moved, prev := p.peekChanged(key, val)
		if !moved {
			return record.Envelope{}, false, commit
		}
		cd := data
		cd.PreviousTotalSupply = prev.String()
		return p.buildEnv(record.TypeContractChange, t.Name, head, blockHash, ts, cd), true, commit
	}
	return reading{sample: sample, applyChange: apply}, nil
}

// readContractTransferCount counts Transfer logs emitted by the contract over the
// trailing window [head-window+1, head] and returns a contract_sample with field
// transfer_count. window_blocks is the configured window width.
func (p *Poller) readContractTransferCount(ctx context.Context, t ContractTarget, head uint64, ts, blockHash string) (reading, error) {
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
		return reading{}, fmt.Errorf("contract transfer_count %q: %w", t.Name, err)
	}
	count := big.NewInt(int64(len(logs)))
	wb := int(window)

	p.opts.Metrics.SetContractTransferCount(t.Name, lower(t.Address), window, float64(len(logs)))

	data := record.ContractData{
		Address:      t.Address,
		Field:        record.FieldTransferCount,
		Count:        count.String(),
		WindowBlocks: &wb,
	}
	sample := p.buildEnv(record.TypeContractSample, t.Name, head, blockHash, ts, data)
	key := "contract-transfers:" + t.Name
	apply := func() (record.Envelope, bool, func()) {
		commit := func() { p.commitValue(key, count) }
		moved, prev := p.peekChanged(key, count)
		if !moved {
			return record.Envelope{}, false, commit
		}
		cd := data
		cd.PreviousCount = prev.String()
		return p.buildEnv(record.TypeContractChange, t.Name, head, blockHash, ts, cd), true, commit
	}
	return reading{sample: sample, applyChange: apply}, nil
}

// buildEnv constructs one record envelope for the balance tool. Sampled records
// carry block_hash for provenance.
func (p *Poller) buildEnv(typ record.Type, name string, head uint64, blockHash, ts string, data any) record.Envelope {
	return record.Envelope{
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
