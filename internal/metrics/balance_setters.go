package metrics

import (
	"math/big"
	"time"

	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// SetUp marks the process available (true) or not.
func (b *Balance) SetUp(up bool) { b.up.Set(b2f(up)) }

// SetConfiguredNative records the number of configured native accounts.
func (b *Balance) SetConfiguredNative(n int) { b.configuredNative.Set(float64(n)) }

// SetConfiguredERC20 records the number of configured ERC-20 accounts.
func (b *Balance) SetConfiguredERC20(n int) { b.configuredERC20.Set(float64(n)) }

// SetConfiguredContracts records the number of configured contract-state entries.
func (b *Balance) SetConfiguredContracts(n int) { b.configuredContract.Set(float64(n)) }

// SetWorkers records the active poller worker count.
func (b *Balance) SetWorkers(n int) { b.workers.Set(float64(n)) }

// SetHead records the latest RPC head block number.
func (b *Balance) SetHead(n uint64) { b.headBlock.Set(float64(n)) }

// SetHeadBlockTime records the head block timestamp and its wall-clock age.
func (b *Balance) SetHeadBlockTime(t time.Time, now time.Time) {
	b.headBlockTimestamp.Set(float64(t.Unix()))
	age := now.Sub(t).Seconds()
	if age < 0 {
		age = 0
	}
	b.timeSinceLastBlock.Set(age)
}

// SetLastSampledBlock records the highest block sampled.
func (b *Balance) SetLastSampledBlock(n uint64) { b.lastSampledBlock.Set(float64(n)) }

// SetLagBlocks records head-minus-sampled lag.
func (b *Balance) SetLagBlocks(lag uint64) { b.lagBlocks.Set(float64(lag)) }

// SetEmitBlockedSeconds records how long the current/last stdout write blocked.
func (b *Balance) SetEmitBlockedSeconds(sec float64) { b.emitBlockedSeconds.Set(sec) }

// IncSampleRecord counts one emitted *_sample record.
func (b *Balance) IncSampleRecord() {
	b.recordsEmitted.Inc()
	b.sampleRecords.Inc()
}

// IncChangeRecord counts one emitted *_change record.
func (b *Balance) IncChangeRecord() {
	b.recordsEmitted.Inc()
	b.changeRecords.Inc()
}

// SetAccountBalanceWei records a native account balance in wei.
func (b *Balance) SetAccountBalanceWei(accountName, accountAddr string, wei *big.Int) {
	b.accountBalanceWei.WithLabelValues(accountName, accountAddr).Set(bigToFloat(wei))
}

// SetAccountBalanceEth records a native account balance in ether.
func (b *Balance) SetAccountBalanceEth(accountName, accountAddr string, eth float64) {
	b.accountBalanceEth.WithLabelValues(accountName, accountAddr).Set(eth)
}

// SetAccountTokenBalanceRaw records an ERC-20 account balance in raw units.
func (b *Balance) SetAccountTokenBalanceRaw(accountName, accountAddr, tokenName, tokenAddr string, raw *big.Int) {
	b.accountTokenBalRaw.WithLabelValues(accountName, accountAddr, tokenName, tokenAddr).Set(bigToFloat(raw))
}

// SetAccountTokenBalance records an ERC-20 account balance in whole tokens.
func (b *Balance) SetAccountTokenBalance(accountName, accountAddr, tokenName, tokenAddr string, bal float64) {
	b.accountTokenBalance.WithLabelValues(accountName, accountAddr, tokenName, tokenAddr).Set(bal)
}

// SetContractBalanceWei records a contract native balance in wei.
func (b *Balance) SetContractBalanceWei(contractName, contractAddr string, wei *big.Int) {
	b.contractBalanceWei.WithLabelValues(contractName, contractAddr).Set(bigToFloat(wei))
}

// SetContractBalanceEth records a contract native balance in ether.
func (b *Balance) SetContractBalanceEth(contractName, contractAddr string, eth float64) {
	b.contractBalanceEth.WithLabelValues(contractName, contractAddr).Set(eth)
}

// SetContractTokenTotalSupply records a contract token total supply.
func (b *Balance) SetContractTokenTotalSupply(contractName, contractAddr string, supply float64) {
	b.contractTokenSupply.WithLabelValues(contractName, contractAddr).Set(supply)
}

// SetContractTransferCount records the transfers observed in a contract window.
func (b *Balance) SetContractTransferCount(contractName, contractAddr string, count float64) {
	b.contractTransferCount.WithLabelValues(contractName, contractAddr).Set(count)
}

// IncReconnects counts an RPC reconnect after a transport error.
func (b *Balance) IncReconnects() { b.reconnects.Inc() }

// ObserveLoop records one sampling-loop duration.
func (b *Balance) ObserveLoop(d time.Duration) { b.loopDuration.Observe(d.Seconds()) }

// SetConsecutiveFailures records the current consecutive failure count.
func (b *Balance) SetConsecutiveFailures(n int) { b.consecutiveFail.Set(float64(n)) }

// SetBackoffSeconds records the current retry backoff.
func (b *Balance) SetBackoffSeconds(d time.Duration) { b.backoffSeconds.Set(d.Seconds()) }

// RPCObserver returns an rpc.CallObserver that records call duration and, on
// failure, increments the coarse-typed error counter. Plug it into rpc.Options.
func (b *Balance) RPCObserver() rpc.CallObserver {
	return func(operation string, dur time.Duration, et rpc.ErrorType) {
		b.rpcCallDuration.WithLabelValues(operation).Observe(dur.Seconds())
		if et != rpc.ErrorNone {
			b.rpcError.WithLabelValues(operation, string(et)).Inc()
		}
	}
}

// bigToFloat converts an integer to a float64 for gauge reporting. Precision is
// lossy for very large values, which is acceptable for a metric (the exact value
// lives in the JSONL record).
func bigToFloat(v *big.Int) float64 {
	f := new(big.Float).SetInt(v)
	out, _ := f.Float64()
	return out
}
