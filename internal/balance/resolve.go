package balance

import (
	"fmt"
	"time"

	"github.com/daxchain-io/evm-tools/internal/config"
)

// Resolved holds the targets and cadence derived from a [balance] config
// section, ready to hand to New. It separates config parsing/validation (offline,
// usable by `validate`) from the running poller.
type Resolved struct {
	Cadence   Cadence
	Native    []NativeTarget
	ERC20     []ERC20Target
	Contracts []ContractTarget
}

// Resolve validates and converts a [balance] config section into the typed
// targets the poller consumes. It enforces the interval-XOR-every_blocks rule,
// requires at least one target, and rejects entries missing a name or address.
// ERC-721 balance/ownership entries are accepted by config decoding but their
// runtime is deferred (design Build Milestones), so they are not resolved here.
func Resolve(cfg config.BalanceConfig) (Resolved, error) {
	cad, err := resolveCadence(cfg)
	if err != nil {
		return Resolved{}, err
	}

	var out Resolved
	out.Cadence = cad

	for i, n := range cfg.Native {
		if n.Name == "" {
			return Resolved{}, fmt.Errorf("balance.native[%d]: missing name", i)
		}
		if n.Address == "" {
			return Resolved{}, fmt.Errorf("balance.native %q: missing address", n.Name)
		}
		out.Native = append(out.Native, NativeTarget{Name: n.Name, Address: n.Address})
	}

	for i, e := range cfg.ERC20 {
		if e.Name == "" {
			return Resolved{}, fmt.Errorf("balance.erc20[%d]: missing name", i)
		}
		if e.Token == "" {
			return Resolved{}, fmt.Errorf("balance.erc20 %q: missing token", e.Name)
		}
		if e.Address == "" {
			return Resolved{}, fmt.Errorf("balance.erc20 %q: missing address (holder)", e.Name)
		}
		if e.Decimals != nil && (*e.Decimals < 0 || *e.Decimals > 255) {
			return Resolved{}, fmt.Errorf("balance.erc20 %q: decimals out of range [0,255]", e.Name)
		}
		out.ERC20 = append(out.ERC20, ERC20Target{
			Name:     e.Name,
			Token:    e.Token,
			Address:  e.Address,
			Decimals: e.Decimals,
		})
	}

	for i, c := range cfg.Contracts {
		if c.Name == "" {
			return Resolved{}, fmt.Errorf("balance.contracts[%d]: missing name", i)
		}
		if c.Address == "" {
			return Resolved{}, fmt.Errorf("balance.contracts %q: missing address", c.Name)
		}
		if !c.NativeBalance && !c.TokenSupply && c.TransferCountWindowBlocks <= 0 {
			return Resolved{}, fmt.Errorf("balance.contracts %q: enable at least one of native_balance, token_supply, or transfer_count_window_blocks", c.Name)
		}
		if c.TransferCountWindowBlocks < 0 {
			return Resolved{}, fmt.Errorf("balance.contracts %q: transfer_count_window_blocks must be non-negative", c.Name)
		}
		if c.Decimals != nil && (*c.Decimals < 0 || *c.Decimals > 255) {
			return Resolved{}, fmt.Errorf("balance.contracts %q: decimals out of range [0,255]", c.Name)
		}
		out.Contracts = append(out.Contracts, ContractTarget{
			Name:                      c.Name,
			Address:                   c.Address,
			NativeBalance:             c.NativeBalance,
			TokenSupply:               c.TokenSupply,
			TransferCountWindowBlocks: uint64(c.TransferCountWindowBlocks),
			Decimals:                  c.Decimals,
		})
	}

	if len(out.Native) == 0 && len(out.ERC20) == 0 && len(out.Contracts) == 0 {
		return Resolved{}, fmt.Errorf("nothing to poll: configure [[balance.native]], [[balance.erc20]], or [[balance.contracts]]")
	}

	return out, nil
}

// resolveCadence parses and validates the sampling cadence: exactly one of
// interval (a duration string) or every_blocks (a positive count) must be set.
func resolveCadence(cfg config.BalanceConfig) (Cadence, error) {
	hasInterval := cfg.Interval != ""
	hasBlocks := cfg.EveryBlocks > 0

	switch {
	case hasInterval && hasBlocks:
		return Cadence{}, fmt.Errorf("set exactly one of balance.interval or balance.every_blocks, not both")
	case !hasInterval && !hasBlocks:
		return Cadence{}, fmt.Errorf("set exactly one of balance.interval or balance.every_blocks")
	case hasBlocks:
		if cfg.EveryBlocks < 0 {
			return Cadence{}, fmt.Errorf("balance.every_blocks must be positive (got %d)", cfg.EveryBlocks)
		}
		return Cadence{EveryBlocks: uint64(cfg.EveryBlocks)}, nil
	default:
		d, err := time.ParseDuration(cfg.Interval)
		if err != nil {
			return Cadence{}, fmt.Errorf("invalid balance.interval %q: %w", cfg.Interval, err)
		}
		if d <= 0 {
			return Cadence{}, fmt.Errorf("balance.interval must be positive (got %q)", cfg.Interval)
		}
		return Cadence{Interval: d}, nil
	}
}
