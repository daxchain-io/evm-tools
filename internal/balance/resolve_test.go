package balance

import (
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/config"
)

func TestResolveCadence(t *testing.T) {
	tests := []struct {
		name    string
		cfg     config.BalanceConfig
		wantErr bool
		check   func(t *testing.T, c Cadence)
	}{
		{
			name: "interval",
			cfg:  config.BalanceConfig{Interval: "1m", Native: []config.BalanceNative{{Name: "n", Address: "0x1"}}},
			check: func(t *testing.T, c Cadence) {
				if c.Interval != time.Minute || c.EveryBlocks != 0 {
					t.Errorf("interval cadence wrong: %+v", c)
				}
			},
		},
		{
			name: "every_blocks",
			cfg:  config.BalanceConfig{EveryBlocks: 50, Native: []config.BalanceNative{{Name: "n", Address: "0x1"}}},
			check: func(t *testing.T, c Cadence) {
				if c.EveryBlocks != 50 || c.Interval != 0 {
					t.Errorf("block cadence wrong: %+v", c)
				}
			},
		},
		{
			name:    "both set",
			cfg:     config.BalanceConfig{Interval: "1m", EveryBlocks: 50, Native: []config.BalanceNative{{Name: "n", Address: "0x1"}}},
			wantErr: true,
		},
		{
			name:    "neither set",
			cfg:     config.BalanceConfig{Native: []config.BalanceNative{{Name: "n", Address: "0x1"}}},
			wantErr: true,
		},
		{
			name:    "bad interval",
			cfg:     config.BalanceConfig{Interval: "soon", Native: []config.BalanceNative{{Name: "n", Address: "0x1"}}},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := Resolve(tc.cfg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.check != nil {
				tc.check(t, res.Cadence)
			}
		})
	}
}

func TestResolveRejectsEmptyAndBadEntries(t *testing.T) {
	cases := map[string]config.BalanceConfig{
		"no targets": {Interval: "1m"},
		"native missing address": {
			Interval: "1m",
			Native:   []config.BalanceNative{{Name: "n"}},
		},
		"erc20 missing token": {
			Interval: "1m",
			ERC20:    []config.BalanceERC20{{Name: "e", Address: "0xh"}},
		},
		"contract no fields enabled": {
			Interval:  "1m",
			Contracts: []config.BalanceContract{{Name: "c", Address: "0xc"}},
		},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Resolve(cfg); err == nil {
				t.Errorf("expected error for %q", name)
			}
		})
	}
}

func TestResolveContractFields(t *testing.T) {
	d := 6
	res, err := Resolve(config.BalanceConfig{
		Interval: "30s",
		Contracts: []config.BalanceContract{{
			Name:                      "usdc",
			Address:                   "0xC",
			TokenSupply:               true,
			TransferCountWindowBlocks: 1000,
			Decimals:                  &d,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Contracts) != 1 {
		t.Fatalf("want 1 contract, got %d", len(res.Contracts))
	}
	c := res.Contracts[0]
	if !c.TokenSupply || c.TransferCountWindowBlocks != 1000 || c.Decimals == nil || *c.Decimals != 6 {
		t.Errorf("contract target wrong: %+v", c)
	}
}
