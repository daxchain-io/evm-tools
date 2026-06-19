package balance

import (
	"math/big"
	"testing"
)

// TestKnownSelectors pins the function selectors against the well-known 4-byte
// signatures so a keccak/encoding regression is caught immediately.
func TestKnownSelectors(t *testing.T) {
	cases := map[string]string{
		"decimals()":         "0x313ce567",
		"totalSupply()":      "0x18160ddd",
		"balanceOf(address)": "0x70a08231",
	}
	for sig, want := range cases {
		if got := functionSelector(sig); got != want {
			t.Errorf("selector(%q) = %s, want %s", sig, got, want)
		}
	}
}

// TestCallDataBalanceOf verifies balanceOf encodes the selector plus a 32-byte
// left-padded holder address.
func TestCallDataBalanceOf(t *testing.T) {
	got := callDataBalanceOf("0x000000000000000000000000000000000000dEaD")
	want := "0x70a08231" + "000000000000000000000000000000000000000000000000000000000000dead"
	if got != want {
		t.Errorf("callDataBalanceOf = %s, want %s", got, want)
	}
}

func TestDecodeUint256(t *testing.T) {
	got, err := decodeUint256("0x0000000000000000000000000000000000000000000000000000000000001312d0")
	if err != nil {
		t.Fatal(err)
	}
	if got.Cmp(big.NewInt(1_250_000)) != 0 {
		t.Errorf("decodeUint256 = %s, want 1250000", got)
	}

	if _, err := decodeUint256("0x"); err == nil {
		t.Error("empty result should error")
	}
}

func TestDecodeUint8(t *testing.T) {
	got, err := decodeUint8("0x0000000000000000000000000000000000000000000000000000000000000006")
	if err != nil {
		t.Fatal(err)
	}
	if got != 6 {
		t.Errorf("decodeUint8 = %d, want 6", got)
	}

	// Out of range (>255) is rejected.
	if _, err := decodeUint8("0x0000000000000000000000000000000000000000000000000000000000000100"); err == nil {
		t.Error("decimals > 255 should error")
	}
}

func TestFormatUnits(t *testing.T) {
	cases := []struct {
		amount   int64
		decimals int
		want     string
	}{
		{2_000_000, 6, "2.0"},
		{1_250_000, 6, "1.25"},
		{1, 18, "0.000000000000000001"},
		{0, 6, "0.0"},
		{50_000_000, 0, "50000000"},
	}
	for _, tc := range cases {
		if got := formatUnits(big.NewInt(tc.amount), tc.decimals); got != tc.want {
			t.Errorf("formatUnits(%d, %d) = %s, want %s", tc.amount, tc.decimals, got, tc.want)
		}
	}
}
