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

// TestOwnerOfSelector pins ownerOf(uint256) against its well-known selector.
func TestOwnerOfSelector(t *testing.T) {
	if got := functionSelector("ownerOf(uint256)"); got != "0x6352211e" {
		t.Errorf("selector(ownerOf(uint256)) = %s, want 0x6352211e", got)
	}
}

// TestCallDataOwnerOf verifies ownerOf encodes the selector plus a 32-byte
// big-endian token ID, accepting both decimal and 0x-hex IDs.
func TestCallDataOwnerOf(t *testing.T) {
	want := "0x6352211e" + "00000000000000000000000000000000000000000000000000000000000004d2"
	if got := callDataOwnerOf("1234"); got != want {
		t.Errorf("callDataOwnerOf(decimal) = %s, want %s", got, want)
	}
	if got := callDataOwnerOf("0x4d2"); got != want {
		t.Errorf("callDataOwnerOf(hex) = %s, want %s", got, want)
	}
}

// TestParseBigInt covers decimal, hex, and invalid token IDs.
func TestParseBigInt(t *testing.T) {
	if v, ok := parseBigInt("1234"); !ok || v.Cmp(big.NewInt(1234)) != 0 {
		t.Errorf("parseBigInt(decimal) = %v, %v", v, ok)
	}
	if v, ok := parseBigInt("0xff"); !ok || v.Cmp(big.NewInt(255)) != 0 {
		t.Errorf("parseBigInt(hex) = %v, %v", v, ok)
	}
	if _, ok := parseBigInt("0xZZ"); ok {
		t.Error("parseBigInt should reject non-hex digits")
	}
	if _, ok := parseBigInt("not-a-number"); ok {
		t.Error("parseBigInt should reject non-numeric strings")
	}
	if _, ok := parseBigInt(""); ok {
		t.Error("parseBigInt should reject empty string")
	}
}

// TestDecodeAddress decodes the low 20 bytes of a 32-byte word into a lowercased
// 0x address and rejects an empty result.
func TestDecodeAddress(t *testing.T) {
	got, err := decodeAddress("0x000000000000000000000000000000000000000000000000000000000000dEaD")
	if err != nil {
		t.Fatal(err)
	}
	if got != "0x000000000000000000000000000000000000dead" {
		t.Errorf("decodeAddress = %s", got)
	}
	if _, err := decodeAddress("0x"); err == nil {
		t.Error("empty result should error")
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
