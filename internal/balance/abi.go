package balance

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"golang.org/x/crypto/sha3"
)

// Function selectors for the read-only ERC view functions evm-balance calls.
// A selector is the first 4 bytes of the keccak-256 of the canonical function
// signature. They are computed once at package init so a typo is a programming
// error, caught by tests, not a silent miscall.
var (
	selDecimals    = functionSelector("decimals()")
	selTotalSupply = functionSelector("totalSupply()")
	selBalanceOf   = functionSelector("balanceOf(address)")
)

// functionSelector returns the 0x-prefixed 4-byte selector for a canonical
// function signature, e.g. "balanceOf(address)".
func functionSelector(sig string) string {
	h := sha3.NewLegacyKeccak256()
	h.Write([]byte(sig))
	sum := h.Sum(nil)
	return "0x" + hex.EncodeToString(sum[:4])
}

// callDataDecimals is the calldata for decimals().
func callDataDecimals() string { return selDecimals }

// callDataTotalSupply is the calldata for totalSupply().
func callDataTotalSupply() string { return selTotalSupply }

// callDataBalanceOf encodes balanceOf(address): the selector followed by the
// 32-byte left-padded holder address.
func callDataBalanceOf(addr string) string {
	return selBalanceOf + padAddress(addr)
}

// callDataOwnerOf is reserved for ERC-721 ownership (deferred per design); kept
// unused-safe by not defining it until that milestone.

// padAddress renders a 20-byte address as a 32-byte left-padded hex word
// (no 0x prefix). A malformed address yields zero padding; the call then
// returns whatever the contract does for the zero address, which the caller
// surfaces rather than panicking.
func padAddress(addr string) string {
	a := strings.ToLower(strings.TrimPrefix(strings.TrimPrefix(addr, "0x"), "0X"))
	if len(a) > 40 {
		a = a[len(a)-40:] // keep the low 20 bytes
	}
	return strings.Repeat("0", 64-len(a)) + a
}

// decodeUint256 decodes a 0x-hex eth_call result word into a *big.Int. An empty
// result ("0x" or "") is treated as an error since a working view function
// always returns a 32-byte word; a non-existent function reverts (an RPC error)
// rather than returning empty, but some nodes return "0x" for a missing method.
func decodeUint256(hexResult string) (*big.Int, error) {
	b, err := hexBytes(hexResult)
	if err != nil {
		return nil, fmt.Errorf("decode call result: %w", err)
	}
	if len(b) == 0 {
		return nil, fmt.Errorf("empty call result (function may not exist)")
	}
	// Take the last 32 bytes if the result is wider; a single uint256 occupies
	// exactly one word but be tolerant of left padding.
	if len(b) > 32 {
		b = b[len(b)-32:]
	}
	return new(big.Int).SetBytes(b), nil
}

// decodeUint8 decodes an eth_call result into a uint8 in [0,255] (for
// decimals()). A value outside that range is rejected.
func decodeUint8(hexResult string) (int, error) {
	v, err := decodeUint256(hexResult)
	if err != nil {
		return 0, err
	}
	if !v.IsInt64() || v.Int64() < 0 || v.Int64() > 255 {
		return 0, fmt.Errorf("decimals() out of range: %s", v.String())
	}
	return int(v.Int64()), nil
}

// hexBytes parses an optional-0x hex string into bytes.
func hexBytes(s string) ([]byte, error) {
	s = strings.TrimPrefix(strings.TrimPrefix(s, "0x"), "0X")
	if s == "" {
		return nil, nil
	}
	return hex.DecodeString(s)
}
