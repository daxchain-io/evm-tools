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
	selOwnerOf     = functionSelector("ownerOf(uint256)")
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

// callDataOwnerOf encodes ownerOf(uint256): the selector followed by the
// 32-byte big-endian token ID. tokenID is a decimal or 0x-hex string; a value
// that does not parse yields a zero-padded word, and the call then returns
// whatever the contract does for token 0 (typically a revert the caller
// surfaces) rather than panicking.
func callDataOwnerOf(tokenID string) string {
	return selOwnerOf + padUint256(tokenID)
}

// padUint256 renders a token ID (decimal, or 0x-hex) as a 32-byte big-endian
// hex word (no 0x prefix). An unparseable value yields the zero word.
func padUint256(tokenID string) string {
	id, ok := parseBigInt(tokenID)
	if !ok || id.Sign() < 0 {
		id = big.NewInt(0)
	}
	h := id.Text(16)
	if len(h) > 64 {
		h = h[len(h)-64:] // keep the low 256 bits
	}
	return strings.Repeat("0", 64-len(h)) + h
}

// parseBigInt parses a token ID expressed as a 0x-hex or decimal string.
func parseBigInt(s string) (*big.Int, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		return new(big.Int).SetString(s[2:], 16)
	}
	return new(big.Int).SetString(s, 10)
}

// decodeAddress decodes a 32-byte eth_call result word into a checksum-agnostic
// lowercased 0x-hex address (the low 20 bytes). An empty result is an error
// since a working ownerOf() always returns a 32-byte word.
func decodeAddress(hexResult string) (string, error) {
	b, err := hexBytes(hexResult)
	if err != nil {
		return "", fmt.Errorf("decode call result: %w", err)
	}
	if len(b) == 0 {
		return "", &permanentErr{err: errEmptyCallResult}
	}
	if len(b) > 32 {
		b = b[len(b)-32:]
	}
	// The address occupies the low 20 bytes of the 32-byte word.
	addr := b
	if len(addr) > 20 {
		addr = addr[len(addr)-20:]
	}
	return "0x" + hex.EncodeToString(addr), nil
}

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
		return nil, &permanentErr{err: errEmptyCallResult}
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
