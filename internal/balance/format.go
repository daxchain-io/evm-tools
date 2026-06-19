package balance

import (
	"math/big"
	"strings"
)

// weiToEther renders a wei amount as an ether-decimal string (18 decimals).
func weiToEther(wei *big.Int) string {
	return formatUnits(wei, 18)
}

// formatUnits renders an integer amount scaled by 10^decimals as a decimal
// string. It trims trailing fractional zeros, leaving "x.0" for whole numbers.
// With decimals <= 0 the bare integer string is returned.
func formatUnits(amount *big.Int, decimals int) string {
	if decimals <= 0 {
		return amount.String()
	}
	neg := amount.Sign() < 0
	abs := new(big.Int).Abs(amount)

	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	intPart := new(big.Int)
	fracPart := new(big.Int)
	intPart.DivMod(abs, scale, fracPart)

	frac := fracPart.String()
	if len(frac) < decimals {
		frac = strings.Repeat("0", decimals-len(frac)) + frac
	}
	frac = strings.TrimRight(frac, "0")
	if frac == "" {
		frac = "0"
	}

	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	b.WriteString(intPart.String())
	b.WriteByte('.')
	b.WriteString(frac)
	return b.String()
}
