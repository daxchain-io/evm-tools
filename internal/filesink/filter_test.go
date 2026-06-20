package filesink

import (
	"testing"

	"github.com/daxchain-io/evm-tools/internal/record"
)

func env(typ, name string) record.Envelope {
	return record.Envelope{Type: record.Type(typ), Name: name}
}

func TestFilterAllowAllByDefault(t *testing.T) {
	f := NewFilter(FilterOptions{})
	if !f.Allow(env("event", "USDT")) {
		t.Error("empty filter should allow everything")
	}
}

func TestFilterIncludeTypes(t *testing.T) {
	f := NewFilter(FilterOptions{IncludeTypes: []string{"event"}})
	if !f.Allow(env("event", "USDT")) {
		t.Error("event should be allowed by include_types")
	}
	if f.Allow(env("native_transfer", "ETH")) {
		t.Error("native_transfer should be excluded by include_types allowlist")
	}
}

func TestFilterExcludeTypes(t *testing.T) {
	f := NewFilter(FilterOptions{ExcludeTypes: []string{"balance_sample"}})
	if f.Allow(env("balance_sample", "x")) {
		t.Error("balance_sample should be denied by exclude_types")
	}
	if !f.Allow(env("event", "USDT")) {
		t.Error("event should pass when only balance_sample is excluded")
	}
}

func TestFilterIncludeExcludeNames(t *testing.T) {
	f := NewFilter(FilterOptions{IncludeNames: []string{"USDT", "USDC"}, ExcludeNames: []string{"USDC"}})
	if !f.Allow(env("event", "USDT")) {
		t.Error("USDT should be allowed")
	}
	// Exclude takes effect even within the allowlist.
	if f.Allow(env("event", "USDC")) {
		t.Error("USDC should be denied by exclude_names despite include_names")
	}
	if f.Allow(env("event", "DAI")) {
		t.Error("DAI not in include_names should be denied")
	}
}
