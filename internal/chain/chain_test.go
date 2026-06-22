package chain

import (
	"context"
	"testing"

	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// fakeReader satisfies Reader; only ChainID is exercised by these tests.
type fakeReader struct {
	id  int64
	err error
}

func (f fakeReader) ChainID(context.Context) (int64, error)      { return f.id, f.err }
func (f fakeReader) BlockNumber(context.Context) (uint64, error) { return 0, nil }
func (f fakeReader) BlockByNumberUint(context.Context, uint64, bool) (*rpc.Block, error) {
	return nil, nil
}
func (f fakeReader) GetLogs(context.Context, rpc.LogFilter) ([]rpc.Log, error) { return nil, nil }
func (f fakeReader) TransactionReceipt(context.Context, string) (*rpc.Receipt, error) {
	return nil, nil
}

func TestNameForID(t *testing.T) {
	cases := map[int64]string{
		1:        "ethereum",
		8453:     "base",
		42161:    "arbitrum",
		10:       "optimism",
		137:      "polygon",
		13371337: "chain-13371337", // unknown -> fallback, never blank
	}
	for id, want := range cases {
		if got := NameForID(id); got != want {
			t.Errorf("NameForID(%d) = %q, want %q", id, got, want)
		}
	}
}

func TestResolveDerivesNameWhenBlank(t *testing.T) {
	// A blank label is filled from the resolved chain ID.
	info, err := Resolve(context.Background(), fakeReader{id: 1}, "")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if info.Name != "ethereum" || info.ID != 1 {
		t.Errorf("blank label: got Name=%q ID=%d, want ethereum/1", info.Name, info.ID)
	}

	// Whitespace-only is treated as blank.
	if info, _ := Resolve(context.Background(), fakeReader{id: 8453}, "   "); info.Name != "base" {
		t.Errorf("whitespace label: got Name=%q, want base", info.Name)
	}

	// An unknown ID gets the chain-<id> fallback, never an empty label.
	if info, _ := Resolve(context.Background(), fakeReader{id: 424242}, ""); info.Name != "chain-424242" {
		t.Errorf("unknown id: got Name=%q, want chain-424242", info.Name)
	}
}

func TestResolveKeepsExplicitName(t *testing.T) {
	// An explicit label always wins, even on a known ID.
	info, err := Resolve(context.Background(), fakeReader{id: 1}, "my-mainnet")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if info.Name != "my-mainnet" {
		t.Errorf("explicit label: got Name=%q, want my-mainnet", info.Name)
	}
}
