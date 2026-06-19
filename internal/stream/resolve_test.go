package stream

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/daxchain-io/evm-tools/internal/config"
)

func TestResolveBuiltinERC20(t *testing.T) {
	rcs, err := ResolveContracts([]config.StreamContract{{
		Name:    "usdc",
		Address: "0xAAA",
		Events:  []string{"Transfer", "Approval"},
	}})
	if err != nil {
		t.Fatalf("ResolveContracts: %v", err)
	}
	if len(rcs) != 1 {
		t.Fatalf("got %d contracts", len(rcs))
	}
	rc := rcs[0]
	if rc.Address != "0xaaa" {
		t.Errorf("address should be lowercased, got %q", rc.Address)
	}
	if _, ok := rc.byTopic0[transferTopic0]; !ok {
		t.Errorf("Transfer topic0 not indexed: %+v", rc.byTopic0)
	}
	if len(rc.events) != 2 {
		t.Errorf("expected 2 events, got %d", len(rc.events))
	}
}

func TestResolveUnknownEventFatal(t *testing.T) {
	_, err := ResolveContracts([]config.StreamContract{{
		Name:    "x",
		Address: "0x1",
		Events:  []string{"Frobnicate"},
	}})
	if err == nil || !strings.Contains(err.Error(), "no known signature") {
		t.Fatalf("expected unknown-event error, got %v", err)
	}
}

func TestResolveSignatureOverride(t *testing.T) {
	rcs, err := ResolveContracts([]config.StreamContract{{
		Name:       "proto",
		Address:    "0x2",
		Events:     []string{"Settled"},
		Signatures: map[string]string{"Settled": "Settled(address,uint256,bytes32)"},
	}})
	if err != nil {
		t.Fatalf("ResolveContracts: %v", err)
	}
	want := Topic0("Settled(address,uint256,bytes32)")
	if _, ok := rcs[0].byTopic0[want]; !ok {
		t.Errorf("override topic0 not indexed")
	}
}

func TestResolveABIFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "abi.json")
	abi := `[{"type":"event","name":"Ping","anonymous":false,"inputs":[{"name":"n","type":"uint256","indexed":false}]}]`
	if err := os.WriteFile(p, []byte(abi), 0o600); err != nil {
		t.Fatal(err)
	}
	rcs, err := ResolveContracts([]config.StreamContract{{
		Name:    "p",
		Address: "0x3",
		Events:  []string{"Ping"},
		ABIFile: p,
	}})
	if err != nil {
		t.Fatalf("ResolveContracts: %v", err)
	}
	if _, ok := rcs[0].byTopic0[Topic0("Ping(uint256)")]; !ok {
		t.Errorf("Ping topic0 not indexed from abi_file")
	}
}

func TestResolveBothABIAndFileFatal(t *testing.T) {
	_, err := ResolveContracts([]config.StreamContract{{
		Name:    "x",
		Address: "0x1",
		Events:  []string{"Transfer"},
		ABI:     "[]",
		ABIFile: "/tmp/x.json",
	}})
	if err == nil || !strings.Contains(err.Error(), "only one of abi or abi_file") {
		t.Fatalf("expected abi/abi_file conflict error, got %v", err)
	}
}

// TestResolveOverloadedAmbiguous verifies an overloaded name with no
// disambiguating signature is fatal. We use an inline ABI declaring two events
// with the same name.
func TestResolveOverloadedAmbiguous(t *testing.T) {
	abi := `[
      {"type":"event","name":"Log","anonymous":false,"inputs":[{"name":"a","type":"uint256","indexed":false}]},
      {"type":"event","name":"Log","anonymous":false,"inputs":[{"name":"a","type":"address","indexed":false}]}
    ]`
	_, err := ResolveContracts([]config.StreamContract{{
		Name:    "x",
		Address: "0x1",
		Events:  []string{"Log"},
		ABI:     abi,
	}})
	if err == nil || !strings.Contains(err.Error(), "overloaded") {
		t.Fatalf("expected overloaded error, got %v", err)
	}
}
