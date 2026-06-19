package stream

import "testing"

// transferTopic0 is the well-known keccak-256 of
// Transfer(address,address,uint256), shared by ERC-20 and ERC-721.
const transferTopic0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

func TestTopic0KnownVector(t *testing.T) {
	got := Topic0("Transfer(address,address,uint256)")
	if got != transferTopic0 {
		t.Errorf("Topic0(Transfer) = %s, want %s", got, transferTopic0)
	}
	// Approval(address,address,uint256) canonical topic0.
	const approvalTopic0 = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"
	if got := Topic0("Approval(address,address,uint256)"); got != approvalTopic0 {
		t.Errorf("Topic0(Approval) = %s, want %s", got, approvalTopic0)
	}
}

func TestParseBuiltinABIs(t *testing.T) {
	evs := builtinEvents()
	if len(evs) == 0 {
		t.Fatal("no built-in events parsed")
	}
	// Find the ERC-20 Transfer and verify its canonical signature/topic0.
	var found bool
	for _, e := range evs {
		if e.Name == "Transfer" && e.Signature == "Transfer(address,address,uint256)" {
			found = true
			if e.Topic0 != transferTopic0 {
				t.Errorf("Transfer topic0 = %s", e.Topic0)
			}
		}
	}
	if !found {
		t.Error("built-in Transfer event not found")
	}
}

func TestCanonicalSignatureTuple(t *testing.T) {
	inputs := []abiInput{
		{Name: "a", Type: "tuple", Components: []abiInput{
			{Name: "x", Type: "uint256"},
			{Name: "y", Type: "address"},
		}},
		{Name: "b", Type: "uint8"},
	}
	got := canonicalSignature("Settled", inputs)
	want := "Settled((uint256,address),uint8)"
	if got != want {
		t.Errorf("canonicalSignature = %q, want %q", got, want)
	}
}

func TestParseSignature(t *testing.T) {
	ev, err := parseSignature("Settled", "Settled(address,uint256,bytes32)")
	if err != nil {
		t.Fatalf("parseSignature: %v", err)
	}
	if ev.Signature != "Settled(address,uint256,bytes32)" {
		t.Errorf("signature = %q", ev.Signature)
	}
	if len(ev.Inputs) != 3 {
		t.Errorf("inputs = %d, want 3", len(ev.Inputs))
	}
	if ev.Topic0 != Topic0("Settled(address,uint256,bytes32)") {
		t.Errorf("topic0 mismatch")
	}
}

func TestParseSignatureInvalid(t *testing.T) {
	if _, err := parseSignature("X", "not a signature"); err == nil {
		t.Error("expected error for malformed signature")
	}
}
