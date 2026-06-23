package record

import (
	"strings"
	"testing"
)

// TestInternalTransferDedupKey verifies sibling internal transfers in one tx get
// distinct, reorg-stable keys via trace_address, while the top-level
// native_transfer key for the same tx is unchanged (no third component).
func TestInternalTransferDedupKey(t *testing.T) {
	base := Envelope{ChainID: 1, TxHash: "0xtx"}

	native := base
	native.Type = TypeNativeTransfer
	if got, want := native.DedupKey(), "1|0xtx"; got != want {
		t.Errorf("native_transfer key = %q, want %q (unchanged)", got, want)
	}

	a := base
	a.Type = TypeInternalTransfer
	a.TraceAddress = []int{0}
	b := base
	b.Type = TypeInternalTransfer
	b.TraceAddress = []int{1}
	nested := base
	nested.Type = TypeInternalTransfer
	nested.TraceAddress = []int{0, 2, 1}

	if a.DedupKey() != "1|0xtx|0" {
		t.Errorf("internal key [0] = %q, want 1|0xtx|0", a.DedupKey())
	}
	if b.DedupKey() != "1|0xtx|1" {
		t.Errorf("internal key [1] = %q, want 1|0xtx|1", b.DedupKey())
	}
	if a.DedupKey() == b.DedupKey() {
		t.Error("sibling internal transfers must have distinct dedup keys")
	}
	if nested.DedupKey() != "1|0xtx|0-2-1" {
		t.Errorf("nested key = %q, want 1|0xtx|0-2-1", nested.DedupKey())
	}
	// A top-level native_transfer and a root-level internal transfer never collide.
	if native.DedupKey() == a.DedupKey() {
		t.Error("native_transfer and internal_transfer keys collided")
	}
}

// TestInternalTransferPartitionCoLocates verifies all of a tx's transfers
// (top-level + internal) share one partition identity so their order is kept.
func TestInternalTransferPartitionCoLocates(t *testing.T) {
	native := Envelope{Type: TypeNativeTransfer, ChainID: 1, TxHash: "0xtx"}
	internal := Envelope{Type: TypeInternalTransfer, ChainID: 1, TxHash: "0xtx", TraceAddress: []int{3, 0}}
	if native.PartitionIdentity() != internal.PartitionIdentity() {
		t.Errorf("native (%q) and internal (%q) must co-partition by tx",
			native.PartitionIdentity(), internal.PartitionIdentity())
	}
	if internal.PartitionIdentity() != "1|0xtx" {
		t.Errorf("internal partition = %q, want 1|0xtx (no trace_address)", internal.PartitionIdentity())
	}
}

func TestTraceAddrStr(t *testing.T) {
	cases := []struct {
		in   []int
		want string
	}{
		{nil, "_"},
		{[]int{}, "_"},
		{[]int{0}, "0"},
		{[]int{0, 2, 1}, "0-2-1"},
	}
	for _, c := range cases {
		if got := traceAddrStr(c.in); got != c.want {
			t.Errorf("traceAddrStr(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestInternalTransferJSON verifies the on-the-wire shape: type, trace_address,
// and call_type are present for an internal transfer, and trace_address is
// omitted for record classes that don't set it.
func TestInternalTransferJSON(t *testing.T) {
	var buf strings.Builder
	w := NewWriter(&buf)
	li := uint64(0)
	// An event has no trace_address.
	if err := w.Emit(Envelope{
		Type: TypeEvent, Tool: ToolStream, Name: "usdc", Chain: "c", ChainID: 1,
		BlockNumber: 1, TxHash: "0x1", LogIndex: &li,
		Data: EventData{Event: "Transfer", Signature: "x", Contract: "0xc", Params: map[string]string{}},
	}); err != nil {
		t.Fatal(err)
	}
	// An internal transfer carries trace_address + call_type.
	if err := w.Emit(Envelope{
		Type: TypeInternalTransfer, Tool: ToolStream, Name: "native", Chain: "c", ChainID: 1,
		BlockNumber: 2, TxHash: "0x2", TraceAddress: []int{0, 1},
		Data: InternalTransferData{From: "0xa", To: "0xb", ValueWei: "1", Value: "0.0", CallType: "call"},
	}); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	if strings.Contains(lines[0], "trace_address") {
		t.Errorf("event record should not carry trace_address: %s", lines[0])
	}
	for _, want := range []string{`"type":"internal_transfer"`, `"trace_address":[0,1]`, `"call_type":"call"`} {
		if !strings.Contains(lines[1], want) {
			t.Errorf("internal_transfer JSON missing %s:\n%s", want, lines[1])
		}
	}
}
