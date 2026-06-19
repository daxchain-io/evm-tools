package stream

import (
	"testing"

	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// erc20Transfer returns the resolved ERC-20 Transfer event.
func erc20Transfer(t *testing.T) eventABI {
	t.Helper()
	for _, e := range builtinEvents() {
		if e.Name == "Transfer" && e.Signature == "Transfer(address,address,uint256)" {
			return e
		}
	}
	t.Fatal("Transfer event not found")
	return eventABI{}
}

func TestDecodeERC20Transfer(t *testing.T) {
	ev := erc20Transfer(t)
	// from = 0x1111..., to = 0x2222..., value = 1250000 (0x1312d0)
	from := "0x000000000000000000000000" + "1111111111111111111111111111111111111111"
	to := "0x000000000000000000000000" + "2222222222222222222222222222222222222222"
	data := "0x" + "00000000000000000000000000000000000000000000000000000000001312d0"
	l := rpc.Log{
		Topics: []string{transferTopic0, from, to},
		Data:   data,
	}
	params, err := decodeLog(ev, l)
	if err != nil {
		t.Fatalf("decodeLog: %v", err)
	}
	if params["from"] != "0x1111111111111111111111111111111111111111" {
		t.Errorf("from = %q", params["from"])
	}
	if params["to"] != "0x2222222222222222222222222222222222222222" {
		t.Errorf("to = %q", params["to"])
	}
	if params["value"] != "1250000" {
		t.Errorf("value = %q, want 1250000", params["value"])
	}
}

func TestDecodeBoolAndInt(t *testing.T) {
	// ApprovalForAll(owner indexed, operator indexed, approved bool) — bool in data.
	var ev eventABI
	for _, e := range builtinEvents() {
		if e.Name == "ApprovalForAll" {
			ev = e
			break
		}
	}
	if ev.Name == "" {
		t.Fatal("ApprovalForAll not found")
	}
	owner := "0x000000000000000000000000" + "1111111111111111111111111111111111111111"
	operator := "0x000000000000000000000000" + "2222222222222222222222222222222222222222"
	dataTrue := "0x" + "0000000000000000000000000000000000000000000000000000000000000001"
	params, err := decodeLog(ev, rpc.Log{Topics: []string{ev.Topic0, owner, operator}, Data: dataTrue})
	if err != nil {
		t.Fatalf("decodeLog: %v", err)
	}
	if params["approved"] != "true" {
		t.Errorf("approved = %q, want true", params["approved"])
	}
}

func TestDecodeWordSignedInt(t *testing.T) {
	// -1 as int256 is all 0xff.
	word := make([]byte, 32)
	for i := range word {
		word[i] = 0xff
	}
	got, err := decodeWord("int256", word)
	if err != nil {
		t.Fatal(err)
	}
	if got != "-1" {
		t.Errorf("int256(-1) decoded as %q", got)
	}
}

func TestDecodeDynamicString(t *testing.T) {
	// A single non-indexed string param "hi".
	inputs := []abiInput{{Name: "s", Type: "string"}}
	// head: offset 0x20; then len=2; then "hi" padded.
	data := concatWords(
		"0000000000000000000000000000000000000000000000000000000000000020",
		"0000000000000000000000000000000000000000000000000000000000000002",
		"6869000000000000000000000000000000000000000000000000000000000000",
	)
	vals, err := decodeData(inputs, data)
	if err != nil {
		t.Fatalf("decodeData: %v", err)
	}
	if vals[0] != "hi" {
		t.Errorf("string decoded as %q, want hi", vals[0])
	}
}

func TestDecodeUintArray(t *testing.T) {
	inputs := []abiInput{{Name: "ids", Type: "uint256[]"}}
	data := concatWords(
		"0000000000000000000000000000000000000000000000000000000000000020", // offset
		"0000000000000000000000000000000000000000000000000000000000000002", // len 2
		"0000000000000000000000000000000000000000000000000000000000000007", // 7
		"000000000000000000000000000000000000000000000000000000000000000a", // 10
	)
	vals, err := decodeData(inputs, data)
	if err != nil {
		t.Fatalf("decodeData: %v", err)
	}
	if vals[0] != "[7,10]" {
		t.Errorf("array decoded as %q, want [7,10]", vals[0])
	}
}

// TestDecodeIndexedDynamicHashed verifies an indexed dynamic param surfaces as
// its 32-byte topic hash rather than a panic.
func TestDecodeIndexedDynamicHashed(t *testing.T) {
	ev := eventABI{
		Name:      "E",
		Signature: "E(string)",
		Inputs:    []abiInput{{Name: "s", Type: "string", Indexed: true}},
	}
	hash := "0x" + "abc0000000000000000000000000000000000000000000000000000000000000"
	params, err := decodeLog(ev, rpc.Log{Topics: []string{"0xsig", hash}})
	if err != nil {
		t.Fatalf("decodeLog: %v", err)
	}
	if params["s"] != hash {
		t.Errorf("indexed dynamic = %q, want hash %q", params["s"], hash)
	}
}

func concatWords(words ...string) []byte {
	var out []byte
	for _, w := range words {
		b, err := hexToBytes("0x" + w)
		if err != nil {
			panic(err)
		}
		out = append(out, b...)
	}
	return out
}
