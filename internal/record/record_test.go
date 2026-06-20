package record

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "update golden files")

func ptrInt(v int) *int          { return &v }
func ptrUint64(v uint64) *uint64 { return &v }

// fixedTime is a deterministic timestamp used across the golden tests.
func fixedTime(sec int) time.Time {
	return time.Date(2026, 6, 19, 12, 0, sec, 0, time.UTC)
}

// goldenCases enumerates one representative envelope per record type plus the
// edge cases the contract calls out (omit-empty, string amounts, log_index 0,
// contract creation).
func goldenCases() []struct {
	name string
	env  Envelope
} {
	ts := RFC3339(fixedTime(0))
	emitted := RFC3339(fixedTime(3))

	return []struct {
		name string
		env  Envelope
	}{
		{
			name: "event",
			env: Envelope{
				Type:        TypeEvent,
				Tool:        ToolStream,
				Name:        "usdc",
				Chain:       "my-chain",
				ChainID:     4242,
				BlockNumber: 19000001,
				BlockHash:   "0xblockhash",
				TxHash:      "0xtxhash",
				LogIndex:    ptrUint64(12),
				Timestamp:   ts,
				EmittedAt:   emitted,
				Data: EventData{
					Event:     "Transfer",
					Signature: "Transfer(address,address,uint256)",
					Contract:  "0xcontract",
					Params: map[string]string{
						"from":  "0xfrom",
						"to":    "0xto",
						"value": "1250000",
					},
				},
			},
		},
		{
			// log_index 0 must be preserved, not dropped as empty.
			name: "event_log_index_zero",
			env: Envelope{
				Type:        TypeEvent,
				Tool:        ToolStream,
				Name:        "usdc",
				Chain:       "my-chain",
				ChainID:     4242,
				BlockNumber: 19000001,
				BlockHash:   "0xblockhash",
				TxHash:      "0xtxhash",
				LogIndex:    ptrUint64(0),
				Timestamp:   ts,
				EmittedAt:   emitted,
				Data: EventData{
					Event:     "Approval",
					Signature: "Approval(address,address,uint256)",
					Contract:  "0xcontract",
					Params:    map[string]string{"owner": "0xowner", "spender": "0xspender", "value": "0"},
				},
			},
		},
		{
			name: "native_transfer",
			env: Envelope{
				Type:        TypeNativeTransfer,
				Tool:        ToolStream,
				Name:        "native",
				Chain:       "my-chain",
				ChainID:     4242,
				BlockNumber: 19000002,
				BlockHash:   "0xblockhash",
				TxHash:      "0xtxhash",
				Timestamp:   ts,
				EmittedAt:   emitted,
				Data: NativeTransferData{
					From:     "0xfrom",
					To:       "0xto",
					ValueWei: "1250000000000000000",
					Value:    "1.25",
				},
			},
		},
		{
			// contract creation: To omitted, contract_creation true, no log_index.
			name: "native_transfer_contract_creation",
			env: Envelope{
				Type:        TypeNativeTransfer,
				Tool:        ToolStream,
				Name:        "native",
				Chain:       "my-chain",
				ChainID:     4242,
				BlockNumber: 19000003,
				BlockHash:   "0xblockhash",
				TxHash:      "0xtxhash",
				Timestamp:   ts,
				EmittedAt:   emitted,
				Data: NativeTransferData{
					From:             "0xfrom",
					ValueWei:         "5000000000000000000",
					Value:            "5",
					ContractCreation: true,
				},
			},
		},
		{
			name: "balance_sample_native",
			env: Envelope{
				Type:        TypeBalanceSample,
				Tool:        ToolBalance,
				Name:        "treasury-eth",
				Chain:       "my-chain",
				ChainID:     4242,
				BlockNumber: 19000050,
				BlockHash:   "0xblockhash",
				Timestamp:   ts,
				EmittedAt:   emitted,
				Data: BalanceData{
					Kind:       KindNative,
					Address:    "0xaddr",
					BalanceWei: "4200000000000000000",
					Balance:    "4.2",
				},
			},
		},
		{
			name: "balance_change_erc20",
			env: Envelope{
				Type:        TypeBalanceChange,
				Tool:        ToolBalance,
				Name:        "treasury-usdc",
				Chain:       "my-chain",
				ChainID:     4242,
				BlockNumber: 19000061,
				BlockHash:   "0xblockhash",
				Timestamp:   ts,
				EmittedAt:   emitted,
				Data: BalanceData{
					Kind:        KindERC20,
					Token:       "0xtoken",
					Address:     "0xaddr",
					PreviousRaw: "1000000",
					BalanceRaw:  "2000000",
					Balance:     "2.0",
					Decimals:    ptrInt(6),
				},
			},
		},
		{
			name: "balance_sample_erc721",
			env: Envelope{
				Type:        TypeBalanceSample,
				Tool:        ToolBalance,
				Name:        "vault-nft-count",
				Chain:       "my-chain",
				ChainID:     4242,
				BlockNumber: 19000065,
				BlockHash:   "0xblockhash",
				Timestamp:   ts,
				EmittedAt:   emitted,
				Data: BalanceData{
					Kind:    KindERC721,
					Token:   "0xtoken",
					Address: "0xaddr",
					Count:   "7",
				},
			},
		},
		{
			name: "ownership_change",
			env: Envelope{
				Type:        TypeOwnershipChange,
				Tool:        ToolBalance,
				Name:        "special-token-owner",
				Chain:       "my-chain",
				ChainID:     4242,
				BlockNumber: 19000070,
				BlockHash:   "0xblockhash",
				Timestamp:   ts,
				EmittedAt:   emitted,
				Data: OwnershipData{
					Kind:          KindERC721,
					Token:         "0xtoken",
					TokenID:       "1234",
					PreviousOwner: "0xold",
					Owner:         "0xnew",
				},
			},
		},
		{
			name: "ownership_sample",
			env: Envelope{
				Type:        TypeOwnershipSample,
				Tool:        ToolBalance,
				Name:        "special-token-owner",
				Chain:       "my-chain",
				ChainID:     4242,
				BlockNumber: 19000071,
				BlockHash:   "0xblockhash",
				Timestamp:   ts,
				EmittedAt:   emitted,
				Data: OwnershipData{
					Kind:    KindERC721,
					Token:   "0xtoken",
					TokenID: "1234",
					Owner:   "0xnew",
				},
			},
		},
		{
			name: "contract_sample_total_supply",
			env: Envelope{
				Type:        TypeContractSample,
				Tool:        ToolBalance,
				Name:        "usdc",
				Chain:       "my-chain",
				ChainID:     4242,
				BlockNumber: 19000080,
				BlockHash:   "0xblockhash",
				Timestamp:   ts,
				EmittedAt:   emitted,
				Data: ContractData{
					Address:        "0xcontract",
					Field:          FieldTokenTotalSupply,
					TotalSupplyRaw: "50000000000000",
					TotalSupply:    "50000000.0",
					Decimals:       ptrInt(6),
				},
			},
		},
		{
			name: "contract_change_transfer_count",
			env: Envelope{
				Type:        TypeContractChange,
				Tool:        ToolBalance,
				Name:        "usdc",
				Chain:       "my-chain",
				ChainID:     4242,
				BlockNumber: 19000090,
				BlockHash:   "0xblockhash",
				Timestamp:   ts,
				EmittedAt:   emitted,
				Data: ContractData{
					Address:       "0xcontract",
					Field:         FieldTransferCount,
					PreviousCount: "100",
					Count:         "150",
					WindowBlocks:  ptrInt(1000),
				},
			},
		},
		{
			name: "contract_sample_native_balance",
			env: Envelope{
				Type:        TypeContractSample,
				Tool:        ToolBalance,
				Name:        "vault",
				Chain:       "my-chain",
				ChainID:     4242,
				BlockNumber: 19000095,
				BlockHash:   "0xblockhash",
				Timestamp:   ts,
				EmittedAt:   emitted,
				Data: ContractData{
					Address:    "0xcontract",
					Field:      FieldNativeBalance,
					BalanceWei: "9000000000000000000",
					Balance:    "9",
				},
			},
		},
	}
}

func TestGolden(t *testing.T) {
	for _, tc := range goldenCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			if err := w.Emit(tc.env); err != nil {
				t.Fatalf("Emit: %v", err)
			}

			got := buf.Bytes()
			goldenPath := filepath.Join("testdata", tc.name+".golden.json")

			if *update {
				if err := os.WriteFile(goldenPath, got, 0o644); err != nil {
					t.Fatalf("write golden: %v", err)
				}
				return
			}

			want, err := os.ReadFile(goldenPath)
			if err != nil {
				t.Fatalf("read golden (run with -update to create): %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("output mismatch\n got: %s\nwant: %s", got, want)
			}

			// The line must be exactly one JSON object terminated by one
			// newline — the atomic-line invariant.
			if c := bytes.Count(got, []byte("\n")); c != 1 {
				t.Errorf("expected exactly one trailing newline, got %d", c)
			}
			if got[len(got)-1] != '\n' {
				t.Errorf("line must end with newline")
			}
		})
	}
}

// TestEnvelopeInvariants checks contract guarantees that are independent of any
// single golden file: schema_version is stamped, emitted_at is always present,
// and amounts are JSON strings while the enumerated counters are JSON numbers.
func TestEnvelopeInvariants(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.now = func() time.Time { return fixedTime(9) } // exercise auto-stamp

	env := Envelope{
		Type:        TypeBalanceSample,
		Tool:        ToolBalance,
		Name:        "treasury-eth",
		Chain:       "my-chain",
		ChainID:     4242,
		BlockNumber: 19000050,
		Timestamp:   RFC3339(fixedTime(0)),
		// EmittedAt deliberately left empty to exercise auto-stamp.
		Data: BalanceData{Kind: KindNative, Address: "0xa", BalanceWei: "1", Balance: "0.000000000000000001"},
	}
	if err := w.Emit(env); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if string(m["schema_version"]) != "1" {
		t.Errorf("schema_version = %s, want 1", m["schema_version"])
	}
	if string(m["emitted_at"]) != `"2026-06-19T12:00:09Z"` {
		t.Errorf("emitted_at = %s, want auto-stamped RFC3339", m["emitted_at"])
	}
	// chain_id and block_number are bare numbers (safe to use in keys).
	if string(m["chain_id"]) != "4242" {
		t.Errorf("chain_id should be a bare number, got %s", m["chain_id"])
	}
	if string(m["block_number"]) != "19000050" {
		t.Errorf("block_number should be a bare number, got %s", m["block_number"])
	}

	// Amounts inside data must be JSON strings.
	var data map[string]json.RawMessage
	if err := json.Unmarshal(m["data"], &data); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	for _, k := range []string{"balance_wei", "balance"} {
		v := string(data[k])
		if !strings.HasPrefix(v, `"`) {
			t.Errorf("data.%s must be a JSON string, got %s", k, v)
		}
	}
}

// TestOmitEmpty verifies that inapplicable fields are omitted rather than
// emitted as null/empty: a native_transfer carries no log_index and a
// contract-creation transfer omits "to".
func TestOmitEmpty(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.now = func() time.Time { return fixedTime(0) }

	if err := w.Emit(Envelope{
		Type:        TypeNativeTransfer,
		Tool:        ToolStream,
		Name:        "native",
		Chain:       "my-chain",
		ChainID:     4242,
		BlockNumber: 1,
		TxHash:      "0xtx",
		Data:        NativeTransferData{From: "0xf", ValueWei: "1", Value: "0", ContractCreation: true},
	}); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	out := buf.String()
	for _, absent := range []string{`"log_index"`, `"to"`} {
		if strings.Contains(out, absent) {
			t.Errorf("expected %s to be omitted, got: %s", absent, out)
		}
	}
	if strings.Contains(out, "null") {
		t.Errorf("no field should be emitted as null, got: %s", out)
	}
}

// TestConcurrentEmitAtomic ensures concurrent writers never interleave: every
// line in the output is a complete, parseable JSON object.
func TestConcurrentEmitAtomic(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	const goroutines = 16
	const perGoroutine = 64
	done := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < perGoroutine; i++ {
				_ = w.Emit(Envelope{
					Type:        TypeBalanceSample,
					Tool:        ToolBalance,
					Name:        "acct",
					Chain:       "my-chain",
					ChainID:     4242,
					BlockNumber: uint64(i),
					Data:        BalanceData{Kind: KindNative, Address: "0xa", BalanceWei: "1", Balance: "0"},
				})
			}
		}()
	}
	for g := 0; g < goroutines; g++ {
		<-done
	}

	lines := bytes.Split(bytes.TrimRight(buf.Bytes(), "\n"), []byte("\n"))
	if len(lines) != goroutines*perGoroutine {
		t.Fatalf("expected %d lines, got %d", goroutines*perGoroutine, len(lines))
	}
	for i, line := range lines {
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatalf("line %d is not valid JSON (interleaved write?): %v\n%s", i, err, line)
		}
	}
}
