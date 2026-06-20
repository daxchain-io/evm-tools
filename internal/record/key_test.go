package record

import "testing"

// TestDedupKey covers the per-class key composition specified in the contract,
// including the reorg-stability guarantee (block_hash excluded from the event
// key) and log_index 0 preservation.
func TestDedupKey(t *testing.T) {
	tests := []struct {
		name string
		env  Envelope
		want string
	}{
		{
			name: "event",
			env: Envelope{
				Type: TypeEvent, ChainID: 4242, TxHash: "0xtx", LogIndex: ptrUint64(12),
				BlockHash: "0xshouldnotappear",
			},
			want: "4242|0xtx|12",
		},
		{
			name: "event_log_index_zero",
			env: Envelope{
				Type: TypeEvent, ChainID: 4242, TxHash: "0xtx", LogIndex: ptrUint64(0),
			},
			want: "4242|0xtx|0",
		},
		{
			name: "native_transfer",
			env:  Envelope{Type: TypeNativeTransfer, ChainID: 4242, TxHash: "0xtx"},
			want: "4242|0xtx",
		},
		{
			name: "balance_sample_includes_emitted_at",
			env: Envelope{
				Type: TypeBalanceSample, ChainID: 4242, Name: "treasury",
				BlockNumber: 100, EmittedAt: "2026-06-19T12:00:03Z",
			},
			want: "4242|balance_sample|treasury|100|2026-06-19T12:00:03Z",
		},
		{
			name: "balance_change_no_emitted_at",
			env: Envelope{
				Type: TypeBalanceChange, ChainID: 4242, Name: "treasury",
				BlockNumber: 100, EmittedAt: "2026-06-19T12:00:03Z",
			},
			want: "4242|balance_change|treasury|100",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.env.DedupKey(); got != tt.want {
				t.Errorf("DedupKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestDedupKeyReorgStable verifies the same event re-observed after a reorg
// (different block_hash and block_number, same tx_hash/log_index) yields the
// same dedup key.
func TestDedupKeyReorgStable(t *testing.T) {
	a := Envelope{Type: TypeEvent, ChainID: 1, TxHash: "0xabc", LogIndex: ptrUint64(3), BlockHash: "0xaaa", BlockNumber: 10}
	b := Envelope{Type: TypeEvent, ChainID: 1, TxHash: "0xabc", LogIndex: ptrUint64(3), BlockHash: "0xbbb", BlockNumber: 11}
	if a.DedupKey() != b.DedupKey() {
		t.Errorf("event dedup key not reorg-stable: %q != %q", a.DedupKey(), b.DedupKey())
	}
}

// TestPartitionIdentityOrdering verifies that two samples of the same configured
// entry at different blocks (and emitted_at) share a partition identity so
// per-key ordering holds, even though their full dedup keys differ.
func TestPartitionIdentityOrdering(t *testing.T) {
	a := Envelope{Type: TypeBalanceSample, ChainID: 1, Name: "treasury", BlockNumber: 100, EmittedAt: "2026-06-19T12:00:00Z"}
	b := Envelope{Type: TypeBalanceSample, ChainID: 1, Name: "treasury", BlockNumber: 101, EmittedAt: "2026-06-19T12:01:00Z"}
	if a.PartitionIdentity() != b.PartitionIdentity() {
		t.Errorf("same entry should share a partition identity: %q != %q", a.PartitionIdentity(), b.PartitionIdentity())
	}
	if a.DedupKey() == b.DedupKey() {
		t.Errorf("different samples should have distinct dedup keys")
	}

	// A transaction-backed record's partition identity equals its dedup key.
	ev := Envelope{Type: TypeEvent, ChainID: 1, TxHash: "0xtx", LogIndex: ptrUint64(0)}
	if ev.PartitionIdentity() != ev.DedupKey() {
		t.Errorf("event partition identity should equal its dedup key: %q != %q",
			ev.PartitionIdentity(), ev.DedupKey())
	}
}
