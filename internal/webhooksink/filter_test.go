package webhooksink

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/record"
)

// fakePoster captures forwarded payloads without any network.
type fakePoster struct {
	got [][]byte
}

func (f *fakePoster) Post(_ context.Context, payload []byte) error {
	f.got = append(f.got, append([]byte(nil), payload...))
	return nil
}
func (f *fakePoster) Close() error { return nil }

func runWithFilter(t *testing.T, in string, opts FilterOptions) (*fakePoster, *countingMetrics) {
	t.Helper()
	fp := &fakePoster{}
	mm := &countingMetrics{}
	sink, err := New(Options{
		Reader:      record.NewReader(strings.NewReader(in)),
		Poster:      fp,
		Filter:      newFilterOrFatal(t, opts),
		Metrics:     mm,
		BackoffBase: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := sink.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return fp, mm
}

// TestFilterDefaultForwardsAll verifies an empty filter forwards every record.
func TestFilterDefaultForwardsAll(t *testing.T) {
	in := streamFrom(t, eventEnv("0x1", 0), nativeSampleEnv("treasury", 100, "1"))
	fp, mm := runWithFilter(t, in, FilterOptions{})
	if len(fp.got) != 2 {
		t.Fatalf("expected 2 forwarded, got %d", len(fp.got))
	}
	if mm.filtered != 0 {
		t.Errorf("expected 0 filtered, got %d", mm.filtered)
	}
}

// TestFilterIncludeType verifies an include-types allowlist forwards only the
// listed types and drops the rest (counted, not an error).
func TestFilterIncludeType(t *testing.T) {
	in := streamFrom(t, eventEnv("0x1", 0), nativeSampleEnv("treasury", 100, "1"))
	fp, mm := runWithFilter(t, in, FilterOptions{IncludeTypes: []string{"event"}})
	if len(fp.got) != 1 {
		t.Fatalf("expected 1 forwarded (event), got %d", len(fp.got))
	}
	if !strings.Contains(string(fp.got[0]), `"type":"event"`) {
		t.Errorf("forwarded record is not an event: %s", fp.got[0])
	}
	if mm.filtered != 1 {
		t.Errorf("expected 1 filtered, got %d", mm.filtered)
	}
}

// TestFilterExcludeType verifies an exclude-types denylist drops the listed types.
func TestFilterExcludeType(t *testing.T) {
	in := streamFrom(t, eventEnv("0x1", 0), nativeSampleEnv("treasury", 100, "1"))
	fp, _ := runWithFilter(t, in, FilterOptions{ExcludeTypes: []string{"balance_sample"}})
	if len(fp.got) != 1 {
		t.Fatalf("expected 1 forwarded after excluding balance_sample, got %d", len(fp.got))
	}
	if !strings.Contains(string(fp.got[0]), `"type":"event"`) {
		t.Errorf("forwarded record should be the event: %s", fp.got[0])
	}
}

// TestFilterIncludeName verifies a name allowlist forwards only matching names.
func TestFilterIncludeName(t *testing.T) {
	in := streamFrom(t,
		nativeSampleEnv("treasury", 100, "1"),
		nativeSampleEnv("hot-wallet", 101, "2"),
	)
	fp, _ := runWithFilter(t, in, FilterOptions{IncludeNames: []string{"treasury"}})
	if len(fp.got) != 1 {
		t.Fatalf("expected 1 forwarded (treasury), got %d", len(fp.got))
	}
	if !strings.Contains(string(fp.got[0]), `"name":"treasury"`) {
		t.Errorf("forwarded record should be treasury: %s", fp.got[0])
	}
}

// TestFilterExcludeName verifies a name denylist drops matching names.
func TestFilterExcludeName(t *testing.T) {
	in := streamFrom(t,
		nativeSampleEnv("treasury", 100, "1"),
		nativeSampleEnv("hot-wallet", 101, "2"),
	)
	fp, _ := runWithFilter(t, in, FilterOptions{ExcludeNames: []string{"hot-wallet"}})
	if len(fp.got) != 1 {
		t.Fatalf("expected 1 forwarded after excluding hot-wallet, got %d", len(fp.got))
	}
	if !strings.Contains(string(fp.got[0]), `"name":"treasury"`) {
		t.Errorf("forwarded record should be treasury: %s", fp.got[0])
	}
}

// TestFilterFieldConditionGt verifies the numeric gt field condition forwards
// only records whose named field exceeds the comparand. The contract encodes the
// balance as a string, and the comparison must be numeric and range-safe.
func TestFilterFieldConditionGt(t *testing.T) {
	in := streamFrom(t,
		nativeSampleEnv("a", 100, "5"),
		nativeSampleEnv("b", 101, "15"),
		nativeSampleEnv("c", 102, "100000000000000000000"), // > 2^53, must still compare
	)
	fp, mm := runWithFilter(t, in, FilterOptions{
		Field: &FieldCondition{Field: "balance", Op: OpGt, Value: "10"},
	})
	if len(fp.got) != 2 {
		t.Fatalf("expected 2 forwarded (balance > 10), got %d", len(fp.got))
	}
	if mm.filtered != 1 {
		t.Errorf("expected 1 filtered (balance 5), got %d", mm.filtered)
	}
}

// TestFilterFieldConditionLt verifies the numeric lt field condition.
func TestFilterFieldConditionLt(t *testing.T) {
	in := streamFrom(t,
		nativeSampleEnv("a", 100, "5"),
		nativeSampleEnv("b", 101, "15"),
	)
	fp, _ := runWithFilter(t, in, FilterOptions{
		Field: &FieldCondition{Field: "balance", Op: OpLt, Value: "10"},
	})
	if len(fp.got) != 1 {
		t.Fatalf("expected 1 forwarded (balance < 10), got %d", len(fp.got))
	}
	if !strings.Contains(string(fp.got[0]), `"balance":"5"`) {
		t.Errorf("forwarded record should have balance 5: %s", fp.got[0])
	}
}

// TestFilterFieldConditionEq verifies eq compares numerically when both operands
// are numbers (so "1.0" eq "1" holds) and falls back to a string compare for
// non-numeric operands.
func TestFilterFieldConditionEq(t *testing.T) {
	in := streamFrom(t,
		nativeSampleEnv("a", 100, "1.0"),
		nativeSampleEnv("b", 101, "2"),
	)
	fp, _ := runWithFilter(t, in, FilterOptions{
		Field: &FieldCondition{Field: "balance", Op: OpEq, Value: "1"},
	})
	if len(fp.got) != 1 {
		t.Fatalf("expected 1 forwarded (balance == 1), got %d", len(fp.got))
	}
	if !strings.Contains(string(fp.got[0]), `"balance":"1.0"`) {
		t.Errorf("forwarded record should have balance 1.0: %s", fp.got[0])
	}
}

// TestFilterFieldConditionMissingField verifies a record lacking the named field
// does not match a field condition (a positive filter: no field, no match).
func TestFilterFieldConditionMissingField(t *testing.T) {
	// An event record has no "balance" field.
	in := streamFrom(t, eventEnv("0x1", 0))
	fp, _ := runWithFilter(t, in, FilterOptions{
		Field: &FieldCondition{Field: "balance", Op: OpGt, Value: "0"},
	})
	if len(fp.got) != 0 {
		t.Fatalf("expected 0 forwarded (no balance field), got %d", len(fp.got))
	}
}

// TestFilterCombined verifies multiple filters all apply (AND semantics): a
// record must pass every configured filter to be forwarded.
func TestFilterCombined(t *testing.T) {
	in := streamFrom(t,
		nativeSampleEnv("treasury", 100, "5"),  // right name, fails gt
		nativeSampleEnv("treasury", 101, "50"), // right name, passes gt
		nativeSampleEnv("other", 102, "50"),    // wrong name
	)
	fp, _ := runWithFilter(t, in, FilterOptions{
		IncludeNames: []string{"treasury"},
		Field:        &FieldCondition{Field: "balance", Op: OpGt, Value: "10"},
	})
	if len(fp.got) != 1 {
		t.Fatalf("expected 1 forwarded (treasury & balance>10), got %d", len(fp.got))
	}
	if !strings.Contains(string(fp.got[0]), `"balance":"50"`) || !strings.Contains(string(fp.got[0]), `"name":"treasury"`) {
		t.Errorf("forwarded record should be treasury balance 50: %s", fp.got[0])
	}
}

// TestNewFilterRejectsBadCondition verifies an unsupported op or empty field name
// fails fast.
func TestNewFilterRejectsBadCondition(t *testing.T) {
	if _, err := NewFilter(FilterOptions{Field: &FieldCondition{Field: "balance", Op: "ne", Value: "1"}}); err == nil {
		t.Error("expected an error for an unsupported op")
	}
	if _, err := NewFilter(FilterOptions{Field: &FieldCondition{Field: "", Op: OpEq, Value: "1"}}); err == nil {
		t.Error("expected an error for an empty field name")
	}
}
