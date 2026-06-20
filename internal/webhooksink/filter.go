package webhooksink

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/daxchain-io/evm-tools/internal/record"
)

// FieldOp is the comparison operator for the single supported field condition.
type FieldOp string

// Supported field-condition operators. This is deliberately NOT a rule DSL — a
// webhook is a forwarder with optional filters (see docs/design.md Open
// Question 1, settled for this build), so only equality and a numeric
// greater/less-than are supported on one named data field.
const (
	OpEq FieldOp = "eq"
	OpGt FieldOp = "gt"
	OpLt FieldOp = "lt"
)

// FieldCondition is the resolved single field condition: compare the named data
// field against Value with Op. Numeric operands (the contract's string-encoded
// amounts, and JSON numbers like decimals) compare numerically for gt/lt; eq
// compares numerically when both operands parse as numbers, otherwise as
// strings.
type FieldCondition struct {
	Field string
	Op    FieldOp
	Value string
}

// Filter decides whether a record is forwarded. The zero Filter forwards
// everything (a webhook is a forwarder by default). Type/name allow/deny lists
// and the optional field condition narrow that set; every configured filter must
// pass for a record to be forwarded.
type Filter struct {
	includeTypes map[string]bool
	excludeTypes map[string]bool
	includeNames map[string]bool
	excludeNames map[string]bool
	field        *FieldCondition
}

// FilterOptions configures a Filter. Empty lists disable that dimension.
type FilterOptions struct {
	IncludeTypes []string
	ExcludeTypes []string
	IncludeNames []string
	ExcludeNames []string
	Field        *FieldCondition
}

// NewFilter validates and builds a Filter. It rejects an unsupported field
// operator and an empty field name on a configured condition so a typo fails
// fast in `validate` rather than silently forwarding everything.
func NewFilter(opts FilterOptions) (*Filter, error) {
	f := &Filter{
		includeTypes: toSet(opts.IncludeTypes),
		excludeTypes: toSet(opts.ExcludeTypes),
		includeNames: toSet(opts.IncludeNames),
		excludeNames: toSet(opts.ExcludeNames),
	}
	if opts.Field != nil {
		fc := *opts.Field
		if strings.TrimSpace(fc.Field) == "" {
			return nil, fmt.Errorf("webhooksink: filter field condition requires a field name")
		}
		switch fc.Op {
		case OpEq, OpGt, OpLt:
		default:
			return nil, fmt.Errorf("webhooksink: unsupported field condition op %q (want eq|gt|lt)", fc.Op)
		}
		f.field = &fc
	}
	return f, nil
}

// Allow reports whether a record passes every configured filter. A record is
// forwarded only when: its type is allowed (allowlist match if any, no denylist
// match), its name is allowed (same), and the field condition (if any) holds.
func (f *Filter) Allow(env record.Envelope) bool {
	t := string(env.Type)
	if len(f.includeTypes) > 0 && !f.includeTypes[t] {
		return false
	}
	if f.excludeTypes[t] {
		return false
	}
	if len(f.includeNames) > 0 && !f.includeNames[env.Name] {
		return false
	}
	if f.excludeNames[env.Name] {
		return false
	}
	if f.field != nil && !f.field.matches(env) {
		return false
	}
	return true
}

// matches evaluates the field condition against a record's data payload. The
// Reader decodes Data into a generic map (Data is `any`), so we look the field up
// there. A record whose data is not an object, or that lacks the field, does not
// match (the condition is a positive filter: no field, no match).
func (c *FieldCondition) matches(env record.Envelope) bool {
	data, ok := env.Data.(map[string]any)
	if !ok {
		return false
	}
	raw, ok := data[c.Field]
	if !ok {
		return false
	}
	got := scalarString(raw)

	gotNum, gotIsNum := parseNumber(got)
	wantNum, wantIsNum := parseNumber(c.Value)

	switch c.Op {
	case OpEq:
		if gotIsNum && wantIsNum {
			return gotNum.Cmp(wantNum) == 0
		}
		return got == c.Value
	case OpGt:
		if !gotIsNum || !wantIsNum {
			return false
		}
		return gotNum.Cmp(wantNum) > 0
	case OpLt:
		if !gotIsNum || !wantIsNum {
			return false
		}
		return gotNum.Cmp(wantNum) < 0
	default:
		return false
	}
}

// scalarString renders a decoded JSON scalar to a string for comparison. The
// contract encodes amounts as strings already; JSON numbers (e.g. decimals)
// arrive as float64 and bools as bool, so we normalize them too. Non-scalars
// (objects/arrays) yield "" and will not match a scalar comparand.
func scalarString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	case float64:
		return new(big.Float).SetFloat64(t).Text('f', -1)
	case nil:
		return ""
	default:
		return ""
	}
}

// parseNumber parses a decimal numeric string into a big.Float for exact,
// range-safe comparison (the contract's amounts can exceed 2^53). It returns
// (nil, false) for a non-numeric string.
func parseNumber(s string) (*big.Float, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	f, _, err := big.ParseFloat(s, 10, 256, big.ToNearestEven)
	if err != nil {
		return nil, false
	}
	return f, true
}

// toSet builds a lookup set from a slice, returning nil for an empty input so
// the "no allowlist configured" case is a cheap nil check.
func toSet(items []string) map[string]bool {
	if len(items) == 0 {
		return nil
	}
	m := make(map[string]bool, len(items))
	for _, it := range items {
		if s := strings.TrimSpace(it); s != "" {
			m[s] = true
		}
	}
	if len(m) == 0 {
		return nil
	}
	return m
}
