package filesink

import (
	"strings"

	"github.com/daxchain-io/evm-tools/internal/record"
)

// Filter decides whether a record is written. The zero Filter writes everything
// (the file sink is a recorder by default). Type/name allow- and deny-lists
// narrow that set; every configured dimension must pass for a record to be
// written. Unlike the webhook sink, the file sink has no field condition — keep
// the recorded stream complete and filter downstream, or use evm-sink-webhook.
type Filter struct {
	includeTypes map[string]bool
	excludeTypes map[string]bool
	includeNames map[string]bool
	excludeNames map[string]bool
}

// FilterOptions configures a Filter. Empty lists disable that dimension.
type FilterOptions struct {
	IncludeTypes []string
	ExcludeTypes []string
	IncludeNames []string
	ExcludeNames []string
}

// NewFilter builds a Filter from the options. There is nothing to validate (no
// operators), so it never errors.
func NewFilter(opts FilterOptions) *Filter {
	return &Filter{
		includeTypes: toSet(opts.IncludeTypes),
		excludeTypes: toSet(opts.ExcludeTypes),
		includeNames: toSet(opts.IncludeNames),
		excludeNames: toSet(opts.ExcludeNames),
	}
}

// Allow reports whether a record passes every configured filter: its type is
// allowed (allowlist match if any, no denylist match) and its name is allowed
// (same).
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
	return true
}

// toSet builds a lookup set from a slice, returning nil for an empty input so the
// "no list configured" case is a cheap nil check.
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
