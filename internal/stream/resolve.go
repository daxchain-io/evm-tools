package stream

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/daxchain-io/evm-tools/internal/config"
)

// ResolvedContract is a configured contract with its events resolved to ABIs.
// It is the unit the poll loop matches logs against.
type ResolvedContract struct {
	Name    string
	Address string // lowercased 0x-hex for case-insensitive matching
	// byTopic0 maps a log's topics[0] to the event that produced it, so the
	// loop matches by topic0 (see docs/design.md, "Event identification").
	byTopic0 map[string]eventABI
	// events is the resolved set in config order (for metrics/iteration).
	events []eventABI
}

// ResolveContracts resolves every configured contract's events to ABIs. Name
// resolution order, per docs/design.md: (1) a per-contract abi/abi_file when
// provided, plus explicit signatures; otherwise (2) the built-in standard
// interface ABIs. A configured event name that resolves to no signature, or to
// an overloaded name with no disambiguating ABI, is a fatal error here — at
// startup — rather than a silent miss.
func ResolveContracts(contracts []config.StreamContract) ([]ResolvedContract, error) {
	out := make([]ResolvedContract, 0, len(contracts))
	for _, c := range contracts {
		rc, err := resolveContract(c)
		if err != nil {
			return nil, fmt.Errorf("contract %q: %w", c.Name, err)
		}
		out = append(out, rc)
	}
	return out, nil
}

func resolveContract(c config.StreamContract) (ResolvedContract, error) {
	if c.Name == "" {
		return ResolvedContract{}, fmt.Errorf("missing name")
	}
	if c.Address == "" {
		return ResolvedContract{}, fmt.Errorf("missing address")
	}
	if c.ABI != "" && c.ABIFile != "" {
		return ResolvedContract{}, fmt.Errorf("set only one of abi or abi_file")
	}

	// Build the candidate event universe: explicit ABI (if any) else built-ins,
	// then explicit signatures layered on top (they always win).
	var universe []eventABI
	switch {
	case c.ABI != "":
		evs, err := parseABI([]byte(c.ABI))
		if err != nil {
			return ResolvedContract{}, fmt.Errorf("inline abi: %w", err)
		}
		universe = evs
	case c.ABIFile != "":
		raw, err := os.ReadFile(c.ABIFile)
		if err != nil {
			return ResolvedContract{}, fmt.Errorf("read abi_file %q: %w", c.ABIFile, err)
		}
		evs, err := parseABI(raw)
		if err != nil {
			return ResolvedContract{}, fmt.Errorf("abi_file %q: %w", c.ABIFile, err)
		}
		universe = evs
	default:
		universe = builtinEvents()
	}

	// Index by name; collect overloads so an ambiguous bare name is caught.
	byName := map[string][]eventABI{}
	for _, e := range universe {
		byName[e.Name] = append(byName[e.Name], e)
	}

	// Explicit signature overrides: each adds/replaces a name unambiguously.
	overrides := map[string]eventABI{}
	for name, sig := range c.Signatures {
		ev, err := parseSignature(name, sig)
		if err != nil {
			return ResolvedContract{}, err
		}
		overrides[name] = ev
	}

	if len(c.Events) == 0 {
		return ResolvedContract{}, fmt.Errorf("no events configured")
	}

	rc := ResolvedContract{
		Name:     c.Name,
		Address:  strings.ToLower(c.Address),
		byTopic0: map[string]eventABI{},
	}
	for _, name := range c.Events {
		ev, err := resolveEventName(name, byName, overrides)
		if err != nil {
			return ResolvedContract{}, err
		}
		// A non-anonymous event matches on topic0; anonymous events have no
		// topic0 and need explicit handling, which is out of M1 scope.
		if ev.Anonymous {
			return ResolvedContract{}, fmt.Errorf("event %q is anonymous; supply an explicit signature/topic0 (out of M1 scope)", name)
		}
		rc.byTopic0[ev.Topic0] = ev
		rc.events = append(rc.events, ev)
	}
	return rc, nil
}

// resolveEventName picks the eventABI for a configured name. An explicit
// signature override wins; otherwise the name must resolve to exactly one
// distinct signature in the candidate universe. Matches that share the same
// canonical signature (e.g. the ERC-20 and ERC-721 Transfer, which differ only
// in indexed-ness) collapse to a single resolution — they have the same topic0
// and are not a true overload. Zero matches, or two genuinely different
// signatures, is fatal.
func resolveEventName(name string, byName map[string][]eventABI, overrides map[string]eventABI) (eventABI, error) {
	if ev, ok := overrides[name]; ok {
		return ev, nil
	}
	matches := byName[name]
	if len(matches) == 0 {
		return eventABI{}, fmt.Errorf("event %q resolves to no known signature (provide abi/abi_file or a signatures override)", name)
	}

	// Collapse matches by canonical signature.
	bySig := map[string]eventABI{}
	for _, m := range matches {
		bySig[m.Signature] = m
	}
	if len(bySig) == 1 {
		return matches[0], nil
	}

	// Genuinely overloaded: distinct signatures with no disambiguating override.
	sigs := make([]string, 0, len(bySig))
	for sig := range bySig {
		sigs = append(sigs, sig)
	}
	sort.Strings(sigs)
	return eventABI{}, fmt.Errorf("event %q is overloaded (%s); add a signatures override to disambiguate", name, strings.Join(sigs, ", "))
}
