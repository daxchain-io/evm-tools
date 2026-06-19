package stream

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"golang.org/x/crypto/sha3"
)

// abiEntry is the subset of a JSON ABI element the stream needs. Only "event"
// entries are used for log decoding; other types are ignored.
type abiEntry struct {
	Type      string     `json:"type"`
	Name      string     `json:"name"`
	Anonymous bool       `json:"anonymous"`
	Inputs    []abiInput `json:"inputs"`
}

// abiInput is one event parameter.
type abiInput struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Indexed bool   `json:"indexed"`
	// Components carries the nested fields of a tuple type.
	Components []abiInput `json:"components"`
}

// eventABI is a resolved event definition: its name, canonical signature,
// topic0, and ordered inputs (used to decode topics/data into named params).
type eventABI struct {
	Name      string
	Signature string
	Topic0    string // 0x-prefixed keccak-256 of the signature
	Inputs    []abiInput
	Anonymous bool
}

// parseABI decodes a JSON ABI document into the event definitions it declares.
// Non-event entries are skipped. The result preserves declaration order.
func parseABI(raw []byte) ([]eventABI, error) {
	var entries []abiEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("parse abi: %w", err)
	}
	var events []eventABI
	for _, e := range entries {
		if e.Type != "event" {
			continue
		}
		sig := canonicalSignature(e.Name, e.Inputs)
		events = append(events, eventABI{
			Name:      e.Name,
			Signature: sig,
			Topic0:    Topic0(sig),
			Inputs:    e.Inputs,
			Anonymous: e.Anonymous,
		})
	}
	return events, nil
}

// canonicalSignature renders an event's canonical signature, e.g.
// "Transfer(address,address,uint256)". Tuple types expand to their component
// type list, matching Solidity's keccak input.
func canonicalSignature(name string, inputs []abiInput) string {
	parts := make([]string, len(inputs))
	for i, in := range inputs {
		parts[i] = canonicalType(in)
	}
	return name + "(" + strings.Join(parts, ",") + ")"
}

// canonicalType renders one input's canonical type, expanding tuples (including
// arrays of tuples) into their component lists.
func canonicalType(in abiInput) string {
	if strings.HasPrefix(in.Type, "tuple") {
		comps := make([]string, len(in.Components))
		for i, c := range in.Components {
			comps[i] = canonicalType(c)
		}
		// Preserve any array suffix on the tuple, e.g. "tuple[]" -> "(...)[]".
		suffix := strings.TrimPrefix(in.Type, "tuple")
		return "(" + strings.Join(comps, ",") + ")" + suffix
	}
	return in.Type
}

// Topic0 computes the 0x-prefixed keccak-256 of an event signature — the value
// matched against a log's topics[0].
func Topic0(signature string) string {
	h := sha3.NewLegacyKeccak256()
	h.Write([]byte(signature))
	return "0x" + hex.EncodeToString(h.Sum(nil))
}

// parseSignature parses an explicit canonical signature string (the override
// form, e.g. "Settled(address,uint256,bytes32)") into an eventABI with
// positional, unnamed inputs. Since the names are unknown, params decoded from
// such a signature are keyed positionally (see decode).
func parseSignature(name, signature string) (eventABI, error) {
	open := strings.IndexByte(signature, '(')
	if open < 0 || !strings.HasSuffix(signature, ")") {
		return eventABI{}, fmt.Errorf("invalid event signature %q (want Name(type,...))", signature)
	}
	inner := signature[open+1 : len(signature)-1]
	var inputs []abiInput
	if strings.TrimSpace(inner) != "" {
		types, err := splitTopLevel(inner)
		if err != nil {
			return eventABI{}, fmt.Errorf("event signature %q: %w", signature, err)
		}
		for _, t := range types {
			inputs = append(inputs, abiInput{Type: strings.TrimSpace(t)})
		}
	}
	// Re-render canonically so topic0 is computed from a normalized form.
	canon := canonicalSignature(signature[:open], inputs)
	return eventABI{
		Name:      name,
		Signature: canon,
		Topic0:    Topic0(canon),
		Inputs:    inputs,
	}, nil
}

// splitTopLevel splits a comma-separated type list, respecting parentheses so
// tuple components are not split.
func splitTopLevel(s string) ([]string, error) {
	var parts []string
	depth := 0
	start := 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("unbalanced parentheses")
			}
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	if depth != 0 {
		return nil, fmt.Errorf("unbalanced parentheses")
	}
	parts = append(parts, s[start:])
	return parts, nil
}
