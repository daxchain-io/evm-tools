package stream

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// decodeLog decodes a matched log into the param map for an EventData record.
// Indexed parameters are read from topics[1:] in order; non-indexed parameters
// are ABI-decoded from data. Every value is rendered as a string per the
// contract's numeric-encoding rule.
//
// For a value type indexed in a topic the slot holds the value directly. For a
// reference/dynamic type indexed in a topic, EVM stores only its keccak hash, so
// the param is rendered as that 0x-hash (the value itself is not recoverable).
func decodeLog(ev eventABI, l rpc.Log) (map[string]string, error) {
	params := make(map[string]string, len(ev.Inputs))

	dataBytes, err := hexToBytes(l.Data)
	if err != nil {
		return nil, fmt.Errorf("decode data: %w", err)
	}

	// topics[0] is the event signature; indexed values start at topics[1].
	topicIdx := 1
	var nonIndexed []abiInput
	var nonIndexedNames []string

	for i, in := range ev.Inputs {
		name := paramName(in.Name, i)
		if in.Indexed {
			if topicIdx >= len(l.Topics) {
				return nil, fmt.Errorf("indexed param %q has no topic (have %d topics)", name, len(l.Topics))
			}
			topic := l.Topics[topicIdx]
			topicIdx++
			val, err := decodeTopic(in.Type, topic)
			if err != nil {
				return nil, fmt.Errorf("indexed param %q: %w", name, err)
			}
			params[name] = val
		} else {
			nonIndexed = append(nonIndexed, in)
			nonIndexedNames = append(nonIndexedNames, name)
		}
	}

	if len(nonIndexed) > 0 {
		vals, err := decodeData(nonIndexed, dataBytes)
		if err != nil {
			return nil, fmt.Errorf("decode non-indexed data: %w", err)
		}
		for i, v := range vals {
			params[nonIndexedNames[i]] = v
		}
	}

	return params, nil
}

// paramName returns the ABI parameter name, or a positional "arg<i>" when the
// ABI/signature omitted it (the override-signature case).
func paramName(name string, i int) string {
	if name == "" {
		return "arg" + strconv.Itoa(i)
	}
	return name
}

// decodeTopic renders a single indexed value held in a 32-byte topic word.
func decodeTopic(typ, topic string) (string, error) {
	word, err := hexToBytes(topic)
	if err != nil {
		return "", err
	}
	if len(word) != 32 {
		return "", fmt.Errorf("topic is not 32 bytes (got %d)", len(word))
	}
	// Reference/dynamic types are stored as their keccak hash in a topic; the
	// raw value is not recoverable, so surface the hash.
	if isDynamicType(typ) {
		return "0x" + hex.EncodeToString(word), nil
	}
	return decodeWord(typ, word)
}

// decodeData ABI-decodes the non-indexed parameter tuple from the log data.
func decodeData(inputs []abiInput, data []byte) ([]string, error) {
	out := make([]string, len(inputs))
	for i, in := range inputs {
		head := i * 32
		if head+32 > len(data) {
			return nil, fmt.Errorf("data too short for param %d (%s)", i, in.Type)
		}
		word := data[head : head+32]
		if isDynamicType(in.Type) {
			// Head holds the offset to the dynamic payload, relative to the
			// start of the tuple's data region.
			off := new(big.Int).SetBytes(word)
			if !off.IsUint64() {
				return nil, fmt.Errorf("param %d (%s): offset overflows", i, in.Type)
			}
			v, err := decodeDynamic(in.Type, data, int(off.Uint64()))
			if err != nil {
				return nil, fmt.Errorf("param %d (%s): %w", i, in.Type, err)
			}
			out[i] = v
			continue
		}
		v, err := decodeWord(in.Type, word)
		if err != nil {
			return nil, fmt.Errorf("param %d (%s): %w", i, in.Type, err)
		}
		out[i] = v
	}
	return out, nil
}

// decodeDynamic decodes a dynamic value (bytes, string, or T[] for a static T)
// whose payload begins at offset within data.
func decodeDynamic(typ string, data []byte, offset int) (string, error) {
	if offset+32 > len(data) {
		return "", fmt.Errorf("offset %d past data end", offset)
	}
	lenWord := data[offset : offset+32]
	n := new(big.Int).SetBytes(lenWord)
	if !n.IsUint64() {
		return "", fmt.Errorf("length overflows")
	}
	count := int(n.Uint64())

	switch {
	case typ == "bytes":
		end := offset + 32 + count
		if end > len(data) {
			return "", fmt.Errorf("bytes payload past data end")
		}
		return "0x" + hex.EncodeToString(data[offset+32:end]), nil
	case typ == "string":
		end := offset + 32 + count
		if end > len(data) {
			return "", fmt.Errorf("string payload past data end")
		}
		return string(data[offset+32 : end]), nil
	case strings.HasSuffix(typ, "[]"):
		elemType := strings.TrimSuffix(typ, "[]")
		if isDynamicType(elemType) {
			return "", fmt.Errorf("nested dynamic array element %q unsupported", elemType)
		}
		elems := make([]string, count)
		base := offset + 32
		for i := 0; i < count; i++ {
			ws := base + i*32
			if ws+32 > len(data) {
				return "", fmt.Errorf("array element %d past data end", i)
			}
			v, err := decodeWord(elemType, data[ws:ws+32])
			if err != nil {
				return "", err
			}
			elems[i] = v
		}
		return "[" + strings.Join(elems, ",") + "]", nil
	default:
		return "", fmt.Errorf("unsupported dynamic type %q", typ)
	}
}

// decodeWord decodes a single 32-byte static value into its string form.
func decodeWord(typ string, word []byte) (string, error) {
	if len(word) != 32 {
		return "", fmt.Errorf("word is not 32 bytes")
	}
	switch {
	case typ == "address":
		// Address is the low 20 bytes, lowercased 0x-hex.
		return "0x" + hex.EncodeToString(word[12:]), nil
	case typ == "bool":
		if word[31] == 0 {
			return "false", nil
		}
		return "true", nil
	case strings.HasPrefix(typ, "uint"):
		return new(big.Int).SetBytes(word).String(), nil
	case strings.HasPrefix(typ, "int"):
		return decodeSignedInt(word), nil
	case strings.HasPrefix(typ, "bytes"):
		// Fixed-size bytesN: the leading N bytes, 0x-hex.
		n, err := fixedBytesSize(typ)
		if err != nil {
			return "", err
		}
		return "0x" + hex.EncodeToString(word[:n]), nil
	default:
		return "", fmt.Errorf("unsupported static type %q", typ)
	}
}

// decodeSignedInt decodes a two's-complement intN word into its decimal string.
func decodeSignedInt(word []byte) string {
	v := new(big.Int).SetBytes(word)
	// If the high bit is set, it's negative: subtract 2^256.
	if word[0]&0x80 != 0 {
		twoTo256 := new(big.Int).Lsh(big.NewInt(1), 256)
		v.Sub(v, twoTo256)
	}
	return v.String()
}

// fixedBytesSize returns N for a "bytesN" type (1..32).
func fixedBytesSize(typ string) (int, error) {
	if typ == "bytes" {
		return 0, fmt.Errorf("dynamic bytes has no fixed size")
	}
	n, err := strconv.Atoi(strings.TrimPrefix(typ, "bytes"))
	if err != nil || n < 1 || n > 32 {
		return 0, fmt.Errorf("invalid fixed bytes type %q", typ)
	}
	return n, nil
}

// isDynamicType reports whether an ABI type is dynamically sized (stored
// out-of-line in data, or hashed in a topic).
func isDynamicType(typ string) bool {
	return typ == "bytes" || typ == "string" || strings.HasSuffix(typ, "[]") || strings.HasPrefix(typ, "tuple")
}

// hexToBytes parses an optional-0x hex string into bytes. An empty string or
// bare "0x" yields an empty slice.
func hexToBytes(s string) ([]byte, error) {
	s = strings.TrimPrefix(strings.TrimPrefix(s, "0x"), "0X")
	if s == "" {
		return nil, nil
	}
	return hex.DecodeString(s)
}
