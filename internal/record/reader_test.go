package record

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

// TestReaderRoundTrip writes every golden envelope through the Writer and reads
// it back through the Reader, asserting the decoded envelope re-encodes to the
// semantically identical JSON value. This proves the Reader is the faithful
// inverse of the Writer (the encoder stays the source of truth) for every record
// type. The comparison is value-based, not byte-based: Envelope.Data is `any`,
// so a round trip decodes the payload into a generic map whose re-encoding sorts
// keys — which the contract explicitly permits (consumers ignore field order).
func TestReaderRoundTrip(t *testing.T) {
	for _, tc := range goldenCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			if err := w.Emit(tc.env); err != nil {
				t.Fatalf("Emit: %v", err)
			}
			original := append([]byte(nil), buf.Bytes()...)

			r := NewReader(bytes.NewReader(original))
			got, err := r.Next()
			if err != nil {
				t.Fatalf("Next: %v", err)
			}

			// Raw() must equal the original line minus the trailing newline.
			wantRaw := bytes.TrimRight(original, "\n")
			if !bytes.Equal(r.Raw(), wantRaw) {
				t.Errorf("Raw mismatch\n got: %s\nwant: %s", r.Raw(), wantRaw)
			}

			// Re-encode the decoded envelope and compare to the original as JSON
			// values (key order is not part of the contract).
			var reBuf bytes.Buffer
			rw := NewWriter(&reBuf)
			if err := rw.Emit(got); err != nil {
				t.Fatalf("re-Emit: %v", err)
			}
			if !jsonEqual(t, reBuf.Bytes(), original) {
				t.Errorf("round-trip not value-identical\n got: %s\nwant: %s", reBuf.Bytes(), original)
			}

			if got.Type != tc.env.Type {
				t.Errorf("type = %q, want %q", got.Type, tc.env.Type)
			}

			// And EOF on the next read.
			if _, err := r.Next(); !errors.Is(err, io.EOF) {
				t.Errorf("expected io.EOF after one record, got %v", err)
			}
		})
	}
}

// jsonEqual reports whether two JSON documents are semantically equal,
// disregarding key ordering and insignificant whitespace.
func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		t.Fatalf("unmarshal a: %v", err)
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		t.Fatalf("unmarshal b: %v", err)
	}
	return reflect.DeepEqual(av, bv)
}

// TestReaderMultipleLines reads a stream of several records, including blank
// lines between them, and asserts every record decodes in order.
func TestReaderMultipleLines(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	cases := goldenCases()
	for _, tc := range cases {
		if err := w.Emit(tc.env); err != nil {
			t.Fatalf("Emit %s: %v", tc.name, err)
		}
	}
	// Inject blank lines: they must be skipped, not treated as records.
	stream := strings.ReplaceAll(buf.String(), "\n", "\n\n")

	r := NewReader(strings.NewReader(stream))
	var got []Type
	for {
		env, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		got = append(got, env.Type)
	}
	if len(got) != len(cases) {
		t.Fatalf("read %d records, want %d", len(got), len(cases))
	}
	for i, tc := range cases {
		if got[i] != tc.env.Type {
			t.Errorf("record %d type = %q, want %q", i, got[i], tc.env.Type)
		}
	}
}

// TestReaderMalformedLine confirms a malformed line is a hard error: a sink must
// not silently skip a record it cannot parse (the stream is the contract).
func TestReaderMalformedLine(t *testing.T) {
	r := NewReader(strings.NewReader(`{"schema_version":1,"type":"event"` + "\n"))
	_, err := r.Next()
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("expected a decode error for a truncated line, got %v", err)
	}
	if !strings.Contains(err.Error(), "decode line") {
		t.Errorf("error should mention decode, got: %v", err)
	}
}

// TestReaderRejectsTrailingData verifies a line carrying more than one JSON
// object (or trailing garbage) is a hard error rather than silently dropping the
// trailing record(s) — Decode alone stops after the first value.
func TestReaderRejectsTrailingData(t *testing.T) {
	line := `{"schema_version":1,"type":"event","name":"a"}{"schema_version":1,"type":"event","name":"b"}` + "\n"
	r := NewReader(strings.NewReader(line))
	_, err := r.Next()
	if err == nil {
		t.Fatal("expected a hard error on trailing data after the JSON object")
	}
	if !strings.Contains(err.Error(), "trailing data") {
		t.Errorf("error should mention trailing data, got: %v", err)
	}
}

// TestReaderRejectsUnsupportedSchema verifies the reader rejects a record whose
// schema_version this build does not accept — both higher and lower — with a
// matchable error, rather than best-effort parsing it.
func TestReaderRejectsUnsupportedSchema(t *testing.T) {
	for _, v := range []int{0, 2, 99} {
		line := `{"schema_version":` + itoa(v) + `,"type":"event","tool":"evm-stream","name":"x","chain":"c","chain_id":1,"block_number":1,"emitted_at":"2026-06-19T12:00:00Z","data":{}}` + "\n"
		r := NewReader(strings.NewReader(line))
		_, err := r.Next()
		if !errors.Is(err, ErrSchemaUnsupported) {
			t.Errorf("schema_version %d: expected ErrSchemaUnsupported, got %v", v, err)
		}
		var se *SchemaError
		if !errors.As(err, &se) || se.Got != v {
			t.Errorf("schema_version %d: expected *SchemaError with Got=%d, got %v", v, v, err)
		}
	}
}

// TestReaderQuarantineSkipsPoison verifies that with a quarantine hook installed
// the reader routes each poison line (invalid JSON, trailing data, unsupported
// schema_version) to the hook with the verbatim bytes + error and then continues,
// returning only the valid records — instead of failing fast.
func TestReaderQuarantineSkipsPoison(t *testing.T) {
	good1 := `{"schema_version":1,"type":"event","tool":"evm-stream","name":"a","chain":"c","chain_id":1,"block_number":1,"emitted_at":"2026-06-19T12:00:00Z","data":{}}`
	badJSON := `{"schema_version":1,"type":"event"` // truncated
	badSchema := `{"schema_version":99,"type":"event","tool":"evm-stream","name":"x","chain":"c","chain_id":1,"block_number":1,"emitted_at":"2026-06-19T12:00:00Z","data":{}}`
	good2 := `{"schema_version":1,"type":"event","tool":"evm-stream","name":"b","chain":"c","chain_id":1,"block_number":2,"emitted_at":"2026-06-19T12:00:00Z","data":{}}`
	stream := strings.Join([]string{good1, badJSON, badSchema, good2}, "\n") + "\n"

	type poison struct {
		line string
		err  error
	}
	var quarantined []poison
	r := NewReader(strings.NewReader(stream))
	r.Quarantine(func(line []byte, err error) error {
		quarantined = append(quarantined, poison{line: string(line), err: err})
		return nil
	})

	var names []string
	for {
		env, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error with quarantine installed: %v", err)
		}
		names = append(names, env.Name)
	}

	if want := []string{"a", "b"}; !reflect.DeepEqual(names, want) {
		t.Errorf("decoded names = %v, want %v (poison lines should be skipped)", names, want)
	}
	if len(quarantined) != 2 {
		t.Fatalf("quarantined %d lines, want 2", len(quarantined))
	}
	if quarantined[0].line != badJSON {
		t.Errorf("first quarantined line = %q, want the verbatim bad-JSON line %q", quarantined[0].line, badJSON)
	}
	if !strings.Contains(quarantined[0].err.Error(), "decode line") {
		t.Errorf("first quarantine error should mention decode, got: %v", quarantined[0].err)
	}
	if quarantined[1].line != badSchema {
		t.Errorf("second quarantined line = %q, want the verbatim bad-schema line %q", quarantined[1].line, badSchema)
	}
	if !errors.Is(quarantined[1].err, ErrSchemaUnsupported) {
		t.Errorf("second quarantine error should be ErrSchemaUnsupported, got: %v", quarantined[1].err)
	}
}

// TestReaderQuarantineErrorIsFatal verifies that if the quarantine hook itself
// fails (e.g. the dead-letter write failed) the error is propagated from Next and
// is fatal — a record is never dropped without a durable record of it.
func TestReaderQuarantineErrorIsFatal(t *testing.T) {
	sentinel := errors.New("dead-letter write failed")
	r := NewReader(strings.NewReader(`{bad` + "\n" + `{"schema_version":1,"type":"event","name":"never"}` + "\n"))
	r.Quarantine(func([]byte, error) error { return sentinel })
	_, err := r.Next()
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected the hook error to propagate from Next, got: %v", err)
	}
}

// TestReaderRawStableAcrossNext confirms Raw() reflects the most recent line and
// that a copy taken before the next Next is not corrupted by buffer reuse.
func TestReaderRawStableAcrossNext(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	cases := goldenCases()
	if err := w.Emit(cases[0].env); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if err := w.Emit(cases[1].env); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	r := NewReader(bytes.NewReader(buf.Bytes()))
	if _, err := r.Next(); err != nil {
		t.Fatalf("Next 1: %v", err)
	}
	first := append([]byte(nil), r.Raw()...)
	if _, err := r.Next(); err != nil {
		t.Fatalf("Next 2: %v", err)
	}
	second := r.Raw()
	if bytes.Equal(first, second) {
		t.Errorf("expected the two raw lines to differ; buffer reuse may have corrupted the first copy")
	}
	// The first copy must still be valid JSON of the first record's type.
	var m map[string]json.RawMessage
	if err := json.Unmarshal(first, &m); err != nil {
		t.Fatalf("first raw copy not valid JSON: %v", err)
	}
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
