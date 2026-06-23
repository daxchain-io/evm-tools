package deadletter

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestWriterRecordsLosslessJSONL verifies the dead-letter file is valid JSONL,
// one Entry per poison record, and that the original bytes round-trip exactly via
// base64 — even when the poison line itself is not valid UTF-8.
func TestWriterRecordsLosslessJSONL(t *testing.T) {
	// Place the file under a non-existent subdir to also prove dir creation.
	path := filepath.Join(t.TempDir(), "quarantine", "dead-letter.jsonl")
	w, err := NewWriter(path, "evm-sink-kafka")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	poison := [][]byte{
		[]byte(`{"schema_version":1,"type":"event"`), // truncated JSON
		{0xff, 0xfe, 0x00, 0x42},                     // not valid UTF-8
	}
	causes := []error{errors.New("record: decode line: unexpected EOF"), errors.New("record: trailing data after JSON object on line")}
	for i, p := range poison {
		if err := w.Record(p, causes[i]); err != nil {
			t.Fatalf("Record[%d]: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open dead-letter file: %v", err)
	}
	defer func() { _ = f.Close() }()

	var entries []Entry
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == "" {
			continue
		}
		var e Entry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("dead-letter line is not valid JSON: %v\n%s", err, sc.Text())
		}
		entries = append(entries, e)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if len(entries) != len(poison) {
		t.Fatalf("got %d entries, want %d", len(entries), len(poison))
	}
	for i, e := range entries {
		if e.Sink != "evm-sink-kafka" {
			t.Errorf("entry[%d] sink = %q, want evm-sink-kafka", i, e.Sink)
		}
		if e.Error != causes[i].Error() {
			t.Errorf("entry[%d] error = %q, want %q", i, e.Error, causes[i].Error())
		}
		if e.QuarantinedAt == "" {
			t.Errorf("entry[%d] missing quarantined_at", i)
		}
		got, err := base64.StdEncoding.DecodeString(e.RecordBase64)
		if err != nil {
			t.Fatalf("entry[%d] record_base64 not decodable: %v", i, err)
		}
		if string(got) != string(poison[i]) {
			t.Errorf("entry[%d] round-trip = %q, want %q (lossless)", i, got, poison[i])
		}
	}
}

// TestWriterAppends verifies a second Writer over the same path appends rather
// than truncating, so a restart keeps prior quarantine history.
func TestWriterAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dl.jsonl")
	w1, err := NewWriter(path, "evm-sink-file")
	if err != nil {
		t.Fatalf("NewWriter 1: %v", err)
	}
	if err := w1.Record([]byte("first"), errors.New("e1")); err != nil {
		t.Fatalf("Record 1: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	w2, err := NewWriter(path, "evm-sink-file")
	if err != nil {
		t.Fatalf("NewWriter 2: %v", err)
	}
	if err := w2.Record([]byte("second"), errors.New("e2")); err != nil {
		t.Fatalf("Record 2: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close 2: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if n := strings.Count(strings.TrimRight(string(data), "\n"), "\n") + 1; n != 2 {
		t.Fatalf("dead-letter file has %d lines after append, want 2:\n%s", n, data)
	}
}

// TestWriterFileMode verifies the dead-letter file is created with restrictive
// 0600 permissions (it may contain sensitive record payloads). Skipped on Windows,
// which does not honor Unix permission bits (Stat reports 0666).
func TestWriterFileMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not honor Unix file permission bits")
	}
	path := filepath.Join(t.TempDir(), "dl.jsonl")
	w, err := NewWriter(path, "evm-sink-redis")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = w.Close() }()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("dead-letter file mode = %o, want 600", perm)
	}
}

// TestNewWriterEmptyPath rejects an empty path rather than silently creating a
// file named "".
func TestNewWriterEmptyPath(t *testing.T) {
	if _, err := NewWriter("", "evm-sink-kafka"); err == nil {
		t.Fatal("expected an error for an empty dead-letter path")
	}
}

// TestRecordAfterCloseErrors verifies a Record racing in after Close returns an
// error rather than nil-dereferencing the file (a sink's reader goroutine can
// outlive a cancelled read and quarantine a late line after the deferred Close).
func TestRecordAfterCloseErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dl.jsonl")
	w, err := NewWriter(path, "evm-sink-kafka")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := w.Record([]byte("late"), errors.New("boom")); err == nil {
		t.Fatal("expected an error writing to a closed dead-letter writer")
	}
	// Close is idempotent.
	if err := w.Close(); err != nil {
		t.Fatalf("second Close should be a no-op: %v", err)
	}
}
