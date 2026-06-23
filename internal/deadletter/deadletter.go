// Package deadletter provides an opt-in quarantine destination for poison
// records: stdin lines a sink cannot parse (invalid JSON, trailing data after
// the object, or an unsupported schema_version).
//
// By default the sinks are fail-fast — a malformed line is a hard error and the
// sink exits non-zero, because the stream is the contract and a record is never
// silently skipped (see docs/design.md). Operationally, though, a single corrupt
// byte in a long-lived pipe halts the sink until manual intervention. A
// dead-letter file is the opt-in alternative: the poison line is appended here
// and the sink continues. The file *is* the record of it, so nothing is dropped.
//
// The Writer plugs into [record.Reader] via its Quarantine hook; the wiring lives
// in the shared sink CLI layer, so every sink gets the behavior uniformly.
package deadletter

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Entry is one quarantined poison record, serialized as a single JSONL line in
// the dead-letter file. The original line is stored base64-encoded so the
// dead-letter file is itself valid, lossless JSONL regardless of the poison bytes
// (the offending line is by definition not a well-formed record). Recover the
// original with e.g. `jq -r .record_base64 dead-letter.jsonl | base64 -d`.
type Entry struct {
	QuarantinedAt string `json:"quarantined_at"` // RFC3339 (UTC) wall-clock time
	Sink          string `json:"sink"`           // which sink quarantined it
	Error         string `json:"error"`          // the parse failure
	RecordBase64  string `json:"record_base64"`  // the verbatim original line
}

// Writer appends poison records to a dead-letter file as JSONL, one [Entry] per
// line. It is the shared quarantine destination wired onto [record.Reader.Quarantine]:
// a sink with a dead-letter file configured routes lines it cannot parse here
// instead of halting. Writes are line-atomic and fsync'd — poison records are
// rare, so per-line durability is cheap and guarantees the file survives a crash,
// matching the "never drop a record" contract. A Writer is safe for concurrent
// use.
type Writer struct {
	path string
	sink string
	now  func() time.Time

	mu sync.Mutex
	f  *os.File
}

// NewWriter opens (creating it and any parent directories) the dead-letter file
// at path for append, and tags every entry with sink. The file and its directory
// are created with restrictive permissions (0600/0700) because quarantined record
// payloads may carry sensitive data. The caller must Close the Writer.
func NewWriter(path, sink string) (*Writer, error) {
	if path == "" {
		return nil, fmt.Errorf("deadletter: empty file path")
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("deadletter: create dir %s: %w", dir, err)
		}
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("deadletter: open %s: %w", path, err)
	}
	return &Writer{path: path, sink: sink, now: time.Now, f: f}, nil
}

// Path returns the dead-letter file path (for logging).
func (w *Writer) Path() string { return w.path }

// Record appends one poison line and its parse error to the dead-letter file,
// fsync'ing before it returns so the entry is durable. line is the verbatim
// original (without the trailing newline); cause is the reader's parse error. It
// returns an error only if the durable write fails — and the caller (the reader
// Quarantine hook) treats that as fatal, so a record is never lost without a
// trace. This matches the [record.QuarantineFunc] signature.
func (w *Writer) Record(line []byte, cause error) error {
	entry := Entry{
		QuarantinedAt: w.now().UTC().Format(time.RFC3339Nano),
		Sink:          w.sink,
		Error:         errString(cause),
		RecordBase64:  base64.StdEncoding.EncodeToString(line),
	}
	buf, err := json.Marshal(entry)
	if err != nil {
		// json.Marshal of this fixed struct cannot fail in practice; guard anyway.
		return fmt.Errorf("deadletter: marshal entry: %w", err)
	}
	buf = append(buf, '\n')

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		// Closed already. A sink's reader runs in a background goroutine that can
		// outlive a cancelled NextCtx (a blocked stdin read is abandoned, not
		// interrupted); if such a read finally yields a poison line after the
		// deferred Close, return an error rather than nil-dereferencing the file.
		return fmt.Errorf("deadletter: write to closed writer %s", w.path)
	}
	if _, err := w.f.Write(buf); err != nil {
		return fmt.Errorf("deadletter: write %s: %w", w.path, err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("deadletter: sync %s: %w", w.path, err)
	}
	return nil
}

// Close flushes and closes the dead-letter file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	if err != nil {
		return fmt.Errorf("deadletter: close %s: %w", w.path, err)
	}
	return nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
