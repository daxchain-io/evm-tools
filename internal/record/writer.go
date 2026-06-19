package record

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"
)

// Writer is the single synchronized sink for records. It serializes concurrent
// producers onto one underlying stream, encodes each record as one JSON object
// followed by a newline, writes that line as a single atomic operation, and
// flushes after every line so a downstream jq/tail/sink sees each record
// promptly.
//
// A Writer is safe for concurrent use by multiple goroutines. Because the
// underlying write blocks when a downstream pipe fills, calls to Emit/Write
// propagate backpressure to their callers — records are never dropped or
// buffered without bound.
type Writer struct {
	mu  sync.Mutex
	bw  *bufio.Writer
	enc *json.Encoder
	// now is injectable for deterministic tests; defaults to time.Now.
	now func() time.Time
}

// NewWriter returns a Writer that emits to w. Pass os.Stdout in production.
func NewWriter(w io.Writer) *Writer {
	bw := bufio.NewWriter(w)
	enc := json.NewEncoder(bw)
	// Keep raw 0x-hex and signatures intact; do not escape <, >, & in strings.
	enc.SetEscapeHTML(false)
	return &Writer{
		bw:  bw,
		enc: enc,
		now: time.Now,
	}
}

// Emit stamps EmittedAt (if unset) with the current wall-clock time, sets the
// schema version, and writes the envelope as one atomic JSONL line. It returns
// any error from encoding or the underlying write so backpressure and I/O
// failures surface to the caller.
func (w *Writer) Emit(env Envelope) error {
	env.SchemaVersion = SchemaVersion
	if env.EmittedAt == "" {
		env.EmittedAt = RFC3339(w.now())
	}
	return w.write(env)
}

// write holds the lock for the full encode+flush so concurrent monitors never
// interleave partial lines on the shared stream.
func (w *Writer) write(env Envelope) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// json.Encoder.Encode appends a trailing newline, so the object plus its
	// newline reach the buffered writer together.
	if err := w.enc.Encode(env); err != nil {
		return fmt.Errorf("encode record: %w", err)
	}
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("flush record: %w", err)
	}
	return nil
}

// Flush flushes any buffered bytes to the underlying writer. Emit already
// flushes per line; Flush exists for shutdown paths that want to be explicit.
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bw.Flush()
}
