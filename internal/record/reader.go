package record

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// ErrSchemaUnsupported reports a record whose schema_version the reader does not
// understand. Per the contract (docs/design.md, "Versioning rules") a sink
// accepts the schema_version values it understands and rejects any other value
// — higher (newer, would-be-mishandled) or lower (older, no longer supported) —
// with a clear error rather than best-effort parsing it.
var ErrSchemaUnsupported = errors.New("record: unsupported schema_version")

// SchemaError carries the offending schema_version so callers can report it
// without re-parsing the line.
type SchemaError struct {
	Got      int
	Accepted int
}

func (e *SchemaError) Error() string {
	return fmt.Sprintf("record: unsupported schema_version %d (this build accepts %d)", e.Got, e.Accepted)
}

func (e *SchemaError) Is(target error) bool { return target == ErrSchemaUnsupported }

// Reader decodes a JSONL stream of records into [Envelope] values. It is the
// shared counterpart to [Writer]: producers encode through Writer, and any sink
// (evm-sink-kafka, evm-sink-webhook) decodes through Reader, so the contract
// stays in sync by construction.
//
// The reader is line-oriented and streaming: it reads one JSON object per line
// and never buffers the whole stream, so backpressure from a slow downstream
// consumer propagates back up the pipe to the lossless producer. Blank lines are
// skipped. A Reader is not safe for concurrent use.
type Reader struct {
	sc *bufio.Scanner
	// raw holds the bytes of the most recently decoded line so a caller (e.g. a
	// sink that forwards the original payload verbatim) can reuse them without
	// re-encoding.
	raw []byte

	// quarantine, when non-nil, makes a malformed/unsupported line recoverable:
	// instead of failing fast, Next hands the verbatim line and the parse error
	// to this hook and continues to the next line. See [Reader.Quarantine].
	quarantine QuarantineFunc

	// reqCh/resCh drive the single background reader goroutine that NextCtx uses
	// to make a blocking read cancellable. Lazily created on first NextCtx use.
	reqCh chan struct{}
	resCh chan readResult
}

// QuarantineFunc handles a poison line: one that is not valid JSON, carries
// trailing data after the object, or has an unsupported schema_version. It
// receives a private copy of the verbatim line (without the trailing newline)
// and the parse error. Returning nil tells the reader to skip the line and
// continue; returning an error makes that error fatal (propagated from Next),
// because a record must never be dropped without a durable record of it — so a
// dead-letter write failure halts the sink rather than silently losing data.
type QuarantineFunc func(line []byte, err error) error

// Quarantine installs (or, with nil, removes) a poison-line handler. With no
// handler the reader is fail-fast by default: a malformed line is a hard error
// and the sink exits non-zero, preserving the contract that a record is never
// silently skipped. With a handler installed, malformed lines are routed to it
// (e.g. appended to a dead-letter file and counted) and the reader continues.
// Set this before the first Next/NextCtx; it is not safe to change concurrently
// with a read.
func (r *Reader) Quarantine(fn QuarantineFunc) { r.quarantine = fn }

// readResult is one cancellable-read outcome: the decoded envelope, a private
// copy of its raw bytes, and any error.
type readResult struct {
	env Envelope
	raw []byte
	err error
}

// maxLineBytes bounds a single JSONL line. Records are descriptive but bounded;
// a 16 MiB ceiling comfortably covers any realistic envelope while protecting a
// sink from an unbounded line on a corrupt stream.
const maxLineBytes = 16 << 20

// NewReader returns a Reader over r. Pass os.Stdin in a sink.
func NewReader(r io.Reader) *Reader {
	sc := bufio.NewScanner(r)
	// Grow the token buffer up to maxLineBytes so long-but-valid lines decode
	// rather than failing with bufio.ErrTooLong.
	sc.Buffer(make([]byte, 0, 64*1024), maxLineBytes)
	return &Reader{sc: sc}
}

// Next reads, decodes, and validates the next record. It returns io.EOF when the
// stream is exhausted. A malformed line is a hard error (the stream is the
// contract; a sink must not silently skip a record it cannot parse). A record
// whose schema_version this build does not accept returns a [*SchemaError]
// (matchable with errors.Is(err, ErrSchemaUnsupported)).
func (r *Reader) Next() (Envelope, error) {
	for {
		if !r.sc.Scan() {
			if err := r.sc.Err(); err != nil {
				return Envelope{}, fmt.Errorf("record: read line: %w", err)
			}
			return Envelope{}, io.EOF
		}
		line := r.sc.Bytes()
		if len(trimSpace(line)) == 0 {
			// Skip blank lines between records.
			continue
		}
		// Copy the line: bufio.Scanner reuses its buffer on the next Scan, so a
		// caller that keeps r.raw past the next Next would otherwise see it
		// overwritten.
		r.raw = append(r.raw[:0], line...)

		var env Envelope
		dec := json.NewDecoder(newBytesReader(r.raw))
		if err := dec.Decode(&env); err != nil {
			if e := r.handleMalformed(fmt.Errorf("record: decode line: %w", err)); e != nil {
				return Envelope{}, e
			}
			continue
		}
		// A line must contain exactly one JSON object. json.Decoder.Decode stops
		// after the first value and would silently ignore a concatenated second
		// object or trailing garbage; treat that as a hard error so a malformed
		// line fails fast rather than dropping the trailing record(s).
		if dec.More() {
			if e := r.handleMalformed(fmt.Errorf("record: trailing data after JSON object on line")); e != nil {
				return Envelope{}, e
			}
			continue
		}
		if env.SchemaVersion != SchemaVersion {
			if e := r.handleMalformed(&SchemaError{Got: env.SchemaVersion, Accepted: SchemaVersion}); e != nil {
				return Envelope{}, e
			}
			continue
		}
		return env, nil
	}
}

// handleMalformed routes a poison line through the quarantine hook. With no hook
// (the default) it returns err unchanged, so Next fails fast and the malformed
// line halts the sink — the stream is the contract and a record is never silently
// skipped. With a hook installed it hands the verbatim line + err to the hook and
// returns its result: nil tells Next to skip the line and continue; a non-nil
// error (e.g. the dead-letter write itself failed) is fatal, so a record is never
// dropped without a durable record of it. err already describes the failure
// (invalid JSON / trailing data / unsupported schema_version); *SchemaError still
// matches errors.Is(err, ErrSchemaUnsupported) for the hook.
func (r *Reader) handleMalformed(err error) error {
	if r.quarantine == nil {
		return err
	}
	return r.quarantine(append([]byte(nil), r.raw...), err)
}

// Raw returns the verbatim bytes of the line most recently returned by Next,
// without the trailing newline. The slice is valid until the next call to Next;
// copy it if it must outlive that call. This lets a forwarding sink (webhook)
// POST the original payload byte-for-byte rather than a re-encoding that might
// reorder fields.
func (r *Reader) Raw() []byte { return r.raw }

// NextCtx behaves like Next but returns ctx.Err() promptly if ctx is cancelled
// while a read is blocked, so a signal stops an idle sink instead of waiting for
// stdin to close. It returns the decoded envelope and a private copy of its raw
// bytes (valid independently of any later call). The blocking read runs in a
// single background goroutine started on first use; a blocked OS read cannot be
// interrupted, so on cancellation that goroutine is abandoned and exits when the
// underlying stream finally yields a line or closes. NextCtx must be called from
// a single goroutine (like Next); do not mix Next and NextCtx on one Reader.
func (r *Reader) NextCtx(ctx context.Context) (Envelope, []byte, error) {
	if r.reqCh == nil {
		r.reqCh = make(chan struct{})
		r.resCh = make(chan readResult, 1) // buffered so an abandoned read can finish
		go r.readLoop()
	}
	select {
	case <-ctx.Done():
		return Envelope{}, nil, ctx.Err()
	case r.reqCh <- struct{}{}:
	}
	select {
	case <-ctx.Done():
		return Envelope{}, nil, ctx.Err()
	case res := <-r.resCh:
		// res.raw is a private copy; callers use it directly (not Raw()), so only
		// the reader goroutine ever touches r.raw — no cross-goroutine access.
		return res.env, res.raw, res.err
	}
}

// readLoop serves NextCtx requests: one blocking Next per request, returning the
// result on resCh with a private copy of the raw bytes. It exits after the first
// EOF/error (no further reads are possible).
func (r *Reader) readLoop() {
	for range r.reqCh {
		env, err := r.Next()
		var raw []byte
		if err == nil {
			raw = append([]byte(nil), r.raw...)
		}
		r.resCh <- readResult{env: env, raw: raw, err: err}
		if err != nil {
			return
		}
	}
}

// trimSpace trims ASCII whitespace from both ends of b without allocating.
func trimSpace(b []byte) []byte {
	start := 0
	for start < len(b) && isSpace(b[start]) {
		start++
	}
	end := len(b)
	for end > start && isSpace(b[end-1]) {
		end--
	}
	return b[start:end]
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n'
}

// newBytesReader wraps a byte slice as an io.Reader for json.Decoder without
// pulling in bytes.Reader's extra surface; a tiny local type keeps the
// allocation profile predictable.
func newBytesReader(b []byte) io.Reader { return &sliceReader{b: b} }

type sliceReader struct {
	b   []byte
	pos int
}

func (s *sliceReader) Read(p []byte) (int, error) {
	if s.pos >= len(s.b) {
		return 0, io.EOF
	}
	n := copy(p, s.b[s.pos:])
	s.pos += n
	return n, nil
}
