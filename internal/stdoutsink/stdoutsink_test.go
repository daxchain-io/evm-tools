package stdoutsink

import (
	"context"
	"errors"
	"strings"
	"syscall"
	"testing"

	"github.com/daxchain-io/evm-tools/internal/record"
)

// sampleJSONL builds n event records as canonical JSONL (as the reader receives
// them on the wire).
func sampleJSONL(t *testing.T, n int) string {
	t.Helper()
	var sb strings.Builder
	w := record.NewWriter(&sb)
	li := uint64(0)
	for i := range n {
		if err := w.Emit(record.Envelope{
			Type: record.TypeEvent, Tool: record.ToolStream, Name: "usdc",
			Chain: "itest", ChainID: 4242, BlockNumber: uint64(100 + i), TxHash: "0x1", LogIndex: &li,
			Data: record.EventData{Event: "Transfer", Signature: "Transfer(address,address,uint256)", Contract: "0xc", Params: map[string]string{"v": "1"}},
		}); err != nil {
			t.Fatalf("emit sample: %v", err)
		}
	}
	return sb.String()
}

// fakeWriter records the lines it is asked to write and can be made to fail on a
// chosen call.
type fakeWriter struct {
	lines  [][]byte
	failAt int // 1-based call index to fail on (0 = never)
	err    error
	n      int
}

func (w *fakeWriter) WriteLine(p []byte) error {
	w.n++
	if w.failAt != 0 && w.n == w.failAt {
		return w.err
	}
	w.lines = append(w.lines, append([]byte(nil), p...))
	return nil
}

func newSink(t *testing.T, jsonl string, w LineWriter) *Sink {
	t.Helper()
	s, err := New(Options{Reader: record.NewReader(strings.NewReader(jsonl)), Writer: w})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// TestWritesAllRecordsThenEOF verifies every record's verbatim line reaches the
// writer and a clean EOF returns nil.
func TestWritesAllRecordsThenEOF(t *testing.T) {
	w := &fakeWriter{}
	s := newSink(t, sampleJSONL(t, 3), w)
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(w.lines) != 3 {
		t.Fatalf("wrote %d lines, want 3", len(w.lines))
	}
	for i, line := range w.lines {
		if !strings.Contains(string(line), `"type":"event"`) {
			t.Errorf("line %d not a verbatim event record: %s", i, line)
		}
		if strings.Contains(string(line), "\n") {
			t.Errorf("line %d should not embed a newline (writer adds it): %q", i, line)
		}
	}
}

// TestBrokenPipeIsCleanStop verifies an EPIPE from the downstream ends the sink
// cleanly (nil) rather than as an error — the consumer simply left.
func TestBrokenPipeIsCleanStop(t *testing.T) {
	w := &fakeWriter{failAt: 1, err: syscall.EPIPE}
	s := newSink(t, sampleJSONL(t, 3), w)
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("EPIPE should be a clean stop, got: %v", err)
	}
}

// TestPermanentWriteErrorFailsFast verifies a non-EPIPE write error is terminal.
func TestPermanentWriteErrorFailsFast(t *testing.T) {
	w := &fakeWriter{failAt: 1, err: errors.New("device gone")}
	s := newSink(t, sampleJSONL(t, 3), w)
	err := s.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "write to stdout") {
		t.Fatalf("want a write-to-stdout error, got: %v", err)
	}
}

// TestContextCancelStops verifies a cancelled context returns nil promptly.
func TestContextCancelStops(t *testing.T) {
	w := &fakeWriter{}
	s := newSink(t, sampleJSONL(t, 1), w)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Run(ctx); err != nil {
		t.Fatalf("cancelled ctx should return nil, got: %v", err)
	}
}
