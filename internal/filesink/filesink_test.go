package filesink

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/record"
)

// fakeWriter is an in-memory FileWriter for the run-loop tests. It can be told to
// fail the next failN writes with failErr before succeeding.
type fakeWriter struct {
	lines   []string
	failN   int
	failErr error
	syncs   int
	closed  bool
}

func (w *fakeWriter) Write(line []byte) (int, error) {
	if w.failN > 0 {
		w.failN--
		return 0, w.failErr
	}
	w.lines = append(w.lines, string(line))
	return len(line) + 1, nil
}
func (w *fakeWriter) Sync() error  { w.syncs++; return nil }
func (w *fakeWriter) Size() int64  { return int64(len(w.lines)) }
func (w *fakeWriter) Close() error { w.closed = true; return nil }

// jsonl builds a newline-delimited stream of minimal valid envelopes.
func jsonl(lines ...string) string { return strings.Join(lines, "\n") + "\n" }

func rec(typ, name string) string {
	return `{"schema_version":1,"type":"` + typ + `","name":"` + name + `","data":{}}`
}

func newSink(t *testing.T, in string, w FileWriter, opts func(*Options)) *Sink {
	t.Helper()
	o := Options{
		Reader:      record.NewReader(strings.NewReader(in)),
		Writer:      w,
		BackoffBase: time.Millisecond,
		BackoffMax:  2 * time.Millisecond,
		randInt:     func(int64) int64 { return 0 },
	}
	if opts != nil {
		opts(&o)
	}
	s, err := New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestSinkWritesAllRecords(t *testing.T) {
	w := &fakeWriter{}
	in := jsonl(rec("event", "USDT"), rec("event", "USDC"), rec("native_transfer", "ETH"))
	s := newSink(t, in, w, nil)
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(w.lines) != 3 {
		t.Errorf("wrote %d lines, want 3", len(w.lines))
	}
}

func TestSinkFiltersRecords(t *testing.T) {
	w := &fakeWriter{}
	in := jsonl(rec("event", "USDT"), rec("native_transfer", "ETH"), rec("event", "USDC"))
	s := newSink(t, in, w, func(o *Options) {
		o.Filter = NewFilter(FilterOptions{IncludeTypes: []string{"event"}})
	})
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(w.lines) != 2 {
		t.Errorf("wrote %d lines, want 2 (only events)", len(w.lines))
	}
	for _, l := range w.lines {
		if !strings.Contains(l, `"type":"event"`) {
			t.Errorf("non-event record written: %s", l)
		}
	}
}

func TestSinkCleanEOFReturnsNil(t *testing.T) {
	w := &fakeWriter{}
	s := newSink(t, jsonl(rec("event", "USDT")), w, nil)
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("clean EOF should return nil, got %v", err)
	}
}

func TestSinkMalformedLineFailsFast(t *testing.T) {
	w := &fakeWriter{}
	s := newSink(t, "{not json}\n", w, nil)
	if err := s.Run(context.Background()); err == nil {
		t.Fatal("expected an error on a malformed line")
	}
	if len(w.lines) != 0 {
		t.Errorf("nothing should be written for a malformed stream, got %d", len(w.lines))
	}
}

func TestSinkUnsupportedSchemaFailsFast(t *testing.T) {
	w := &fakeWriter{}
	s := newSink(t, `{"schema_version":99,"type":"event","name":"x","data":{}}`+"\n", w, nil)
	err := s.Run(context.Background())
	if err == nil {
		t.Fatal("expected an error on an unsupported schema_version")
	}
	if !errors.Is(err, record.ErrSchemaUnsupported) {
		t.Errorf("want ErrSchemaUnsupported, got %v", err)
	}
}

func TestSinkPermanentWriteFailsFast(t *testing.T) {
	w := &fakeWriter{failN: 1, failErr: errors.New("read-only filesystem")}
	s := newSink(t, jsonl(rec("event", "USDT")), w, nil)
	err := s.Run(context.Background())
	if err == nil {
		t.Fatal("expected a permanent write failure to be returned")
	}
	if !strings.Contains(err.Error(), "permanent write failure") {
		t.Errorf("want permanent write failure, got %v", err)
	}
}

func TestSinkTransientWriteRetriesThenSucceeds(t *testing.T) {
	// Two transient failures, then success — the sink must retry, not drop.
	w := &fakeWriter{failN: 2, failErr: &TransientError{Reason: "disk full"}}
	var retries int
	s := newSink(t, jsonl(rec("event", "USDT")), w, func(o *Options) {
		o.Metrics = &countingMetrics{onRetry: func() { retries++ }}
	})
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run should succeed after transient failures: %v", err)
	}
	if len(w.lines) != 1 {
		t.Errorf("record should eventually be written exactly once, got %d", len(w.lines))
	}
	if retries != 2 {
		t.Errorf("retries = %d, want 2", retries)
	}
}

func TestSinkContextCancelStopsCleanly(t *testing.T) {
	// A writer stuck on a transient error; cancelling the context mid-backoff is a
	// clean stop (nil), not a failure.
	w := &fakeWriter{failN: 1 << 30, failErr: &TransientError{Reason: "disk full"}}
	s := newSink(t, jsonl(rec("event", "USDT")), w, func(o *Options) {
		o.BackoffBase = 50 * time.Millisecond
		o.BackoffMax = 50 * time.Millisecond
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.Run(ctx); err != nil {
		t.Fatalf("context cancel should be a clean stop, got %v", err)
	}
}

func TestSinkContextCancelDoesNotCountWrite(t *testing.T) {
	// A writer permanently stuck on a transient (disk-full) error: the record is
	// never durably appended. Cancelling mid-backoff is a clean stop, and the
	// written counter must stay at 0 — incrementing it here would over-count
	// evm_sink_file_records_written_total by one at shutdown.
	w := &fakeWriter{failN: 1 << 30, failErr: &TransientError{Reason: "disk full"}}
	mm := &countingMetrics{}
	s := newSink(t, jsonl(rec("event", "USDT")), w, func(o *Options) {
		o.BackoffBase = 50 * time.Millisecond
		o.BackoffMax = 50 * time.Millisecond
		o.Metrics = mm
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.Run(ctx); err != nil {
		t.Fatalf("context cancel should be a clean stop, got %v", err)
	}
	if len(w.lines) != 0 {
		t.Errorf("no record should be durably written, got %d", len(w.lines))
	}
	if mm.written != 0 {
		t.Errorf("written = %d, want 0 (record never written before cancel)", mm.written)
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want FailureClass
	}{
		{"enospc", syscall.ENOSPC, ClassTransient},
		{"edquot", syscall.EDQUOT, ClassTransient},
		{"wrapped enospc", fmt.Errorf("write block: %w", syscall.ENOSPC), ClassTransient},
		{"transient marker", &TransientError{Reason: "x"}, ClassTransient},
		{"generic permanent", errors.New("read-only filesystem"), ClassPermanent},
		{"eacces permanent", syscall.EACCES, ClassPermanent},
	}
	for _, c := range cases {
		if got := Classify(c.err); got != c.want {
			t.Errorf("Classify(%s) = %q, want %q", c.name, got, c.want)
		}
	}
}

// countingMetrics is a Metrics that invokes onRetry on each retry and tallies
// confirmed writes (other methods are no-ops). Used to assert retry/write
// counting without a full prometheus set.
type countingMetrics struct {
	noopMetrics
	onRetry func()
	written int
}

func (m *countingMetrics) IncRetry() {
	if m.onRetry != nil {
		m.onRetry()
	}
}

func (m *countingMetrics) IncWritten(string) { m.written++ }
