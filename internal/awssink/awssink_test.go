package awssink

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/smithy-go"

	"github.com/daxchain-io/evm-tools/internal/record"
)

// fakePublisher is an in-memory Publisher for the run-loop tests. It can fail the
// next failN deliveries with failErr before succeeding.
type fakePublisher struct {
	mu      sync.Mutex
	got     []Message
	failN   int
	failErr error
}

func (p *fakePublisher) Publish(_ context.Context, msg Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failN > 0 {
		p.failN--
		return p.failErr
	}
	p.got = append(p.got, msg)
	return nil
}
func (p *fakePublisher) Reachable(context.Context) error { return nil }
func (p *fakePublisher) Target() string                  { return "fake" }
func (p *fakePublisher) Close() error                    { return nil }
func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.got)
}
func (p *fakePublisher) messages() []Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Message(nil), p.got...)
}

func jsonl(lines ...string) string { return strings.Join(lines, "\n") + "\n" }
func rec(typ, name string) string {
	return `{"schema_version":1,"type":"` + typ + `","name":"` + name + `","data":{}}`
}

func newSink(t *testing.T, in string, p Publisher, opt func(*Options)) *Sink {
	t.Helper()
	o := Options{
		Reader:      record.NewReader(strings.NewReader(in)),
		Publisher:   p,
		BackoffBase: time.Millisecond,
		BackoffMax:  2 * time.Millisecond,
		randInt:     func(int64) int64 { return 0 },
	}
	if opt != nil {
		opt(&o)
	}
	s, err := New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestDeliversAllRecords(t *testing.T) {
	p := &fakePublisher{}
	s := newSink(t, jsonl(rec("event", "USDT"), rec("event", "USDC")), p, nil)
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if p.count() != 2 {
		t.Errorf("delivered %d, want 2", p.count())
	}
}

func TestFIFOSetsHashedGroupAndDedup(t *testing.T) {
	p := &fakePublisher{}
	s := newSink(t, jsonl(rec("event", "USDT")), p, func(o *Options) { o.FIFO = true })
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs := p.messages()
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	m := msgs[0]
	if len(m.GroupID) != 64 || len(m.DedupID) != 64 {
		t.Errorf("FIFO ids should be 64-hex (got group=%d dedup=%d chars)", len(m.GroupID), len(m.DedupID))
	}
	// Deterministic and distinguishing: same key -> same id, different keys differ.
	if got1, got2 := fifoID("x"), fifoID("x"); got1 != got2 {
		t.Error("fifoID is not deterministic")
	}
	if fifoID("a") == fifoID("b") {
		t.Error("fifoID does not distinguish distinct keys")
	}
}

func TestNonFIFOOmitsGroupDedup(t *testing.T) {
	p := &fakePublisher{}
	s := newSink(t, jsonl(rec("event", "USDT")), p, nil) // FIFO false
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	m := p.messages()[0]
	if m.GroupID != "" || m.DedupID != "" {
		t.Errorf("non-FIFO must not set group/dedup ids, got %q/%q", m.GroupID, m.DedupID)
	}
}

func TestPermanentFailsFast(t *testing.T) {
	// A client-fault API error (e.g. AccessDenied) is permanent.
	p := &fakePublisher{failN: 1, failErr: &smithy.GenericAPIError{Code: "AccessDenied", Message: "no", Fault: smithy.FaultClient}}
	s := newSink(t, jsonl(rec("event", "USDT")), p, nil)
	err := s.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "permanent delivery failure") {
		t.Fatalf("want permanent delivery failure, got %v", err)
	}
}

func TestTransientRetriesThenSucceeds(t *testing.T) {
	// Throttling is a retryable client fault.
	p := &fakePublisher{failN: 2, failErr: &smithy.GenericAPIError{Code: "ThrottlingException", Message: "slow down", Fault: smithy.FaultClient}}
	s := newSink(t, jsonl(rec("event", "USDT")), p, nil)
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run should succeed after transient throttles: %v", err)
	}
	if p.count() != 1 {
		t.Errorf("record should be delivered exactly once, got %d", p.count())
	}
}

func TestOversizeRecordFailsFast(t *testing.T) {
	big := rec("event", strings.Repeat("x", maxMessageBytes)) // line exceeds the limit
	p := &fakePublisher{}
	s := newSink(t, big+"\n", p, nil)
	err := s.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "AWS message limit") {
		t.Fatalf("want an oversize error, got %v", err)
	}
	if p.count() != 0 {
		t.Errorf("oversize record must not be delivered")
	}
}

func TestContextCancelCleanStop(t *testing.T) {
	p := &fakePublisher{failN: 1 << 30, failErr: &smithy.GenericAPIError{Code: "InternalError", Message: "5xx", Fault: smithy.FaultServer}}
	s := newSink(t, jsonl(rec("event", "USDT")), p, func(o *Options) {
		o.BackoffBase = 50 * time.Millisecond
		o.BackoffMax = 50 * time.Millisecond
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.Run(ctx); err != nil {
		t.Fatalf("context cancel should be a clean stop, got %v", err)
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want FailureClass
	}{
		{"throttle", &smithy.GenericAPIError{Code: "ThrottlingException", Fault: smithy.FaultClient}, ClassTransient},
		{"too-many", &smithy.GenericAPIError{Code: "TooManyRequestsException", Fault: smithy.FaultClient}, ClassTransient},
		{"client 4xx", &smithy.GenericAPIError{Code: "AccessDenied", Fault: smithy.FaultClient}, ClassPermanent},
		{"not-found", &smithy.GenericAPIError{Code: "NotFound", Fault: smithy.FaultClient}, ClassPermanent},
		{"server 5xx", &smithy.GenericAPIError{Code: "InternalError", Fault: smithy.FaultServer}, ClassTransient},
		{"network/unknown", errors.New("dial tcp: timeout"), ClassTransient},
	}
	for _, c := range cases {
		if got := Classify(c.err); got != c.want {
			t.Errorf("Classify(%s) = %q, want %q", c.name, got, c.want)
		}
	}
}
