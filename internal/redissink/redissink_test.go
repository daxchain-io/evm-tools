package redissink

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/record"
)

// redisErr is a test stand-in for a Redis server reply error: it implements the
// go-redis Error interface (error + RedisError()), which has no public
// constructor, so Classify's errors.As against redis.Error matches it.
type redisErr string

func (e redisErr) Error() string { return string(e) }
func (e redisErr) RedisError()   {}

type fakeAppender struct {
	mu       sync.Mutex
	added    int
	deduped  int
	failN    int
	failErr  error
	dedupKey string // when the next env's dedup key matches, report deduped (added=false)
}

func (f *fakeAppender) Append(_ context.Context, env record.Envelope, _ []byte) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failN > 0 {
		f.failN--
		return false, f.failErr
	}
	if f.dedupKey != "" && env.DedupKey() == f.dedupKey {
		f.deduped++
		return false, nil
	}
	f.added++
	return true, nil
}
func (f *fakeAppender) Reachable(context.Context) error { return nil }
func (f *fakeAppender) Target() string                  { return "redis://fake:6379/0" }
func (f *fakeAppender) Close() error                    { return nil }
func (f *fakeAppender) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.added, f.deduped
}

func jsonl(lines ...string) string { return strings.Join(lines, "\n") + "\n" }
func rec(typ, name string) string {
	return `{"schema_version":1,"type":"` + typ + `","name":"` + name + `","data":{}}`
}

func newSink(t *testing.T, in string, ap Appender, opt func(*Options)) *Sink {
	t.Helper()
	o := Options{
		Reader:      record.NewReader(strings.NewReader(in)),
		Appender:    ap,
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

func TestAppendsAllRecords(t *testing.T) {
	ap := &fakeAppender{}
	s := newSink(t, jsonl(rec("event", "USDT"), rec("native_transfer", "native")), ap, nil)
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	added, _ := ap.counts()
	if added != 2 {
		t.Errorf("appended %d, want 2", added)
	}
}

func TestDeduplicatedRecordStillAdvances(t *testing.T) {
	// The first record's append reports deduped (added=false); the run must still
	// advance and treat it as delivered (no error, no stall). balance_sample dedup
	// keys include the name, so the two records have distinct keys.
	first := record.Envelope{SchemaVersion: 1, Type: record.TypeBalanceSample, Name: "acct-a"}
	ap := &fakeAppender{dedupKey: first.DedupKey()}
	s := newSink(t, jsonl(rec("balance_sample", "acct-a"), rec("balance_sample", "acct-b")), ap, nil)
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	added, deduped := ap.counts()
	if deduped != 1 || added != 1 {
		t.Errorf("want 1 deduped + 1 added, got deduped=%d added=%d", deduped, added)
	}
}

func TestPermanentFailsFast(t *testing.T) {
	ap := &fakeAppender{failN: 1, failErr: redisErr("WRONGTYPE Operation against a key holding the wrong kind of value")}
	s := newSink(t, jsonl(rec("event", "USDT")), ap, nil)
	err := s.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "permanent append failure") {
		t.Fatalf("want permanent append failure, got %v", err)
	}
}

func TestTransientRetriesThenSucceeds(t *testing.T) {
	ap := &fakeAppender{failN: 2, failErr: redisErr("LOADING Redis is loading the dataset in memory")}
	s := newSink(t, jsonl(rec("event", "USDT")), ap, nil)
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run should succeed after transient LOADING: %v", err)
	}
	added, _ := ap.counts()
	if added != 1 {
		t.Errorf("record should be appended exactly once, got %d", added)
	}
}

func TestContextCancelCleanStop(t *testing.T) {
	ap := &fakeAppender{failN: 1 << 30, failErr: errors.New("dial tcp: connection refused")}
	s := newSink(t, jsonl(rec("event", "USDT")), ap, func(o *Options) {
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
		{"wrongtype", redisErr("WRONGTYPE bad key"), ClassPermanent},
		{"noauth", redisErr("NOAUTH Authentication required."), ClassPermanent},
		{"wrongpass", redisErr("WRONGPASS invalid username-password pair"), ClassPermanent},
		{"noperm", redisErr("NOPERM this user has no permissions"), ClassPermanent},
		{"crossslot", redisErr("CROSSSLOT Keys in request don't hash to the same slot"), ClassPermanent},
		{"loading", redisErr("LOADING Redis is loading"), ClassTransient},
		{"clusterdown", redisErr("CLUSTERDOWN The cluster is down"), ClassTransient},
		{"tryagain", redisErr("TRYAGAIN Multiple keys request during rehashing"), ClassTransient},
		{"network", errors.New("dial tcp: i/o timeout"), ClassTransient},
	}
	for _, c := range cases {
		if got := Classify(c.err); got != c.want {
			t.Errorf("Classify(%s) = %q, want %q", c.name, got, c.want)
		}
	}
}
