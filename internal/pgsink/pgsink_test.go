package pgsink

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daxchain-io/evm-tools/internal/record"
)

type fakeInserter struct {
	mu      sync.Mutex
	got     int
	failN   int
	failErr error
}

func (f *fakeInserter) Insert(_ context.Context, _ record.Envelope, _ []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failN > 0 {
		f.failN--
		return f.failErr
	}
	f.got++
	return nil
}
func (f *fakeInserter) Reachable(context.Context) error { return nil }
func (f *fakeInserter) Target() string                  { return "postgres://fake/db" }
func (f *fakeInserter) Close() error                    { return nil }
func (f *fakeInserter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.got
}

func jsonl(lines ...string) string { return strings.Join(lines, "\n") + "\n" }
func rec(typ, name string) string {
	return `{"schema_version":1,"type":"` + typ + `","name":"` + name + `","data":{}}`
}

func newSink(t *testing.T, in string, ins Inserter, opt func(*Options)) *Sink {
	t.Helper()
	o := Options{
		Reader:      record.NewReader(strings.NewReader(in)),
		Inserter:    ins,
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

func TestInsertsAllRecords(t *testing.T) {
	ins := &fakeInserter{}
	s := newSink(t, jsonl(rec("event", "USDT"), rec("event", "USDC")), ins, nil)
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if ins.count() != 2 {
		t.Errorf("inserted %d, want 2", ins.count())
	}
}

func TestPermanentFailsFast(t *testing.T) {
	// Class 42 (undefined_table) is a schema error: permanent.
	ins := &fakeInserter{failN: 1, failErr: &pgconn.PgError{Code: "42P01", Message: "relation does not exist"}}
	s := newSink(t, jsonl(rec("event", "USDT")), ins, nil)
	err := s.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "permanent insert failure") {
		t.Fatalf("want permanent insert failure, got %v", err)
	}
}

func TestTransientRetriesThenSucceeds(t *testing.T) {
	// Class 40 (deadlock) is retryable.
	ins := &fakeInserter{failN: 2, failErr: &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}}
	s := newSink(t, jsonl(rec("event", "USDT")), ins, nil)
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run should succeed after transient deadlocks: %v", err)
	}
	if ins.count() != 1 {
		t.Errorf("record should be inserted exactly once, got %d", ins.count())
	}
}

func TestContextCancelCleanStop(t *testing.T) {
	ins := &fakeInserter{failN: 1 << 30, failErr: &pgconn.PgError{Code: "08006", Message: "connection failure"}}
	s := newSink(t, jsonl(rec("event", "USDT")), ins, func(o *Options) {
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
		{"connection 08", &pgconn.PgError{Code: "08006"}, ClassTransient},
		{"deadlock 40", &pgconn.PgError{Code: "40P01"}, ClassTransient},
		{"insufficient-resources 53", &pgconn.PgError{Code: "53100"}, ClassTransient},
		{"operator-intervention 57", &pgconn.PgError{Code: "57P01"}, ClassTransient},
		{"undefined-table 42", &pgconn.PgError{Code: "42P01"}, ClassPermanent},
		{"not-null 23", &pgconn.PgError{Code: "23502"}, ClassPermanent},
		{"data-exception 22", &pgconn.PgError{Code: "22001"}, ClassPermanent},
		{"network/unknown", errors.New("dial: timeout"), ClassTransient},
	}
	for _, c := range cases {
		if got := Classify(c.err); got != c.want {
			t.Errorf("Classify(%s) = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestValidateTableName(t *testing.T) {
	ok := []string{"evm_records", "public.evm_records", "_t", "a1.b2"}
	bad := []string{"", "bad;name", "a-b", "1table", "a.b.c", "drop table x", `a"b`}
	for _, s := range ok {
		if err := ValidateTableName(s); err != nil {
			t.Errorf("ValidateTableName(%q) unexpected error: %v", s, err)
		}
	}
	for _, s := range bad {
		if err := ValidateTableName(s); err == nil {
			t.Errorf("ValidateTableName(%q) should have errored", s)
		}
	}
}
