//go:build windows

package transport

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

var pipeCounter atomic.Int64

// pipeSpec returns a unique "pipe:" spec for a test run (no backslashes, so it is
// a bare name that pipePath expands to \\.\pipe\<name>).
func pipeSpec(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("pipe:evm-test-%d-%d", os.Getpid(), pipeCounter.Add(1))
}

func TestPipePath(t *testing.T) {
	if got, want := pipePath("evm"), `\\.\pipe\evm`; got != want {
		t.Errorf("pipePath(bare) = %q, want %q", got, want)
	}
	if got, want := pipePath(`\\.\pipe\custom`), `\\.\pipe\custom`; got != want {
		t.Errorf("pipePath(full) = %q, want %q", got, want)
	}
}

// TestPipeRoundTrip is the end-to-end named-pipe hand-off: a producer listens, a
// sink dials, and the records arrive in order.
func TestPipeRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	spec := pipeSpec(t)
	const n = 5

	werr := make(chan error, 1)
	go func() {
		w, err := OpenWriter(ctx, spec, os.Stdout, WriterOptions{BlockUntilConsumer: true})
		if err != nil {
			werr <- err
			return
		}
		defer func() { _ = w.Close() }()
		for i := range n {
			if _, err := io.WriteString(w, fmt.Sprintf("rec-%d\n", i)); err != nil {
				werr <- err
				return
			}
		}
		werr <- nil
	}()

	r, err := OpenReader(ctx, spec, os.Stdin)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer func() { _ = r.Close() }()
	sc := bufio.NewScanner(r)
	for i := range n {
		if !sc.Scan() {
			t.Fatalf("line %d: Scan false: %v", i, sc.Err())
		}
		if got, want := sc.Text(), fmt.Sprintf("rec-%d", i); got != want {
			t.Fatalf("line %d: got %q, want %q", i, got, want)
		}
	}
	if err := <-werr; err != nil {
		t.Fatalf("writer: %v", err)
	}
}

// TestPipeFanOut verifies one producer delivers every record to two named-pipe
// consumers.
func TestPipeFanOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	spec := pipeSpec(t)
	const n = 5
	want := make([]string, n)
	for i := range n {
		want[i] = fmt.Sprintf("rec-%d", i)
	}

	werr := make(chan error, 1)
	go func() {
		w, err := OpenWriter(ctx, spec, os.Stdout, WriterOptions{BlockUntilConsumer: true})
		if err != nil {
			werr <- err
			return
		}
		defer func() { _ = w.Close() }()
		if err := w.(*fanoutWriter).waitForConns(2); err != nil {
			werr <- err
			return
		}
		for i := range n {
			if _, err := io.WriteString(w, want[i]+"\n"); err != nil {
				werr <- err
				return
			}
		}
		werr <- nil
	}()

	r1, err := OpenReader(ctx, spec, os.Stdin)
	if err != nil {
		t.Fatalf("reader1: %v", err)
	}
	defer func() { _ = r1.Close() }()
	r2, err := OpenReader(ctx, spec, os.Stdin)
	if err != nil {
		t.Fatalf("reader2: %v", err)
	}
	defer func() { _ = r2.Close() }()

	readN := func(r io.Reader) []string {
		sc := bufio.NewScanner(r)
		got := make([]string, 0, n)
		for i := 0; i < n && sc.Scan(); i++ {
			got = append(got, sc.Text())
		}
		return got
	}
	got := make([][]string, 2)
	done := make(chan struct{}, 2)
	go func() { got[0] = readN(r1); done <- struct{}{} }()
	go func() { got[1] = readN(r2); done <- struct{}{} }()
	<-done
	<-done
	if err := <-werr; err != nil {
		t.Fatalf("writer: %v", err)
	}
	for i, g := range got {
		if !slices.Equal(g, want) {
			t.Errorf("reader%d got %v, want %v", i+1, g, want)
		}
	}
}
