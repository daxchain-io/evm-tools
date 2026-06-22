package transport

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// socketPath returns a short temp socket path. A short prefix (not t.TempDir,
// which embeds the test name) keeps it under the OS sun_path limit (~104 bytes
// on macOS).
func socketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "evt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

func TestOpenStdDefaults(t *testing.T) {
	ctx := context.Background()
	// The stdout aliases write to the provided std writer (so cobra's
	// cmd.OutOrStdout redirection is preserved) and Close is a no-op.
	for _, s := range []string{"", "-", "stdout"} {
		var buf bytes.Buffer
		w, err := OpenWriter(ctx, s, &buf, WriterOptions{})
		if err != nil {
			t.Fatalf("OpenWriter(%q): %v", s, err)
		}
		if _, err := io.WriteString(w, "hi\n"); err != nil {
			t.Fatalf("write(%q): %v", s, err)
		}
		if err := w.Close(); err != nil { // no-op: must not close the underlying stream
			t.Errorf("Close writer(%q): %v", s, err)
		}
		if buf.String() != "hi\n" {
			t.Errorf("std writer(%q): got %q, want %q", s, buf.String(), "hi\n")
		}
	}
	// The stdin aliases read from the provided std reader.
	for _, s := range []string{"", "-", "stdin"} {
		r, err := OpenReader(ctx, s, strings.NewReader("yo\n"))
		if err != nil {
			t.Fatalf("OpenReader(%q): %v", s, err)
		}
		got, _ := io.ReadAll(r)
		if string(got) != "yo\n" {
			t.Errorf("std reader(%q): got %q, want %q", s, got, "yo\n")
		}
		_ = r.Close()
	}
	if _, err := OpenWriter(ctx, "tcp://x", io.Discard, WriterOptions{}); err == nil {
		t.Error("OpenWriter with unsupported spec: want error")
	}
	if _, err := OpenReader(ctx, "bogus:/x", strings.NewReader("")); err == nil {
		t.Error("OpenReader with unsupported spec: want error")
	}
}

func TestUnixRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	path := socketPath(t)
	const n = 5

	werr := make(chan error, 1)
	go func() {
		w, err := OpenWriter(ctx, "unix:"+path, os.Stdout, WriterOptions{BlockUntilConsumer: true}) // blocks until the reader connects
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

	r, err := OpenReader(ctx, "unix:"+path, os.Stdin) // retries until the producer listens
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer func() { _ = r.Close() }()

	sc := bufio.NewScanner(r)
	for i := range n {
		if !sc.Scan() {
			t.Fatalf("line %d: Scan returned false: %v", i, sc.Err())
		}
		if got, want := sc.Text(), fmt.Sprintf("rec-%d", i); got != want {
			t.Fatalf("line %d: got %q, want %q", i, got, want)
		}
	}
	if err := <-werr; err != nil {
		t.Fatalf("writer: %v", err)
	}
}

func TestUnixReconnectAcrossPeerRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	path := socketPath(t)

	// serveOnce listens, accepts one consumer, writes line, then closes —
	// simulating a producer that exits and is later restarted on the same path.
	serveOnce := func(line string) <-chan error {
		done := make(chan error, 1)
		go func() {
			w, err := OpenWriter(ctx, "unix:"+path, os.Stdout, WriterOptions{BlockUntilConsumer: true})
			if err != nil {
				done <- err
				return
			}
			if _, err := io.WriteString(w, line); err != nil {
				_ = w.Close()
				done <- err
				return
			}
			done <- w.Close()
		}()
		return done
	}

	done1 := serveOnce("a\n")
	r, err := OpenReader(ctx, "unix:"+path, os.Stdin)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer func() { _ = r.Close() }()

	sc := bufio.NewScanner(r)
	if !sc.Scan() || sc.Text() != "a" {
		t.Fatalf("round 1: got %q err %v", sc.Text(), sc.Err())
	}
	if err := <-done1; err != nil {
		t.Fatalf("round 1 writer: %v", err)
	}

	// Producer #1 is gone; the reader must transparently reconnect to #2.
	done2 := serveOnce("b\n")
	if !sc.Scan() || sc.Text() != "b" {
		t.Fatalf("round 2 (after reconnect): got %q err %v", sc.Text(), sc.Err())
	}
	if err := <-done2; err != nil {
		t.Fatalf("round 2 writer: %v", err)
	}
}

func TestRemoveStaleSocket(t *testing.T) {
	t.Run("removes a stale socket so listen succeeds", func(t *testing.T) {
		path := socketPath(t)
		ln, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		ln.(*net.UnixListener).SetUnlinkOnClose(false) // leave the file behind, as a crash would
		_ = ln.Close()
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected a stale socket file to remain: %v", err)
		}

		removeStaleSocket(path)
		ln2, err := net.Listen("unix", path) // must now succeed
		if err != nil {
			t.Fatalf("listen after removeStaleSocket: %v", err)
		}
		_ = ln2.Close()
	})

	t.Run("leaves a live socket alone", func(t *testing.T) {
		path := socketPath(t)
		ln, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ln.Close() }()

		removeStaleSocket(path) // a peer is listening; must not remove
		if _, err := net.Listen("unix", path); err == nil {
			t.Error("expected the live socket to be left in place (EADDRINUSE)")
		}
	})
}

// TestUnixFanOut verifies one producer delivers every record to multiple
// connected consumers.
func TestUnixFanOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	path := socketPath(t)
	const n = 5
	want := make([]string, n)
	for i := range n {
		want[i] = fmt.Sprintf("rec-%d", i)
	}

	werr := make(chan error, 1)
	go func() {
		w, err := OpenWriter(ctx, "unix:"+path, os.Stdout, WriterOptions{BlockUntilConsumer: true})
		if err != nil {
			werr <- err
			return
		}
		defer func() { _ = w.Close() }()
		// Wait until both consumers are registered so both receive every record.
		if err := w.(*unixFanoutWriter).waitForConns(2); err != nil {
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

	r1, err := OpenReader(ctx, "unix:"+path, os.Stdin)
	if err != nil {
		t.Fatalf("reader1: %v", err)
	}
	defer func() { _ = r1.Close() }()
	r2, err := OpenReader(ctx, "unix:"+path, os.Stdin)
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

// TestUnixFireAndForget verifies that with block-until-consumer off, a write with
// no connected consumer returns immediately (dropped) instead of blocking.
func TestUnixFireAndForget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	path := socketPath(t)

	w, err := OpenWriter(ctx, "unix:"+path, os.Stdout, WriterOptions{BlockUntilConsumer: false})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	defer func() { _ = w.Close() }()

	done := make(chan error, 1)
	go func() { _, werr := io.WriteString(w, "dropped\n"); done <- werr }()
	select {
	case werr := <-done:
		if werr != nil {
			t.Errorf("fire-and-forget write: %v", werr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write blocked with no consumer despite block-until-consumer=false")
	}
}

func TestReaderCtxCancelUnblocksRead(t *testing.T) {
	path := socketPath(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Producer accepts but sends nothing, holding the connection open until ctx.
	go func() {
		w, err := OpenWriter(ctx, "unix:"+path, os.Stdout, WriterOptions{BlockUntilConsumer: true})
		if err != nil {
			return
		}
		<-ctx.Done()
		_ = w.Close()
	}()

	r, err := OpenReader(ctx, "unix:"+path, os.Stdin)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer func() { _ = r.Close() }()

	readErr := make(chan error, 1)
	go func() {
		_, err := r.Read(make([]byte, 16)) // blocks: no data on the wire
		readErr <- err
	}()

	cancel()
	select {
	case err := <-readErr:
		if err == nil {
			t.Error("expected Read to return an error after ctx cancel")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Read did not unblock within 3s of ctx cancel")
	}
}
