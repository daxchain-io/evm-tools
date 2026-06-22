package transport

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
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
	for _, s := range []string{"", "-", "stdout"} {
		w, err := OpenWriter(ctx, s)
		if err != nil {
			t.Fatalf("OpenWriter(%q): %v", s, err)
		}
		if err := w.Close(); err != nil { // no-op: must not close os.Stdout
			t.Errorf("Close writer(%q): %v", s, err)
		}
	}
	for _, s := range []string{"", "-", "stdin"} {
		r, err := OpenReader(ctx, s)
		if err != nil {
			t.Fatalf("OpenReader(%q): %v", s, err)
		}
		_ = r.Close()
	}
	if _, err := OpenWriter(ctx, "tcp://x"); err == nil {
		t.Error("OpenWriter with unsupported spec: want error")
	}
	if _, err := OpenReader(ctx, "bogus:/x"); err == nil {
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
		w, err := OpenWriter(ctx, "unix:"+path) // blocks until the reader connects
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

	r, err := OpenReader(ctx, "unix:"+path) // retries until the producer listens
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
			w, err := OpenWriter(ctx, "unix:"+path)
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
	r, err := OpenReader(ctx, "unix:"+path)
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

func TestReaderCtxCancelUnblocksRead(t *testing.T) {
	path := socketPath(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Producer accepts but sends nothing, holding the connection open until ctx.
	go func() {
		w, err := OpenWriter(ctx, "unix:"+path)
		if err != nil {
			return
		}
		<-ctx.Done()
		_ = w.Close()
	}()

	r, err := OpenReader(ctx, "unix:"+path)
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
