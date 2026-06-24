package cli

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/transport"
)

// TestStdoutSinkRunWritesRecords drives the full evm-sink-stdout run path: a
// producer listens on a socket and writes two records; the sink dials in with
// --input, reads them, and writes their verbatim lines to its stdout (captured
// here). Its own logs go to stderr, so the captured stdout is records only.
func TestStdoutSinkRunWritesRecords(t *testing.T) {
	payload := twoRecordPayload(t)
	sock := sinkSocketPath(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Producer: listen, write both records once the sink connects, hold until done.
	go func() {
		out, err := transport.OpenWriter(ctx, "unix:"+sock, transport.WriterOptions{BlockUntilConsumer: true})
		if err != nil {
			return
		}
		_, _ = io.WriteString(out, payload)
		<-ctx.Done()
		_ = out.Close()
	}()

	type res struct {
		out string
		err error
	}
	resCh := make(chan res, 1)
	go func() {
		out, err := runSink(ctx, t, ToolSinkStdout, "", "run", "--metrics-addr", ":0", "--input", "unix:"+sock)
		resCh <- res{out, err}
	}()

	// Give the sink time to connect, read, and write both records, then stop it
	// (a UDS reader reconnects on EOF, so it won't self-terminate).
	time.Sleep(400 * time.Millisecond)
	cancel()

	var got res
	select {
	case got = <-resCh:
	case <-time.After(3 * time.Second):
		t.Fatal("sink did not shut down within 3s of context cancel")
	}
	if got.err != nil {
		t.Fatalf("run returned error: %v\n%s", got.err, got.out)
	}
	if strings.Count(got.out, `"type":"event"`) != 1 || strings.Count(got.out, `"type":"balance_sample"`) != 1 {
		t.Errorf("expected both records on the sink's stdout, got:\n%s", got.out)
	}
}
