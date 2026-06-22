package cli

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/transport"
)

// sinkSocketPath returns a short temp socket path (short prefix keeps it under
// the macOS sun_path limit).
func sinkSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "evt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

func waitForCount(t *testing.T, fp *cliFakePublisher, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fp.count() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d published records, have %d", want, fp.count())
}

// twoRecordPayload is the JSONL a producer writes for the end-to-end tests.
func twoRecordPayload(t *testing.T) string {
	t.Helper()
	var sb strings.Builder
	w := record.NewWriter(&sb)
	li := uint64(0)
	if err := w.Emit(record.Envelope{
		Type: record.TypeEvent, Tool: record.ToolStream, Name: "usdc",
		Chain: "my-chain", ChainID: 4242, BlockNumber: 100, TxHash: "0x1", LogIndex: &li,
		Data: record.EventData{Event: "Transfer", Signature: "Transfer(address,address,uint256)", Contract: "0xc", Params: map[string]string{"v": "1"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Emit(record.Envelope{
		Type: record.TypeBalanceSample, Tool: record.ToolBalance, Name: "treasury",
		Chain: "my-chain", ChainID: 4242, BlockNumber: 101,
		Data: record.BalanceData{Kind: record.KindNative, Address: "0xa", BalanceWei: "1", Balance: "0"},
	}); err != nil {
		t.Fatal(err)
	}
	return sb.String()
}

// TestSinkRunInputUnix is the end-to-end 1:1 UDS handoff: a producer listens on a
// Unix socket and writes records; the sink, driven with --input unix:/path,
// dials in, reads them, and publishes them — no shell pipe, no stdin.
func TestSinkRunInputUnix(t *testing.T) {
	fp := &cliFakePublisher{}
	withFakePublisher(t, fp)

	payload := twoRecordPayload(t)
	sock := sinkSocketPath(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Producer: listen on the socket, write the records once a consumer connects,
	// then hold the connection open until shutdown.
	go func() {
		out, err := transport.OpenWriter(ctx, "unix:"+sock, os.Stdout, transport.WriterOptions{BlockUntilConsumer: true})
		if err != nil {
			return
		}
		_, _ = io.WriteString(out, payload)
		<-ctx.Done()
		_ = out.Close()
	}()

	cfg := writeStreamConfig(t, `
[kafka]
brokers = ["localhost:9092"]
topic = "evm.events"
`)
	sinkErr := make(chan error, 1)
	go func() {
		_, err := runSink(ctx, t, ToolSinkKafka, "", "run", "--config", cfg, "--metrics-addr", ":0", "--input", "unix:"+sock)
		sinkErr <- err
	}()

	// Both records should flow over the socket and reach the publisher; then stop
	// the sink (a UDS reader reconnects on EOF, so it won't self-terminate).
	waitForCount(t, fp, 2, 10*time.Second)
	cancel()
	<-sinkErr
	if fp.count() != 2 {
		t.Fatalf("expected 2 published records over UDS, got %d", fp.count())
	}
}

// TestSinkValidateAcceptsInputFlag confirms --input is bound on sinks and does not
// open the socket at validate time (validate stays an offline preflight).
func TestSinkValidateAcceptsInputFlag(t *testing.T) {
	cfg := writeStreamConfig(t, `
[kafka]
brokers = ["localhost:9092"]
topic = "t"
`)
	if _, err := runSink(context.Background(), t, ToolSinkKafka, "", "validate", "--config", cfg, "--input", "unix:/tmp/evm-in.sock"); err != nil {
		t.Fatalf("validate with --input should succeed without opening the socket: %v", err)
	}
}

// TestOutputInputSpecPrecedence verifies flag-over-config precedence for the
// transport specs: an explicit flag wins, an unset flag falls back to config, and
// no config means the stdout/stdin default.
func TestOutputInputSpecPrecedence(t *testing.T) {
	// outputSpec is a pure resolver.
	f := &sharedFlags{}
	cmd := &cobra.Command{}
	cmd.Flags().StringVar(&f.output, "output", "", "")
	if err := cmd.Flags().Set("output", "flag-out"); err != nil {
		t.Fatal(err)
	}
	if got := f.outputSpec(cmd, "cfg-out"); got != "flag-out" {
		t.Errorf("flag set: outputSpec = %q, want flag-out", got)
	}

	f2 := &sharedFlags{}
	cmd2 := &cobra.Command{}
	cmd2.Flags().StringVar(&f2.output, "output", "", "")
	if got := f2.outputSpec(cmd2, "cfg-out"); got != "cfg-out" {
		t.Errorf("flag unset: outputSpec = %q, want cfg-out", got)
	}
	if got := f2.outputSpec(cmd2, ""); got != "" {
		t.Errorf("no flag, no config: outputSpec = %q, want empty", got)
	}

	// openInput resolves the same way: flag --input=- (stdin) must win over a
	// config socket spec. If config wrongly won, dialing the nonexistent socket
	// would retry until the short ctx expires and error.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	fs := &sinkFlags{}
	scmd := &cobra.Command{}
	scmd.SetIn(strings.NewReader("x\n"))
	scmd.SetContext(ctx)
	scmd.Flags().StringVar(&fs.input, "input", "", "")
	if err := scmd.Flags().Set("input", "-"); err != nil {
		t.Fatal(err)
	}
	in, err := fs.openInput(scmd, "unix:/nonexistent.sock")
	if err != nil {
		t.Fatalf("openInput should resolve to stdin (flag wins): %v", err)
	}
	_ = in.Close()
}

// TestStreamValidateAcceptsOutputFlag confirms --output is bound on producers and
// does not listen at validate time.
func TestStreamValidateAcceptsOutputFlag(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "empty"))
	if _, err := runWithCtx(context.Background(), t, ToolStream, "validate",
		"--rpc-url", "https://x", "--native-transfers", "--output", "unix:/tmp/evm-out.sock"); err != nil {
		t.Fatalf("validate with --output should succeed without listening: %v", err)
	}
}
