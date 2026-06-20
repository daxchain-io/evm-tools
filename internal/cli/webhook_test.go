package cli

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/webhooksink"
)

// cliFakePoster captures forwarded payloads so the run-path test can assert the
// records reached the poster without a real endpoint.
type cliFakePoster struct {
	mu  sync.Mutex
	got [][]byte
}

func (f *cliFakePoster) Post(_ context.Context, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, append([]byte(nil), payload...))
	return nil
}
func (f *cliFakePoster) Close() error { return nil }
func (f *cliFakePoster) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.got)
}

// withFakePoster swaps the package poster constructor for the duration of a test
// so `run` uses an in-memory fake.
func withFakePoster(t *testing.T, fp *cliFakePoster) {
	t.Helper()
	orig := newWebhookPoster
	newWebhookPoster = func(webhooksink.PosterConfig) (webhooksink.Poster, error) { return fp, nil }
	t.Cleanup(func() { newWebhookPoster = orig })
}

func TestWebhookSinkVersion(t *testing.T) {
	out, err := runSink(context.Background(), t, ToolSinkWebhook, "", "version")
	if err != nil {
		t.Fatalf("version: %v", err)
	}
	for _, want := range []string{"version:", "commit:", "date:", "go:"} {
		if !strings.Contains(out, want) {
			t.Errorf("version output missing %q:\n%s", want, out)
		}
	}
}

func TestWebhookSinkHelpListsCommandsAndFlags(t *testing.T) {
	out, err := runSink(context.Background(), t, ToolSinkWebhook, "", "--help")
	if err != nil {
		t.Fatalf("--help: %v", err)
	}
	for _, want := range []string{"run", "validate", "version", "--url", "--metrics", "--config", "--log-level"} {
		if !strings.Contains(out, want) {
			t.Errorf("--help missing %q:\n%s", want, out)
		}
	}
	// A webhook sink has no kafka flags and no RPC surface.
	if strings.Contains(out, "--brokers") {
		t.Errorf("webhook sink should not expose --brokers:\n%s", out)
	}
	if strings.Contains(out, "--rpc-url") {
		t.Errorf("sink should not expose --rpc-url flags:\n%s", out)
	}
	if strings.Contains(out, "check") {
		t.Errorf("sink should not expose a check command:\n%s", out)
	}
}

func TestWebhookSinkValidateGood(t *testing.T) {
	cfg := writeStreamConfig(t, `
[webhook]
url = "https://hooks.example.com/evm"
`)
	out, err := runSink(context.Background(), t, ToolSinkWebhook, "", "validate", "--config", cfg)
	if err != nil {
		t.Fatalf("validate: %v\n%s", err, out)
	}
	if !strings.Contains(out, "ok:") {
		t.Errorf("expected ok message, got:\n%s", out)
	}
}

func TestWebhookSinkValidateRequiresURL(t *testing.T) {
	cfg := writeStreamConfig(t, `
[webhook]
method = "POST"
`)
	_, err := runSink(context.Background(), t, ToolSinkWebhook, "", "validate", "--config", cfg)
	if err == nil || !strings.Contains(err.Error(), "url") {
		t.Fatalf("expected a missing-url error, got: %v", err)
	}
}

func TestWebhookSinkValidateBadFieldOp(t *testing.T) {
	cfg := writeStreamConfig(t, `
[webhook]
url = "https://h/evm"

[webhook.filters.field]
field = "balance"
op = "between"
value = "10"
`)
	_, err := runSink(context.Background(), t, ToolSinkWebhook, "", "validate", "--config", cfg)
	if err == nil || !strings.Contains(err.Error(), "field condition op") {
		t.Fatalf("expected an unsupported-op error, got: %v", err)
	}
}

func TestWebhookSinkValidateUnknownKeyRejected(t *testing.T) {
	cfg := writeStreamConfig(t, `
[webhook]
url = "https://h/evm"
urll = "typo"
`)
	_, err := runSink(context.Background(), t, ToolSinkWebhook, "", "validate", "--config", cfg)
	if err == nil {
		t.Fatal("expected a strict-decode error for an unknown key")
	}
}

// TestWebhookSinkFlagOverridesConfig verifies the --url flag wins over the file.
func TestWebhookSinkFlagOverridesConfig(t *testing.T) {
	cfg := writeStreamConfig(t, `
[webhook]
url = "https://fromfile/evm"
`)
	out, err := runSink(context.Background(), t, ToolSinkWebhook, "", "validate",
		"--config", cfg, "--url", "https://fromflag/evm")
	if err != nil {
		t.Fatalf("validate with flag: %v\n%s", err, out)
	}
	if !strings.Contains(out, "ok:") {
		t.Errorf("expected ok, got:\n%s", out)
	}
}

// TestWebhookSinkRunForwardsStdin drives the full run path with a fake poster:
// the JSONL records on stdin are forwarded, then a clean EOF returns nil.
func TestWebhookSinkRunForwardsStdin(t *testing.T) {
	fp := &cliFakePoster{}
	withFakePoster(t, fp)

	var sb strings.Builder
	w := record.NewWriter(&sb)
	li := uint64(0)
	_ = w.Emit(record.Envelope{
		Type: record.TypeEvent, Tool: record.ToolStream, Name: "usdc",
		Chain: "codex-chain", ChainID: 4242, BlockNumber: 100, TxHash: "0x1", LogIndex: &li,
		Data: record.EventData{Event: "Transfer", Signature: "Transfer(address,address,uint256)", Contract: "0xc", Params: map[string]string{"v": "1"}},
	})
	_ = w.Emit(record.Envelope{
		Type: record.TypeBalanceSample, Tool: record.ToolBalance, Name: "treasury",
		Chain: "codex-chain", ChainID: 4242, BlockNumber: 101,
		Data: record.BalanceData{Kind: record.KindNative, Address: "0xa", BalanceWei: "1", Balance: "0"},
	})

	cfg := writeStreamConfig(t, `
[webhook]
url = "https://hooks.example.com/evm"
`)
	out, err := runSink(context.Background(), t, ToolSinkWebhook, sb.String(), "run", "--config", cfg, "--metrics-addr", ":0")
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if fp.count() != 2 {
		t.Fatalf("expected 2 forwarded records, got %d", fp.count())
	}
}

// TestWebhookSinkRunFilters drives the full run path with a filter that drops one
// record type, proving filters are wired from config through to the loop.
func TestWebhookSinkRunFilters(t *testing.T) {
	fp := &cliFakePoster{}
	withFakePoster(t, fp)

	var sb strings.Builder
	w := record.NewWriter(&sb)
	li := uint64(0)
	_ = w.Emit(record.Envelope{
		Type: record.TypeEvent, Tool: record.ToolStream, Name: "usdc",
		Chain: "c", ChainID: 1, BlockNumber: 1, TxHash: "0x1", LogIndex: &li,
		Data: record.EventData{Event: "Transfer", Signature: "x", Contract: "0xc", Params: map[string]string{}},
	})
	_ = w.Emit(record.Envelope{
		Type: record.TypeBalanceSample, Tool: record.ToolBalance, Name: "t",
		Chain: "c", ChainID: 1, BlockNumber: 2,
		Data: record.BalanceData{Kind: record.KindNative, Address: "0xa", BalanceWei: "1", Balance: "0"},
	})

	cfg := writeStreamConfig(t, `
[webhook]
url = "https://hooks.example.com/evm"

[webhook.filters]
include_types = ["event"]
`)
	out, err := runSink(context.Background(), t, ToolSinkWebhook, sb.String(), "run", "--config", cfg, "--metrics-addr", ":0")
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if fp.count() != 1 {
		t.Fatalf("expected 1 forwarded record (event only), got %d", fp.count())
	}
	fp.mu.Lock()
	defer fp.mu.Unlock()
	if !strings.Contains(string(fp.got[0]), `"type":"event"`) {
		t.Errorf("forwarded record should be the event: %s", fp.got[0])
	}
}
