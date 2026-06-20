package cli

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/daxchain-io/evm-tools/internal/kafkasink"
	"github.com/daxchain-io/evm-tools/internal/record"
)

// runSink executes a sink root command with args and a stdin string, capturing
// stdout/stderr and returning the error.
func runSink(ctx context.Context, t *testing.T, tool SinkTool, stdin string, args ...string) (string, error) {
	t.Helper()
	root := NewSinkRootCommand(tool)
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetIn(strings.NewReader(stdin))
	root.SetArgs(args)
	if ctx != nil {
		root.SetContext(ctx)
	}
	err := root.Execute()
	return out.String(), err
}

// cliFakePublisher captures published messages so the run-path test can assert
// the records reached the publisher without a real broker.
type cliFakePublisher struct {
	mu        sync.Mutex
	published []kafkasink.Message
}

func (f *cliFakePublisher) Publish(_ context.Context, msg kafkasink.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published = append(f.published, kafkasink.Message{
		Topic: msg.Topic,
		Key:   append([]byte(nil), msg.Key...),
		Value: append([]byte(nil), msg.Value...),
	})
	return nil
}
func (f *cliFakePublisher) Close() error { return nil }
func (f *cliFakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.published)
}

// withFakePublisher swaps the package publisher constructor for the duration of a
// test so `run` uses an in-memory fake.
func withFakePublisher(t *testing.T, fp *cliFakePublisher) {
	t.Helper()
	orig := newKafkaPublisher
	newKafkaPublisher = func(kafkasink.WriterConfig) (kafkasink.Publisher, error) { return fp, nil }
	t.Cleanup(func() { newKafkaPublisher = orig })
}

func TestSinkVersion(t *testing.T) {
	out, err := runSink(context.Background(), t, ToolSinkKafka, "", "version")
	if err != nil {
		t.Fatalf("version: %v", err)
	}
	for _, want := range []string{"version:", "commit:", "date:", "go:"} {
		if !strings.Contains(out, want) {
			t.Errorf("version output missing %q:\n%s", want, out)
		}
	}
}

func TestSinkHelpListsCommandsAndFlags(t *testing.T) {
	out, err := runSink(context.Background(), t, ToolSinkKafka, "", "--help")
	if err != nil {
		t.Fatalf("--help: %v", err)
	}
	for _, want := range []string{"run", "validate", "version", "--brokers", "--topic", "--metrics", "--config", "--log-level"} {
		if !strings.Contains(out, want) {
			t.Errorf("--help missing %q:\n%s", want, out)
		}
	}
	// A sink has no RPC surface.
	if strings.Contains(out, "--rpc-url") {
		t.Errorf("sink should not expose --rpc-url flags:\n%s", out)
	}
	if strings.Contains(out, "check") {
		t.Errorf("sink should not expose a check command:\n%s", out)
	}
}

func TestSinkValidateGood(t *testing.T) {
	cfg := writeStreamConfig(t, `
[kafka]
brokers = ["localhost:9092"]
topic = "evm.events"
`)
	out, err := runSink(context.Background(), t, ToolSinkKafka, "", "validate", "--config", cfg)
	if err != nil {
		t.Fatalf("validate: %v\n%s", err, out)
	}
	if !strings.Contains(out, "ok:") {
		t.Errorf("expected ok message, got:\n%s", out)
	}
}

func TestSinkValidateRequiresBrokers(t *testing.T) {
	cfg := writeStreamConfig(t, `
[kafka]
topic = "evm.events"
`)
	_, err := runSink(context.Background(), t, ToolSinkKafka, "", "validate", "--config", cfg)
	if err == nil || !strings.Contains(err.Error(), "brokers") {
		t.Fatalf("expected a missing-brokers error, got: %v", err)
	}
}

func TestSinkValidateRequiresTopic(t *testing.T) {
	cfg := writeStreamConfig(t, `
[kafka]
brokers = ["localhost:9092"]
`)
	_, err := runSink(context.Background(), t, ToolSinkKafka, "", "validate", "--config", cfg)
	if err == nil || !strings.Contains(err.Error(), "topic") {
		t.Fatalf("expected a missing-topic error, got: %v", err)
	}
}

func TestSinkValidateBadRequiredAcks(t *testing.T) {
	cfg := writeStreamConfig(t, `
[kafka]
brokers = ["localhost:9092"]
topic = "t"
required_acks = "one"
`)
	_, err := runSink(context.Background(), t, ToolSinkKafka, "", "validate", "--config", cfg)
	if err == nil || !strings.Contains(err.Error(), "at-least-once") {
		t.Fatalf("expected required_acks rejection, got: %v", err)
	}
}

func TestSinkValidateBadPartitionKey(t *testing.T) {
	cfg := writeStreamConfig(t, `
[kafka]
brokers = ["localhost:9092"]
topic = "t"
partition_key = "bogus"
`)
	_, err := runSink(context.Background(), t, ToolSinkKafka, "", "validate", "--config", cfg)
	if err == nil || !strings.Contains(err.Error(), "partition_key") {
		t.Fatalf("expected partition_key rejection, got: %v", err)
	}
}

func TestSinkValidateUnknownKeyRejected(t *testing.T) {
	cfg := writeStreamConfig(t, `
[kafka]
brokers = ["localhost:9092"]
topic = "t"
toppic = "typo"
`)
	_, err := runSink(context.Background(), t, ToolSinkKafka, "", "validate", "--config", cfg)
	if err == nil {
		t.Fatal("expected a strict-decode error for an unknown key")
	}
}

// TestSinkFlagsOverrideConfig verifies --brokers/--topic flags win over the file.
func TestSinkFlagsOverrideConfig(t *testing.T) {
	cfg := writeStreamConfig(t, `
[kafka]
brokers = ["fromfile:9092"]
topic = "fromfile"
`)
	out, err := runSink(context.Background(), t, ToolSinkKafka, "", "validate",
		"--config", cfg, "--brokers", "flag1:9092,flag2:9092", "--topic", "flagtopic")
	if err != nil {
		t.Fatalf("validate with flags: %v\n%s", err, out)
	}
	if !strings.Contains(out, "ok:") {
		t.Errorf("expected ok, got:\n%s", out)
	}
}

// TestSinkRunPublishesStdin drives the full run path with a fake publisher: the
// JSONL records on stdin are published, then a clean EOF returns nil.
func TestSinkRunPublishesStdin(t *testing.T) {
	fp := &cliFakePublisher{}
	withFakePublisher(t, fp)

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
[kafka]
brokers = ["localhost:9092"]
topic = "evm.events"
`)
	out, err := runSink(context.Background(), t, ToolSinkKafka, sb.String(), "run", "--config", cfg, "--metrics-addr", ":0")
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if fp.count() != 2 {
		t.Fatalf("expected 2 published records, got %d", fp.count())
	}
}

// TestSinkRunTopicRouting verifies per-type topic routing through the full run
// path with a fake publisher.
func TestSinkRunTopicRouting(t *testing.T) {
	fp := &cliFakePublisher{}
	withFakePublisher(t, fp)

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
[kafka]
brokers = ["localhost:9092"]
topic = "evm.default"

[kafka.topic_by_type]
event = "evm.events"
`)
	out, err := runSink(context.Background(), t, ToolSinkKafka, sb.String(), "run", "--config", cfg, "--metrics-addr", ":0")
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	fp.mu.Lock()
	defer fp.mu.Unlock()
	if len(fp.published) != 2 {
		t.Fatalf("expected 2 records, got %d", len(fp.published))
	}
	if fp.published[0].Topic != "evm.events" {
		t.Errorf("event routed to %q, want evm.events", fp.published[0].Topic)
	}
	if fp.published[1].Topic != "evm.default" {
		t.Errorf("balance_sample routed to %q, want evm.default", fp.published[1].Topic)
	}
}
