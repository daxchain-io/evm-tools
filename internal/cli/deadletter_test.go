package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/daxchain-io/evm-tools/internal/deadletter"
	"github.com/daxchain-io/evm-tools/internal/record"
)

// poisonStdin builds a JSONL stream of [valid, invalid-JSON, bad-schema, valid].
// It returns the stream plus the two verbatim poison lines for assertion.
func poisonStdin(t *testing.T) (stream string, poison []string) {
	t.Helper()
	var sb strings.Builder
	w := record.NewWriter(&sb)
	li := uint64(0)
	mk := func(name string, block uint64) record.Envelope {
		return record.Envelope{
			Type: record.TypeEvent, Tool: record.ToolStream, Name: name,
			Chain: "c", ChainID: 1, BlockNumber: block, TxHash: "0x1", LogIndex: &li,
			Data: record.EventData{Event: "Transfer", Signature: "x", Contract: "0xc", Params: map[string]string{}},
		}
	}
	if err := w.Emit(mk("good1", 1)); err != nil {
		t.Fatal(err)
	}
	badJSON := `{"schema_version":1,"type":"event"` // truncated
	badSchema := `{"schema_version":999,"type":"event"}`
	sb.WriteString(badJSON + "\n")
	sb.WriteString(badSchema + "\n")
	if err := w.Emit(mk("good2", 2)); err != nil {
		t.Fatal(err)
	}
	return sb.String(), []string{badJSON, badSchema}
}

// readDeadLetter parses the dead-letter JSONL file into Entries.
func readDeadLetter(t *testing.T, path string) []deadletter.Entry {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read dead-letter file: %v", err)
	}
	var entries []deadletter.Entry
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == "" {
			continue
		}
		var e deadletter.Entry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("dead-letter line not valid JSON: %v\n%s", err, sc.Text())
		}
		entries = append(entries, e)
	}
	return entries
}

// TestSinkRunDeadLetterQuarantines drives the full kafka run path with a fake
// publisher and a dead-letter file: poison lines are quarantined and the run
// continues, delivering the surrounding valid records (no fail-fast).
func TestSinkRunDeadLetterQuarantines(t *testing.T) {
	fp := &cliFakePublisher{}
	withFakePublisher(t, fp)

	stream, poison := poisonStdin(t)
	dlPath := filepath.Join(t.TempDir(), "dl", "quarantine.jsonl")
	cfg := writeStreamConfig(t, `
[kafka]
brokers = ["localhost:9092"]
topic = "evm.events"
`)
	out, err := runSink(context.Background(), t, ToolSinkKafka, stream, "run", "--input", "-",
		"--config", cfg, "--metrics-addr", ":0", "--dead-letter-file", dlPath)
	if err != nil {
		t.Fatalf("run with dead-letter file should not fail: %v\n%s", err, out)
	}
	if fp.count() != 2 {
		t.Fatalf("expected 2 valid records published, got %d", fp.count())
	}

	entries := readDeadLetter(t, dlPath)
	if len(entries) != 2 {
		t.Fatalf("dead-letter file has %d entries, want 2", len(entries))
	}
	for i, e := range entries {
		if e.Sink != string(ToolSinkKafka) {
			t.Errorf("entry[%d] sink = %q, want %q", i, e.Sink, ToolSinkKafka)
		}
		got, derr := base64.StdEncoding.DecodeString(e.RecordBase64)
		if derr != nil {
			t.Fatalf("entry[%d] base64: %v", i, derr)
		}
		if string(got) != poison[i] {
			t.Errorf("entry[%d] quarantined = %q, want %q", i, got, poison[i])
		}
	}
}

// TestSinkRunFailsFastWithoutDeadLetter confirms the default contract is
// preserved: with no dead-letter file, a poison line halts the sink with a
// non-zero error rather than being silently skipped.
func TestSinkRunFailsFastWithoutDeadLetter(t *testing.T) {
	fp := &cliFakePublisher{}
	withFakePublisher(t, fp)

	stream, _ := poisonStdin(t)
	cfg := writeStreamConfig(t, `
[kafka]
brokers = ["localhost:9092"]
topic = "evm.events"
`)
	_, err := runSink(context.Background(), t, ToolSinkKafka, stream, "run", "--input", "-",
		"--config", cfg, "--metrics-addr", ":0")
	if err == nil {
		t.Fatal("expected a fail-fast error on a poison line without a dead-letter file")
	}
}

// TestSinkDeadLetterFlagOverridesConfig verifies --dead-letter-file wins over the
// top-level [dead_letter_file] config value.
func TestSinkDeadLetterFlagOverridesConfig(t *testing.T) {
	fp := &cliFakePublisher{}
	withFakePublisher(t, fp)

	stream, _ := poisonStdin(t)
	flagPath := filepath.Join(t.TempDir(), "flag.jsonl")
	cfgPath := filepath.Join(t.TempDir(), "config.jsonl")
	cfg := writeStreamConfig(t, `
dead_letter_file = "`+filepath.ToSlash(cfgPath)+`"

[kafka]
brokers = ["localhost:9092"]
topic = "evm.events"
`)
	out, err := runSink(context.Background(), t, ToolSinkKafka, stream, "run", "--input", "-",
		"--config", cfg, "--metrics-addr", ":0", "--dead-letter-file", flagPath)
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if _, statErr := os.Stat(flagPath); statErr != nil {
		t.Errorf("expected the flag dead-letter file to be written: %v", statErr)
	}
	if _, statErr := os.Stat(cfgPath); statErr == nil {
		t.Errorf("config dead-letter file should not be used when the flag is set")
	}
}

// TestSinkDeadLetterFromConfig verifies the [dead_letter_file] config value is
// honored when the flag is absent.
func TestSinkDeadLetterFromConfig(t *testing.T) {
	fp := &cliFakePublisher{}
	withFakePublisher(t, fp)

	stream, _ := poisonStdin(t)
	dlPath := filepath.Join(t.TempDir(), "from-config.jsonl")
	cfg := writeStreamConfig(t, `
dead_letter_file = "`+filepath.ToSlash(dlPath)+`"

[kafka]
brokers = ["localhost:9092"]
topic = "evm.events"
`)
	out, err := runSink(context.Background(), t, ToolSinkKafka, stream, "run", "--input", "-",
		"--config", cfg, "--metrics-addr", ":0")
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if entries := readDeadLetter(t, dlPath); len(entries) != 2 {
		t.Fatalf("dead-letter file has %d entries, want 2", len(entries))
	}
}
