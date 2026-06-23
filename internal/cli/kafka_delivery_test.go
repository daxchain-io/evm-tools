package cli

import (
	"context"
	"strings"
	"testing"
)

// TestSinkValidateDeliveryMode verifies the kafka.delivery_mode knob accepts the
// canonical modes (and the "plain" alias) and rejects anything else.
func TestSinkValidateDeliveryMode(t *testing.T) {
	good := []string{"at-least-once", "plain", "idempotent"}
	for _, mode := range good {
		cfg := writeStreamConfig(t, `
[kafka]
brokers = ["localhost:9092"]
topic = "t"
delivery_mode = "`+mode+`"
`)
		out, err := runSink(context.Background(), t, ToolSinkKafka, "", "validate", "--config", cfg)
		if err != nil {
			t.Fatalf("delivery_mode=%q: unexpected error: %v\n%s", mode, err, out)
		}
		if !strings.Contains(out, "ok:") {
			t.Errorf("delivery_mode=%q: expected ok, got:\n%s", mode, out)
		}
	}

	cfg := writeStreamConfig(t, `
[kafka]
brokers = ["localhost:9092"]
topic = "t"
delivery_mode = "exactly-once"
`)
	_, err := runSink(context.Background(), t, ToolSinkKafka, "", "validate", "--config", cfg)
	if err == nil || !strings.Contains(err.Error(), "delivery_mode") {
		t.Fatalf("expected a delivery_mode rejection, got: %v", err)
	}
}

// TestSinkDeliveryModeDefault verifies the default (no delivery_mode set) is
// at-least-once — i.e. validation passes with no idempotent requirement.
func TestSinkDeliveryModeDefault(t *testing.T) {
	cfg := writeStreamConfig(t, `
[kafka]
brokers = ["localhost:9092"]
topic = "t"
`)
	out, err := runSink(context.Background(), t, ToolSinkKafka, "", "validate", "--config", cfg)
	if err != nil {
		t.Fatalf("validate (default delivery): %v\n%s", err, out)
	}
}
