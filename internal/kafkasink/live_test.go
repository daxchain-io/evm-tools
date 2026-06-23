//go:build livekafka

// Package kafkasink live test. Build-tagged so the default offline `go test
// ./...` never needs a broker. Run against a real cluster with:
//
//	EVM_TOOLS_KAFKA_BROKERS=localhost:9092 \
//	  go test -tags livekafka ./internal/kafkasink/ -run TestLive -v
//
// Bring a local broker up with, e.g., `docker run -p 9092:9092 apache/kafka` or
// redpanda; create the topic first or enable auto-create on the broker.
package kafkasink

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/daxchain-io/evm-tools/internal/record"
)

func TestLiveKafkaPublish(t *testing.T) {
	brokersEnv := os.Getenv("EVM_TOOLS_KAFKA_BROKERS")
	if brokersEnv == "" {
		t.Skip("set EVM_TOOLS_KAFKA_BROKERS to run the live kafka test")
	}
	brokers := strings.Split(brokersEnv, ",")
	topic := os.Getenv("EVM_TOOLS_KAFKA_TOPIC")
	if topic == "" {
		topic = "evm-tools-livetest"
	}

	pub, err := NewKafkaPublisher(WriterConfig{Brokers: brokers, DialTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("NewKafkaPublisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	in := streamFrom(t, eventEnv("0xlive", 0), sampleEnv("treasury", 100))
	sink, err := New(Options{
		Reader:    record.NewReader(strings.NewReader(in)),
		Publisher: pub,
		Router:    newRouterOrFatal(t, topic, nil, PartitionIdentity),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := sink.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Read the two messages back (from the start) to confirm they landed.
	r, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer r.Close()
	readCtx, readCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer readCancel()
	for got := 0; got < 2; {
		fs := r.PollFetches(readCtx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("read back: %v", errs[0].Err)
		}
		got += fs.NumRecords()
	}
}
