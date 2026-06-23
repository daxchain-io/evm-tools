//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	redis "github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/daxchain-io/evm-tools/internal/kafkasink"
	"github.com/daxchain-io/evm-tools/internal/pgsink"
	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/redissink"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// uniqueName returns a per-test destination name. time.Now is fine here (a real
// Go test, not a workflow script).
func uniqueName(prefix string) string {
	return fmt.Sprintf("%s%d", prefix, time.Now().UnixNano())
}

// sampleRecord builds one valid event envelope and its canonical JSONL bytes
// (as a sink would receive them: encoded by record.Writer, decoded by record.Reader).
func sampleRecord(t *testing.T, txHash string) (record.Envelope, []byte) {
	t.Helper()
	li := uint64(0)
	env := record.Envelope{
		Type: record.TypeEvent, Tool: record.ToolStream, Name: "usdc",
		Chain: "itest", ChainID: 4242, BlockNumber: 100, TxHash: txHash, LogIndex: &li,
		Data: record.EventData{
			Event: "Transfer", Signature: "Transfer(address,address,uint256)",
			Contract: "0xc", Params: map[string]string{"value": "1"},
		},
	}
	var sb strings.Builder
	if err := record.NewWriter(&sb).Emit(env); err != nil {
		t.Fatalf("emit sample: %v", err)
	}
	got, err := record.NewReader(strings.NewReader(sb.String())).Next()
	if err != nil {
		t.Fatalf("decode sample: %v", err)
	}
	return got, []byte(strings.TrimRight(sb.String(), "\n"))
}

// TestRedisSinkLive appends a record to a real Redis Stream and verifies it landed
// once (dedup-gated XADD is idempotent on the dedup key).
func TestRedisSinkLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	url := envOr("EVM_TEST_REDIS_URL", "redis://localhost:6379/0")
	stream := uniqueName("evmtest:")

	// A unique dedup prefix isolates the per-record markers per run (they are keyed
	// by record identity and outlive the unique stream otherwise).
	app, err := redissink.NewAppender(redissink.ClientConfig{
		URL: url, Stream: stream, Dedup: true, DedupTTL: time.Hour, DedupPrefix: uniqueName("evmtest:dedup:"),
	})
	if err != nil {
		t.Fatalf("NewAppender: %v", err)
	}
	defer func() { _ = app.Close() }()

	rec, raw := sampleRecord(t, "0xredis")
	if ok, err := app.Append(ctx, rec, raw); err != nil || !ok {
		t.Fatalf("Append: ok=%v err=%v (want ok=true)", ok, err)
	}
	if ok, err := app.Append(ctx, rec, raw); err != nil || ok {
		t.Fatalf("dedup Append: ok=%v err=%v (want ok=false, nil)", ok, err)
	}

	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	rc := redis.NewClient(opt)
	defer func() { _ = rc.Close() }()
	defer func() { _ = rc.Del(context.Background(), stream).Err() }()
	msgs, err := rc.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("stream length = %d, want 1 (idempotent)", len(msgs))
	}
}

// TestPostgresSinkLive inserts a record into a real Postgres table and verifies
// the insert is idempotent (ON CONFLICT (dedup_key) DO NOTHING).
func TestPostgresSinkLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dsn := envOr("EVM_TEST_PG_DSN", "postgres://evm:evm@localhost:5432/evm?sslmode=disable")
	table := uniqueName("evmtest_")

	ins, err := pgsink.NewInserter(ctx, dsn, table, true)
	if err != nil {
		t.Fatalf("NewInserter: %v", err)
	}
	defer func() { _ = ins.Close() }()

	rec, raw := sampleRecord(t, "0xpg")
	if err := ins.Insert(ctx, rec, raw); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := ins.Insert(ctx, rec, raw); err != nil {
		t.Fatalf("Insert#2 (idempotent): %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	defer func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table) }()
	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("row count = %d, want 1 (idempotent ON CONFLICT)", n)
	}
}

// TestKafkaSinkLive publishes a record to a real broker and consumes it back, in
// both delivery modes — proving the idempotent producer (delivery_mode=idempotent)
// negotiates with the broker and that both modes deliver the verbatim payload.
func TestKafkaSinkLive(t *testing.T) {
	for _, idempotent := range []bool{false, true} {
		name := "at-least-once"
		if idempotent {
			name = "idempotent"
		}
		t.Run(name, func(t *testing.T) { runKafkaSinkLive(t, idempotent) })
	}
}

func runKafkaSinkLive(t *testing.T, idempotent bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	brokers := strings.Split(envOr("EVM_TEST_KAFKA_BROKERS", "localhost:9092"), ",")
	topic := uniqueName("evmtest-")

	// Create the topic explicitly so the test does not depend on broker
	// auto-creation.
	admClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	adm := kadm.NewClient(admClient)
	resp, err := adm.CreateTopics(ctx, 1, 1, nil, topic)
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if terr := resp[topic].Err; terr != nil {
		t.Fatalf("create topic %s: %v", topic, terr)
	}
	admClient.Close()

	pub, err := kafkasink.NewKafkaPublisher(kafkasink.WriterConfig{Brokers: brokers, Idempotent: idempotent})
	if err != nil {
		t.Fatalf("NewKafkaPublisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	// The readiness probe must confirm the created topic is usable.
	if err := pub.Reachable(ctx); err != nil {
		t.Fatalf("Reachable: %v", err)
	}

	_, raw := sampleRecord(t, "0xkafka")
	if err := pub.Publish(ctx, kafkasink.Message{Topic: topic, Key: []byte("k"), Value: raw}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	r, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer r.Close()
	rctx, rcancel := context.WithTimeout(ctx, 25*time.Second)
	defer rcancel()
	fs := r.PollFetches(rctx)
	if errs := fs.Errors(); len(errs) > 0 {
		t.Fatalf("poll fetches: %v", errs[0].Err)
	}
	recs := fs.Records()
	if len(recs) == 0 {
		t.Fatal("no record consumed back")
	}
	if string(recs[0].Value) != string(raw) {
		t.Fatalf("kafka value mismatch:\n got %s\nwant %s", recs[0].Value, raw)
	}
}
