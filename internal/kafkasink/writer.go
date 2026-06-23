package kafkasink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/daxchain-io/evm-tools/internal/keyperm"
)

// WriterConfig is the resolved configuration for the real franz-go publisher.
// Secrets (SASL password) arrive already resolved through the config layer's
// env-interpolation/_cmd machinery; this package never reads them from the file
// itself and never logs them.
type WriterConfig struct {
	Brokers      []string
	BatchTimeout time.Duration

	// Idempotent selects the producer mode. false (the default) is plain
	// at-least-once: the broker may receive a duplicate on a producer retry, which
	// a consumer dedups on the record's identity key (the suite's standard posture,
	// matching a non-FIFO AWS queue / Redis with dedup off). true enables the
	// idempotent producer (KIP-98), which suppresses the producer's OWN in-session
	// retry duplicates — but it is session-scoped (a restart re-publishes), so it is
	// NOT cross-run exactly-once; it requires acks=all (kept in both modes).
	Idempotent bool

	// SASL (optional). Mechanism is "", "plain", "scram-sha-256", or
	// "scram-sha-512". SASL must run over TLS.
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string

	// TLS (optional but required when SASL is set).
	TLSEnabled            bool
	TLSCACert             string
	TLSClientCert         string
	TLSClientKey          string
	TLSServerName         string
	TLSInsecureSkipVerify bool

	// DialTimeout bounds connection establishment (TLS handshake + SASL). Zero
	// uses the franz-go default.
	DialTimeout time.Duration

	// Topics the sink may write to (the default topic plus per-type overrides).
	// The readiness probe requests metadata for exactly these, so it verifies the
	// brokers and that the sink's own topics are reachable/authorized; an empty
	// set asks for all-cluster metadata.
	Topics []string
}

// kafkaWriter is the real Publisher backed by a franz-go *kgo.Client with
// RequiredAcks=all. ProduceSync blocks until the broker has acknowledged the
// write, which is what gives the sink its at-least-once confirm-before-advance
// guarantee.
type kafkaWriter struct {
	client *kgo.Client
	topics []string
}

// NewKafkaPublisher builds the real franz-go-backed Publisher. It validates and
// builds the TLS config and SASL mechanism up front (fail fast on bad material)
// but performs no network I/O — connections are established lazily on the first
// produce, so construction stays offline-safe for `validate`.
func NewKafkaPublisher(cfg WriterConfig) (Publisher, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafkasink: at least one broker is required")
	}

	mech, err := saslMechanism(cfg)
	if err != nil {
		return nil, err
	}
	tlsCfg, err := tlsConfig(cfg)
	if err != nil {
		return nil, err
	}
	if mech != nil && tlsCfg == nil {
		// SASL credentials in cleartext over a plain connection would leak the
		// password; require TLS (the design mandates SASL over TLS).
		return nil, errors.New("kafkasink: SASL requires TLS; set [kafka.tls].enabled = true (with CA/cert material)")
	}

	batchTimeout := cfg.BatchTimeout
	if batchTimeout <= 0 {
		batchTimeout = 200 * time.Millisecond
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		// acks=all in BOTH modes: at-least-once needs it for durability, and the
		// idempotent producer mandates it.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Key -> partition (murmur2, Kafka-default-compatible) so per-key ordering
		// holds; an empty key round-robins.
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.ProducerLinger(batchTimeout),
		// franz-go never auto-creates topics on produce unless asked; we don't ask.
	}
	if cfg.DialTimeout > 0 {
		opts = append(opts, kgo.DialTimeout(cfg.DialTimeout))
	}
	if tlsCfg != nil {
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}
	if mech != nil {
		opts = append(opts, kgo.SASL(mech))
	}
	if !cfg.Idempotent {
		// Plain at-least-once: disable the idempotent producer. franz-go then
		// requires an explicit in-flight bound; pin it to 1 so a retry can never
		// reorder records within a partition (preserving per-key ordering).
		opts = append(opts,
			kgo.DisableIdempotentWrite(),
			kgo.MaxProduceRequestsInflightPerBroker(1),
		)
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafkasink: build client: %w", err)
	}
	return &kafkaWriter{client: client, topics: cfg.Topics}, nil
}

// Reachable issues a metadata request to confirm the broker cluster answers
// (TCP + TLS + SASL handshake + a metadata response) AND that the sink's own
// topics exist and are authorized. It is read-only and used by the sink's active
// readiness probe; a nil error means the cluster answered and every configured
// topic resolved without error. Per-topic errors (unknown topic with auto-create
// off, or a topic ACL failure) are decoded from the response — a missing/forbidden
// topic means the sink cannot publish, so /readyz should reflect that. An empty
// topic set asks for all-cluster metadata (only the cluster handshake is checked).
func (k *kafkaWriter) Reachable(ctx context.Context) error {
	req := kmsg.NewPtrMetadataRequest()
	for i := range k.topics {
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = &k.topics[i]
		req.Topics = append(req.Topics, rt)
	}
	resp, err := k.client.Request(ctx, req)
	if err != nil {
		return err
	}
	meta, ok := resp.(*kmsg.MetadataResponse)
	if !ok {
		return nil // transport answered with an unexpected shape; treat as reachable
	}
	for _, topic := range meta.Topics {
		if topic.ErrorCode == 0 {
			continue
		}
		name := ""
		if topic.Topic != nil {
			name = *topic.Topic
		}
		return fmt.Errorf("kafkasink: topic %q not usable: %w", name, kerr.ErrorForCode(topic.ErrorCode))
	}
	return nil
}

// Publish produces one record and blocks until the broker acknowledges it (or the
// produce fails / ctx is cancelled). A broker rejection that retrying cannot fix
// is wrapped in *PermanentError so the sink fails fast.
func (k *kafkaWriter) Publish(ctx context.Context, msg Message) error {
	rec := &kgo.Record{Topic: msg.Topic, Key: msg.Key, Value: msg.Value}
	if err := k.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
		if permanent(err) {
			return &PermanentError{Reason: "broker rejected message", Err: err}
		}
		return err
	}
	return nil
}

// Close flushes any buffered records then closes the underlying client. The sink
// loop is synchronous (ProduceSync confirms each record before advancing), so
// nothing is normally buffered at Close — but kgo.Close itself ABORTS in-flight
// produces rather than flushing, so a bounded Flush first is defense-in-depth
// against any future buffered-produce path. The flush is time-bounded so a dead
// broker cannot hang shutdown.
func (k *kafkaWriter) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = k.client.Flush(ctx)
	k.client.Close()
	return nil
}

// permanent reports whether a produce error is non-retryable. franz-go surfaces
// broker errors as *kerr.Error; a handful of those (message too large, unknown
// topic/partition with auto-create off, authorization failures, invalid record)
// will never succeed on retry, so they fail the sink fast rather than loop forever
// (preserving losslessness — a stuck retry on a permanent error would silently
// wedge the pipeline). Compared by error code so it is robust to error wrapping.
func permanent(err error) bool {
	var ke *kerr.Error
	if !errors.As(err, &ke) {
		return false
	}
	switch ke.Code {
	case kerr.MessageTooLarge.Code,
		kerr.RecordListTooLarge.Code,
		kerr.UnknownTopicOrPartition.Code,
		kerr.TopicAuthorizationFailed.Code,
		kerr.ClusterAuthorizationFailed.Code,
		kerr.SaslAuthenticationFailed.Code,
		kerr.InvalidTopicException.Code,
		kerr.InvalidRecord.Code,
		kerr.UnsupportedForMessageFormat.Code,
		kerr.UnsupportedVersion.Code:
		return true
	default:
		return false
	}
}

// saslMechanism builds the SASL mechanism from config, or nil when SASL is
// disabled. The password is used only to construct the mechanism and is never
// logged.
func saslMechanism(cfg WriterConfig) (sasl.Mechanism, error) {
	mech := strings.ToLower(strings.TrimSpace(cfg.SASLMechanism))
	if mech == "" {
		return nil, nil
	}
	if cfg.SASLUsername == "" {
		return nil, errors.New("kafkasink: SASL username is required when a mechanism is set")
	}
	auth := scram.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}
	switch mech {
	case "plain":
		return plain.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsMechanism(), nil
	case "scram-sha-256":
		return auth.AsSha256Mechanism(), nil
	case "scram-sha-512":
		return auth.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("kafkasink: unsupported SASL mechanism %q (want plain|scram-sha-256|scram-sha-512)", cfg.SASLMechanism)
	}
}

// tlsConfig builds the *tls.Config from config, or nil when TLS is disabled. It
// loads the CA bundle and optional client keypair from disk, failing fast (with
// the path and a generic reason, never file contents) on bad material.
func tlsConfig(cfg WriterConfig) (*tls.Config, error) {
	if !cfg.TLSEnabled {
		return nil, nil
	}
	tc := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         cfg.TLSServerName,
		InsecureSkipVerify: cfg.TLSInsecureSkipVerify, //nolint:gosec // opt-in, documented dev-only escape hatch
	}
	if cfg.TLSCACert != "" {
		pem, err := os.ReadFile(cfg.TLSCACert)
		if err != nil {
			return nil, fmt.Errorf("kafkasink: read ca_cert %q: %w", cfg.TLSCACert, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("kafkasink: ca_cert %q contains no valid certificates", cfg.TLSCACert)
		}
		tc.RootCAs = pool
	}
	if (cfg.TLSClientCert == "") != (cfg.TLSClientKey == "") {
		return nil, errors.New("kafkasink: set both tls.client_cert and tls.client_key, or neither")
	}
	if cfg.TLSClientCert != "" {
		// Warn (don't fail) when the private key is group/world-readable, matching
		// the RPC mTLS client-key check — secret-handling parity across the suite.
		keyperm.WarnIfTooOpen(cfg.TLSClientKey, func(path string, mode os.FileMode) {
			slog.Warn("kafka tls client_key is group/world-readable; tighten its mode",
				"path", path, "mode", fmt.Sprintf("%#o", mode))
		})
		pair, err := tls.LoadX509KeyPair(cfg.TLSClientCert, cfg.TLSClientKey)
		if err != nil {
			return nil, fmt.Errorf("kafkasink: load client keypair (%q/%q): invalid certificate or key", cfg.TLSClientCert, cfg.TLSClientKey)
		}
		tc.Certificates = []tls.Certificate{pair}
	}
	return tc, nil
}
