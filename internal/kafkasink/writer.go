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

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/daxchain-io/evm-tools/internal/keyperm"
)

// WriterConfig is the resolved configuration for the real kafka-go publisher.
// Secrets (SASL password) arrive already resolved through the config layer's
// env-interpolation/_cmd machinery; this package never reads them from the file
// itself and never logs them.
type WriterConfig struct {
	Brokers      []string
	BatchTimeout time.Duration

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
	// uses the kafka-go default.
	DialTimeout time.Duration

	// Topics the sink may write to (the default topic plus per-type overrides).
	// The readiness probe requests metadata for exactly these, so it verifies the
	// brokers and that the sink's own topics are reachable/authorized; an empty
	// set asks for all-cluster metadata.
	Topics []string
}

// kafkaWriter is the real Publisher backed by a segmentio/kafka-go *Writer with
// RequiredAcks=all. WriteMessages blocks until the broker has acknowledged the
// write, which is what gives the sink its at-least-once confirm-before-advance
// guarantee.
type kafkaWriter struct {
	w      *kafka.Writer
	topics []string
}

// NewKafkaPublisher builds the real kafka-go-backed Publisher. It validates and
// builds the TLS config and SASL mechanism up front (fail fast on bad material)
// but performs no network I/O — connections are established lazily on the first
// Publish, so construction stays offline-safe for `validate`.
func NewKafkaPublisher(cfg WriterConfig) (Publisher, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafkasink: at least one broker is required")
	}

	transport := &kafka.Transport{}
	if cfg.DialTimeout > 0 {
		transport.DialTimeout = cfg.DialTimeout
	}

	mech, err := saslMechanism(cfg)
	if err != nil {
		return nil, err
	}
	if mech != nil {
		transport.SASL = mech
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
	if tlsCfg != nil {
		transport.TLS = tlsCfg
	}

	batchTimeout := cfg.BatchTimeout
	if batchTimeout <= 0 {
		batchTimeout = 200 * time.Millisecond
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Balancer:     &kafka.Hash{}, // key -> partition, so per-key ordering holds.
		RequiredAcks: kafka.RequireAll,
		// Synchronous: WriteMessages must block until acked so the sink can
		// confirm before advancing the stdin cursor (at-least-once).
		Async:        false,
		BatchTimeout: batchTimeout,
		Transport:    transport,
		// Topic is set per-message (Message.Topic) so one writer fans out across
		// the configured topic routing.
		AllowAutoTopicCreation: false,
	}
	return &kafkaWriter{w: w, topics: cfg.Topics}, nil
}

// Reachable issues a metadata request to confirm the broker cluster answers
// (TCP + TLS + SASL handshake + a metadata response). It is read-only and used
// by the sink's active readiness probe; a nil error means the cluster is
// reachable. Metadata is scoped to the configured topics so an ACL-restricted
// cluster still answers; an empty topic set asks for all-cluster metadata.
func (k *kafkaWriter) Reachable(ctx context.Context) error {
	client := &kafka.Client{Addr: k.w.Addr, Transport: k.w.Transport}
	_, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: k.topics})
	return err
}

// Publish writes one message and blocks until the broker acknowledges it (or the
// write fails / ctx is cancelled). A broker rejection that retrying cannot fix
// is wrapped in *PermanentError so the sink fails fast.
func (k *kafkaWriter) Publish(ctx context.Context, msg Message) error {
	err := k.w.WriteMessages(ctx, kafka.Message{
		Topic: msg.Topic,
		Key:   msg.Key,
		Value: msg.Value,
	})
	if err == nil {
		return nil
	}
	if permanent(err) {
		return &PermanentError{Reason: "broker rejected message", Err: err}
	}
	return err
}

// Close flushes and closes the underlying writer (used on shutdown).
func (k *kafkaWriter) Close() error { return k.w.Close() }

// permanent reports whether a kafka-go write error is non-retryable. kafka-go
// surfaces broker errors as kafka.Error; a handful of those (message too large,
// unknown topic/partition with auto-create off, authorization failures, invalid
// record) will never succeed on retry, so they fail the sink fast rather than
// loop forever (preserving losslessness — a stuck retry on a permanent error
// would silently wedge the pipeline).
func permanent(err error) bool {
	var kerr kafka.Error
	if !errors.As(err, &kerr) {
		return false
	}
	switch kerr {
	case kafka.MessageSizeTooLarge,
		kafka.RecordListTooLarge,
		kafka.UnknownTopicOrPartition,
		kafka.TopicAuthorizationFailed,
		kafka.ClusterAuthorizationFailed,
		kafka.SASLAuthenticationFailed,
		kafka.InvalidTopic,
		kafka.InvalidRecord,
		kafka.UnsupportedForMessageFormat,
		kafka.UnsupportedVersion:
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
	switch mech {
	case "plain":
		return plain.Mechanism{Username: cfg.SASLUsername, Password: cfg.SASLPassword}, nil
	case "scram-sha-256":
		return scram.Mechanism(scram.SHA256, cfg.SASLUsername, cfg.SASLPassword)
	case "scram-sha-512":
		return scram.Mechanism(scram.SHA512, cfg.SASLUsername, cfg.SASLPassword)
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
