package rpc

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/daxchain-io/evm-tools/internal/keyperm"
)

// TLSConfig is the TLS material for the outbound RPC client. The field names
// mirror the [rpc] config keys and the --rpc-* flags. Client cert/key are only
// needed for endpoints that require mutual TLS (private nodes); public providers
// (Alchemy, Infura, …) use ordinary server-authenticated TLS with none set.
type TLSConfig struct {
	ClientCert string // path to the mTLS client certificate (PEM); optional
	ClientKey  string // path to the mTLS client private key (PEM); optional
	CACert     string // path to a custom CA bundle (PEM); optional
	ServerName string // optional TLS server name override
	// RequireMTLS demands a client certificate/key for HTTPS and fails fast when
	// none is configured. It is off by default so public HTTPS endpoints work
	// out of the box; operators set it for private endpoints that must present a
	// client certificate, restoring the strict fail-fast posture.
	RequireMTLS bool
}

// ErrMTLSRequired reports that RequireMTLS is set for an HTTPS endpoint but no
// client certificate/key was configured. It is returned before any work starts
// so the misconfiguration fails fast (Principle 7).
var ErrMTLSRequired = errors.New("rpc: require_mtls is set but no client certificate/key configured")

// keyPermWarner is the hook used to warn about an over-permissive client_key
// file mode. It is a package var so tests can capture the warning without a
// real logger; production wires it to slog.Warn.
var keyPermWarner = func(_ string, _ os.FileMode) {}

// SetKeyPermWarner installs the callback invoked when client_key is group- or
// world-readable on a non-Windows host. The run command wires this to slog so
// the warning reaches stderr (never the key contents — only path and mode).
func SetKeyPermWarner(fn func(path string, mode os.FileMode)) {
	if fn != nil {
		keyPermWarner = fn
	}
}

// IsHTTPS reports whether the URL uses the https scheme, which is what triggers
// the mTLS requirement. A parse failure is treated as not-HTTPS here; the URL is
// validated separately when the client is built.
func IsHTTPS(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	return u.Scheme == "https"
}

// newHTTPClient builds an *http.Client for rawURL using cfg. HTTPS uses ordinary
// server-authenticated TLS by default; when a client cert/key pair is configured
// it upgrades to mutual TLS, and when RequireMTLS is set it fails fast unless that
// pair is present. A custom CA bundle and server-name override apply in either
// mode. Cert/key load errors are reported clearly and secret-free. Plain HTTP is
// allowed for local development and ignores the TLS fields.
func newHTTPClient(rawURL string, cfg TLSConfig, timeout time.Duration) (*http.Client, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		// url.Parse's *url.Error renders the offending URL with strconv.Quote,
		// which escapes control characters — so the raw URL is not present
		// verbatim and sanitizeTransportErr's substring replace cannot reliably
		// catch a token sitting next to such a character. Wrapping the error
		// with %w would therefore leak the token (the canonical rpc.url carries
		// "?token=${RPC_TOKEN}"). Drop the raw parse error entirely and report a
		// generic, secret-free reason; only the already-redacted URL is shown.
		return nil, fmt.Errorf("rpc: invalid url %q: could not parse RPC URL", RedactURL(rawURL))
	}
	switch u.Scheme {
	case "http":
		return &http.Client{Timeout: timeout}, nil
	case "https":
		// proceed below
	default:
		return nil, fmt.Errorf("rpc: unsupported url scheme %q (want http or https)", u.Scheme)
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: cfg.ServerName,
	}

	// A custom CA bundle pins server verification (private CAs); it applies in
	// both server-auth and mutual-TLS modes. Empty uses the system roots.
	if cfg.CACert != "" {
		pem, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("rpc: read ca bundle %q: %w", cfg.CACert, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("rpc: ca bundle %q contains no usable PEM certificates", cfg.CACert)
		}
		tlsCfg.RootCAs = pool
	}

	hasCert, hasKey := cfg.ClientCert != "", cfg.ClientKey != ""
	switch {
	case hasCert && hasKey:
		// Mutual TLS: present the client certificate. Used by private endpoints
		// that require client authentication.
		warnIfKeyTooOpen(cfg.ClientKey)
		cert, err := tls.LoadX509KeyPair(cfg.ClientCert, cfg.ClientKey)
		if err != nil {
			// LoadX509KeyPair's error names the file paths and a generic reason;
			// it never includes key contents.
			return nil, fmt.Errorf("rpc: load mTLS client cert/key: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	case hasCert != hasKey:
		// Only one half of the pair was supplied — a misconfiguration. Name the
		// missing side without echoing any path contents.
		missing := "client_key"
		if hasKey {
			missing = "client_cert"
		}
		return nil, fmt.Errorf("rpc: mTLS needs both client_cert and client_key (%s is missing)", missing)
	default:
		// Neither supplied: ordinary server-authenticated TLS, correct for public
		// providers. Fail fast only when the operator explicitly required mTLS.
		if cfg.RequireMTLS {
			return nil, ErrMTLSRequired
		}
	}

	transport := &http.Transport{
		TLSClientConfig:     tlsCfg,
		MaxIdleConns:        16,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     90 * time.Second,
	}
	return &http.Client{Transport: transport, Timeout: timeout}, nil
}

// warnIfKeyTooOpen warns (via the installed warner) when the client_key file is
// group- or world-readable on a non-Windows host. The file contents are never
// read or logged — only the path and octal mode.
func warnIfKeyTooOpen(path string) {
	keyperm.WarnIfTooOpen(path, keyPermWarner)
}
