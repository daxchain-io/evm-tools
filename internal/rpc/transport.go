package rpc

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"time"
)

// TLSConfig is the mTLS material for the outbound RPC client. The field names
// mirror the [rpc] config keys and the --rpc-* flags.
type TLSConfig struct {
	ClientCert string // path to the mTLS client certificate (PEM)
	ClientKey  string // path to the mTLS client private key (PEM)
	CACert     string // path to a custom CA bundle (PEM); optional
	ServerName string // optional TLS server name override
}

// ErrMTLSRequired reports that an HTTPS endpoint was configured without the
// client certificate/key mTLS demands. It is returned before any work starts so
// misconfiguration fails fast (Principle 7).
var ErrMTLSRequired = errors.New("rpc: HTTPS endpoint requires mTLS client certificate and key")

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

// newHTTPClient builds an *http.Client for rawURL using cfg. For HTTPS it
// requires and loads the mTLS material, failing fast with a clear, secret-free
// error when files are missing, unreadable, mismatched, or invalid. Plain HTTP
// is allowed for local development and ignores the mTLS fields.
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

	if cfg.ClientCert == "" || cfg.ClientKey == "" {
		return nil, ErrMTLSRequired
	}

	warnIfKeyTooOpen(cfg.ClientKey)

	cert, err := tls.LoadX509KeyPair(cfg.ClientCert, cfg.ClientKey)
	if err != nil {
		// LoadX509KeyPair's error names the file paths and a generic reason; it
		// never includes key contents.
		return nil, fmt.Errorf("rpc: load mTLS client cert/key: %w", err)
	}

	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		ServerName:   cfg.ServerName,
	}

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
	if runtime.GOOS == "windows" {
		return
	}
	info, err := os.Stat(path)
	if err != nil {
		return // load step reports a real read error with the path
	}
	if info.Mode().Perm()&0o077 != 0 {
		keyPermWarner(path, info.Mode().Perm())
	}
}
