package webhooksink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Poster is the HTTP-delivery surface the sink loop depends on. The real
// implementation POSTs to a configured URL; tests substitute a fake. Post must
// block until the server has responded (or the call fails), so the loop can
// confirm-before-advance (at-least-once).
//
// A returned error is classified by [Classify] into transient (retry: network,
// timeout, HTTP 5xx) vs. permanent (fail fast: HTTP 4xx). A 4xx means retrying
// will not help, so the sink exits non-zero rather than silently dropping the
// record (preserves losslessness — design Open Question 1, settled).
type Poster interface {
	Post(ctx context.Context, payload []byte) error
	// Reachable performs a read-only check (an HTTP GET of the configured health
	// URL) that the endpoint is reachable, returning nil on a 2xx. The active
	// readiness probe uses it so /readyz reflects endpoint health while no records
	// are flowing. When no health URL is configured the probe is disabled at the
	// CLI layer and this is not called.
	Reachable(ctx context.Context) error
	Close() error
}

// PosterConfig is the resolved configuration for the real HTTP poster. The auth
// header value arrives already resolved through the config layer's
// env-interpolation/_cmd machinery; this package never reads it from the file
// and never logs it.
type PosterConfig struct {
	URL    string
	Method string
	// Headers are static, non-secret request headers added to every request.
	Headers map[string]string
	// AuthHeader / AuthValue carry the optional secret auth header (e.g.
	// "Authorization: Bearer <token>"). AuthValue is a secret and is never logged.
	AuthHeader string
	AuthValue  string
	// Timeout bounds a single request. Zero uses a built-in default.
	Timeout time.Duration
	// HealthURL, when set, is GET-probed by the active readiness probe to confirm
	// the endpoint is reachable while idle. Empty disables the active probe.
	HealthURL string
}

// httpPoster is the real Poster backed by net/http.
type httpPoster struct {
	client    *http.Client
	url       string
	method    string
	headers   map[string]string
	healthURL string
}

// defaultTimeout bounds a single POST when none is configured.
const defaultTimeout = 10 * time.Second

// NewHTTPPoster builds the real net/http-backed Poster. It validates the URL and
// method up front (fail fast on bad material) but performs no network I/O, so
// construction stays offline-safe for `validate`.
func NewHTTPPoster(cfg PosterConfig) (Poster, error) {
	url := strings.TrimSpace(cfg.URL)
	if url == "" {
		return nil, errors.New("webhooksink: a webhook url is required")
	}
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		return nil, fmt.Errorf("webhooksink: webhook url must be http(s), got %q", RedactURL(url))
	}

	method := strings.ToUpper(strings.TrimSpace(cfg.Method))
	if method == "" {
		method = http.MethodPost
	}
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch:
	default:
		return nil, fmt.Errorf("webhooksink: unsupported webhook method %q (want POST|PUT|PATCH)", method)
	}

	// Merge static headers with the optional secret auth header into one map so a
	// per-request build stays cheap. The auth value is held here but never logged.
	headers := make(map[string]string, len(cfg.Headers)+1)
	for k, v := range cfg.Headers {
		if strings.TrimSpace(k) != "" {
			headers[k] = v
		}
	}
	if strings.TrimSpace(cfg.AuthHeader) != "" && cfg.AuthValue != "" {
		headers[cfg.AuthHeader] = cfg.AuthValue
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	healthURL := strings.TrimSpace(cfg.HealthURL)
	if healthURL != "" && !strings.HasPrefix(healthURL, "http://") && !strings.HasPrefix(healthURL, "https://") {
		return nil, fmt.Errorf("webhooksink: webhook health_url must be http(s), got %q", RedactURL(healthURL))
	}

	return &httpPoster{
		client: &http.Client{
			Timeout: timeout,
			// Never follow redirects. Go strips the standard Authorization header
			// on a cross-host redirect but NOT arbitrary custom headers, so a
			// compromised/misconfigured endpoint returning a 3xx to an
			// attacker-controlled host would leak the configured secret auth header
			// (and the record payload). Returning the 3xx response as-is lets Post
			// treat it as a permanent misconfiguration instead of following it.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		url:       url,
		method:    method,
		headers:   headers,
		healthURL: healthURL,
	}, nil
}

// Reachable GET-probes the configured health URL to confirm the endpoint is
// reachable (used by the active readiness probe). A 2xx means reachable. When no
// health URL is configured it returns nil; the probe is disabled at the CLI
// layer, so that path is not reached in practice.
func (p *httpPoster) Reachable(ctx context.Context) error {
	if p.healthURL == "" {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.healthURL, nil)
	if err != nil {
		return err
	}
	for k, v := range p.headers {
		req.Header.Set(k, v)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("health check returned HTTP %d", resp.StatusCode)
}

// Post sends one record payload as application/json and blocks until the server
// responds (or the request fails / ctx is cancelled). A 2xx is success. A 4xx is
// permanent (wrapped in *PermanentError so the sink fails fast). A 5xx, or any
// transport/timeout error, is transient and surfaces for retry.
func (p *httpPoster) Post(ctx context.Context, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, p.method, p.url, bytes.NewReader(payload))
	if err != nil {
		// A bad request construction is not retryable.
		return &PermanentError{Reason: "build request", Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range p.headers {
		req.Header.Set(k, v)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		// Transport error / timeout / canceled: transient (retry until recovery).
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	// Drain the body so the connection can be reused; cap the read so a chatty
	// error body cannot blow up memory.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		// A redirect we deliberately did not follow (see CheckRedirect). Retrying
		// won't help and following would risk leaking the auth header, so fail
		// fast and tell the operator to configure the final destination directly.
		return &PermanentError{Reason: fmt.Sprintf("HTTP %d redirect not followed (point webhook.url at the final destination; redirects are refused to avoid leaking auth headers)", resp.StatusCode)}
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// 4xx: retrying will not help — fail fast (exit non-zero), never drop.
		return &PermanentError{Reason: fmt.Sprintf("HTTP %d (client error)", resp.StatusCode)}
	default:
		// 5xx and anything else non-2xx: transient, retry with backoff.
		return &transientHTTPError{status: resp.StatusCode}
	}
}

// Close is a no-op for the HTTP poster (the *http.Client needs no teardown
// beyond GC); it exists to satisfy the interface and the graceful-shutdown path.
func (p *httpPoster) Close() error { return nil }

// transientHTTPError marks a non-2xx, non-4xx HTTP response (a 5xx) as a
// retryable failure carrying the status code for the error_type/log.
type transientHTTPError struct{ status int }

func (e *transientHTTPError) Error() string {
	return fmt.Sprintf("HTTP %d (server error)", e.status)
}

// RedactURL strips any query string, userinfo, AND path from a URL for safe
// logging, keeping only scheme://host[:port]. The common webhook providers carry
// the delivery secret in the PATH — Slack (/services/T.../B.../<secret>), Discord
// (/api/webhooks/<id>/<token>), Teams, and most generic services — not just the
// query, so a non-root path is replaced with a fixed marker (mirroring
// rpc.RedactURL; see docs/design.md "Secret Handling"). Intentionally simple
// (string-level) so it works on a raw, possibly-unparseable URL. Exported so the
// CLI can log a redacted URL at startup.
func RedactURL(raw string) string {
	if i := strings.IndexByte(raw, '?'); i >= 0 {
		raw = raw[:i]
	}
	scheme := ""
	rest := raw
	if i := strings.Index(rest, "://"); i >= 0 {
		scheme = rest[:i+3]
		rest = rest[i+3:]
	}
	if i := strings.IndexByte(rest, '@'); i >= 0 {
		rest = rest[i+1:]
	}
	host := rest
	path := ""
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		host = rest[:i]
		path = rest[i:]
	}
	if path != "" && path != "/" {
		// The path may itself be the delivery secret; never echo it.
		return scheme + host + "/[redacted]"
	}
	return scheme + host + path
}
