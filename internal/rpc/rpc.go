// Package rpc provides the shared HTTPS JSON-RPC transport and client used by
// every tool in the suite: normal runs, balance polling, event backfills, and
// one-shot health checks.
//
// HTTPS uses ordinary server-authenticated TLS, so public providers work with no
// extra material; configuring a client cert/key upgrades the connection to mutual
// TLS for private endpoints, and RequireMTLS makes a missing client cert fail
// fast at construction (see [ErrMTLSRequired]). Plain HTTP is allowed for local
// development. Every diagnostic that names the endpoint runs it through
// [RedactURL] so a token in rpc.url never reaches logs, errors, or metrics. RPC
// failures are reduced to the coarse [ErrorType] categories so raw provider error
// text is never used as a metric label.
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// ErrNotImplemented is retained for callers that referenced the scaffold; the
// real client below supersedes it.
var ErrNotImplemented = errors.New("rpc: not implemented")

// defaultTimeout bounds a single RPC call. It is generous enough for a chunked
// eth_getLogs query while still surfacing a wedged endpoint.
const defaultTimeout = 30 * time.Second

// ErrorType is the coarse, low-cardinality error category used for the
// error_type metric label. Raw RPC/transport error text is never used directly.
type ErrorType string

// Error categories. These are the only error_type label values emitted.
const (
	ErrorTimeout    ErrorType = "timeout"
	ErrorConnection ErrorType = "connection_error"
	ErrorRPC        ErrorType = "rpc_error"
	ErrorDecode     ErrorType = "decode_error"
	ErrorUnknown    ErrorType = "unknown"
	ErrorNone       ErrorType = "" // sentinel: no error
)

// CallObserver is notified after every RPC round trip with the operation name,
// its duration, and the coarse error category (ErrorNone on success). The
// metrics package wires this to its RPC histograms/counters; a nil observer is
// ignored. The observer must not retain or log secret-bearing values.
type CallObserver func(operation string, dur time.Duration, et ErrorType)

// Client is a minimal Ethereum JSON-RPC client over the shared TLS transport.
// It is safe for concurrent use.
type Client struct {
	url    string // raw URL (carries any token); never logged directly
	http   *http.Client
	nextID func() uint64
	idCh   chan uint64
	obs    CallObserver
}

// Options configures a Client.
type Options struct {
	URL      string
	TLS      TLSConfig
	Timeout  time.Duration // per-call timeout; defaults to defaultTimeout
	Observer CallObserver  // optional metrics hook
}

// New builds a Client, validating the TLS material for HTTPS URLs fail-fast
// (loading any client cert/key, honoring RequireMTLS).
func New(opts Options) (*Client, error) {
	if opts.URL == "" {
		return nil, errors.New("rpc: url is required")
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}
	hc, err := newHTTPClient(opts.URL, opts.TLS, timeout)
	if err != nil {
		return nil, err
	}
	idCh := make(chan uint64, 1)
	idCh <- 1
	c := &Client{
		url:  opts.URL,
		http: hc,
		idCh: idCh,
		obs:  opts.Observer,
	}
	c.nextID = func() uint64 {
		id := <-c.idCh
		c.idCh <- id + 1
		return id
	}
	return c, nil
}

// RedactedURL returns the log-safe form of the configured endpoint.
func (c *Client) RedactedURL() string { return RedactURL(c.url) }

// rpcRequest is the JSON-RPC 2.0 request envelope.
type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      uint64 `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

// rpcError is the JSON-RPC 2.0 error object.
type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string {
	return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message)
}

// rpcResponse is the JSON-RPC 2.0 response envelope.
type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      uint64          `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error"`
}

// call performs a single JSON-RPC method call and unmarshals Result into out.
// It observes the call's duration and error category through the observer. The
// returned error is always classified via Classify by the caller for metrics.
func (c *Client) call(ctx context.Context, method string, out any, params ...any) (err error) {
	start := time.Now()
	defer func() {
		if c.obs != nil {
			c.obs(method, time.Since(start), Classify(err))
		}
	}()

	if params == nil {
		params = []any{}
	}
	reqBody := rpcRequest{JSONRPC: "2.0", ID: c.nextID(), Method: method, Params: params}
	buf, mErr := json.Marshal(reqBody)
	if mErr != nil {
		return fmt.Errorf("rpc: marshal %s request: %w", method, mErr)
	}

	httpReq, rErr := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(buf))
	if rErr != nil {
		return fmt.Errorf("rpc: build %s request: %w", method, rErr)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	resp, dErr := c.http.Do(httpReq)
	if dErr != nil {
		// Strip the URL (which carries the token) from transport errors.
		return fmt.Errorf("rpc: %s transport error: %w", method, sanitizeTransportErr(dErr, c.url))
	}
	defer func() { _ = resp.Body.Close() }()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if readErr != nil {
		return fmt.Errorf("rpc: %s read body: %w", method, readErr)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("rpc: %s http status %d", method, resp.StatusCode)
	}

	var rpcResp rpcResponse
	if uErr := json.Unmarshal(body, &rpcResp); uErr != nil {
		return &decodeError{op: method, err: uErr}
	}
	if rpcResp.Error != nil {
		return rpcResp.Error
	}
	if out != nil {
		if uErr := json.Unmarshal(rpcResp.Result, out); uErr != nil {
			return &decodeError{op: method, err: uErr}
		}
	}
	return nil
}

// decodeError wraps a JSON decoding failure so Classify can categorize it.
type decodeError struct {
	op  string
	err error
}

func (e *decodeError) Error() string {
	return fmt.Sprintf("rpc: %s decode response: %v", e.op, e.err)
}
func (e *decodeError) Unwrap() error { return e.err }

// redactedError wraps a transport error whose Error() string has had the raw
// URL stripped, while preserving the original error chain via Unwrap so Classify
// (and any other errors.As/errors.Is caller) can still match the wrapped
// *net.OpError / net.Error and categorize it. Rebuilding the error with
// errors.New would discard that chain and force every transport failure to
// classify as ErrorUnknown.
type redactedError struct {
	msg   string // redacted message, safe for logs
	cause error  // original error, kept classifiable; never surfaced verbatim
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.cause }

// sanitizeTransportErr replaces any occurrence of the raw URL in a transport
// error with its redacted form, so a token embedded in the URL never reaches an
// error string surfaced to logs. The returned error preserves the original
// error chain so Classify can still categorize it (e.g. as connection_error).
func sanitizeTransportErr(err error, rawURL string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if rawURL != "" && strings.Contains(msg, rawURL) {
		return &redactedError{
			msg:   strings.ReplaceAll(msg, rawURL, RedactURL(rawURL)),
			cause: err,
		}
	}
	return err
}

// Classify reduces an error to its coarse ErrorType category for metrics. It
// inspects wrapped errors (context deadline, net errors, JSON-RPC errors,
// decode errors) without surfacing raw provider text.
func Classify(err error) ErrorType {
	if err == nil {
		return ErrorNone
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrorTimeout
	}
	var de *decodeError
	if errors.As(err, &de) {
		return ErrorDecode
	}
	var re *rpcError
	if errors.As(err, &re) {
		return ErrorRPC
	}
	var ne net.Error
	if errors.As(err, &ne) {
		if ne.Timeout() {
			return ErrorTimeout
		}
		return ErrorConnection
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return ErrorConnection
	}
	return ErrorUnknown
}
