package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// traceServer is a plain-HTTP JSON-RPC server that records the last request and
// replies with a fixed body, for asserting the debug_traceBlockByNumber request
// shape and decode without mTLS.
func traceServer(t *testing.T, respBody string, captured *rpcRequest) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, captured)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":1,"result":`+respBody+`}`)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestTraceBlockByNumber(t *testing.T) {
	// One tx whose top-level call makes a value-bearing sub-call.
	resp := `[
      {"txHash":"0xaa","result":{
        "type":"CALL","from":"0xf","to":"0xc","value":"0x0",
        "calls":[
          {"type":"CALL","from":"0xc","to":"0xbene","value":"0xde0b6b3a7640000"},
          {"type":"STATICCALL","from":"0xc","to":"0xview","value":"0x0"}
        ]}}
    ]`
	var got rpcRequest
	srv := traceServer(t, resp, &got)

	c, err := New(Options{URL: srv.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	traces, err := c.TraceBlockByNumber(context.Background(), 256)
	if err != nil {
		t.Fatalf("TraceBlockByNumber: %v", err)
	}

	// Request shape: method + [blockTag, {tracer:callTracer,...}].
	if got.Method != OpDebugTraceBlock {
		t.Errorf("method = %q, want %q", got.Method, OpDebugTraceBlock)
	}
	if len(got.Params) != 2 {
		t.Fatalf("params len = %d, want 2", len(got.Params))
	}
	if tag, _ := got.Params[0].(string); tag != "0x100" {
		t.Errorf("block tag param = %v, want 0x100", got.Params[0])
	}
	cfg, ok := got.Params[1].(map[string]any)
	if !ok || cfg["tracer"] != "callTracer" {
		t.Errorf("tracer param = %v, want callTracer", got.Params[1])
	}

	// Decode shape.
	if len(traces) != 1 || traces[0].TxHash != "0xaa" || traces[0].Result == nil {
		t.Fatalf("decoded traces = %+v", traces)
	}
	root := traces[0].Result
	if len(root.Calls) != 2 {
		t.Fatalf("root.Calls len = %d, want 2", len(root.Calls))
	}
	v, err := root.Calls[0].ValueBig()
	if err != nil {
		t.Fatalf("ValueBig: %v", err)
	}
	if v.String() != "1000000000000000000" { // 1 ETH
		t.Errorf("child[0] value = %s, want 1e18", v.String())
	}
	if root.Calls[1].Type != "STATICCALL" {
		t.Errorf("child[1] type = %q, want STATICCALL", root.Calls[1].Type)
	}
}

// TestTraceBlockByNumberUnsupported confirms a node that lacks the debug_
// namespace yields a classified rpc error carrying the method-not-found signal,
// which the stream uses to self-disable internal transfers.
func TestTraceBlockByNumberUnsupported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method debug_traceBlockByNumber does not exist/is not available"}}`)
	}))
	t.Cleanup(srv.Close)

	c, err := New(Options{URL: srv.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = c.TraceBlockByNumber(context.Background(), 1)
	if err == nil {
		t.Fatal("expected an error from an unsupported trace method")
	}
	if Classify(err) != ErrorRPC {
		t.Errorf("Classify = %q, want rpc_error", Classify(err))
	}
	if !strings.Contains(err.Error(), "-32601") {
		t.Errorf("error should carry the -32601 code: %v", err)
	}
}

func TestTraceTransaction(t *testing.T) {
	resp := `{"type":"CALL","from":"0xa","to":"0xb","value":"0x0","calls":[{"type":"CALL","from":"0xb","to":"0xc","value":"0x1"}]}`
	var got rpcRequest
	srv := traceServer(t, resp, &got)
	c, err := New(Options{URL: srv.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	root, err := c.TraceTransaction(context.Background(), "0xtx")
	if err != nil {
		t.Fatalf("TraceTransaction: %v", err)
	}
	if got.Method != OpDebugTraceTx {
		t.Errorf("method = %q, want %q", got.Method, OpDebugTraceTx)
	}
	if root == nil || len(root.Calls) != 1 || root.Calls[0].To != "0xc" {
		t.Fatalf("decoded root = %+v", root)
	}
}

func TestTraceBlockParity(t *testing.T) {
	resp := `[
      {"type":"call","traceAddress":[],"transactionHash":"0xtx","action":{"callType":"call","from":"0xa","to":"0xb","value":"0x0"}},
      {"type":"call","traceAddress":[0],"transactionHash":"0xtx","action":{"callType":"call","from":"0xb","to":"0xc","value":"0x1"}},
      {"type":"suicide","traceAddress":[1],"transactionHash":"0xtx","action":{"address":"0xd","refundAddress":"0xe","balance":"0x2"}}
    ]`
	var got rpcRequest
	srv := traceServer(t, resp, &got)
	c, err := New(Options{URL: srv.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	traces, err := c.TraceBlockParity(context.Background(), 5)
	if err != nil {
		t.Fatalf("TraceBlockParity: %v", err)
	}
	if got.Method != OpTraceBlock {
		t.Errorf("method = %q, want %q", got.Method, OpTraceBlock)
	}
	if len(traces) != 3 {
		t.Fatalf("decoded %d parity traces, want 3", len(traces))
	}
	if traces[1].Type != "call" || len(traces[1].TraceAddress) != 1 || traces[1].Action.To != "0xc" {
		t.Errorf("trace[1] = %+v", traces[1])
	}
	if traces[2].Type != "suicide" || traces[2].Action.RefundAddress != "0xe" || traces[2].Action.Balance != "0x2" {
		t.Errorf("trace[2] (suicide) = %+v", traces[2])
	}
}

func TestIsMethodUnsupported(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"http 404", &HTTPError{Method: "m", Status: 404}, true},
		{"http 403", &HTTPError{Method: "m", Status: 403}, true},
		{"http 501", &HTTPError{Method: "m", Status: 501}, true},
		{"http 500 transient", &HTTPError{Method: "m", Status: 500}, false},
		{"http 429 transient", &HTTPError{Method: "m", Status: 429}, false},
		{"rpc -32601", &rpcError{Code: -32601, Message: "the method m does not exist/is not available"}, true},
		{"rpc method not found", &rpcError{Code: -32000, Message: "method not found"}, true},
		{"rpc generic transient", &rpcError{Code: -32000, Message: "server busy"}, false},
		{"too large per-block", &ResponseTooLargeError{Method: "m", Limit: 1}, false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		if got := IsMethodUnsupported(c.err); got != c.want {
			t.Errorf("%s: IsMethodUnsupported = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestCallLimitedOverflow verifies an over-cap response is reported as a typed
// ResponseTooLargeError rather than silently truncated into a decode error.
func TestCallLimitedOverflow(t *testing.T) {
	big := `"` + strings.Repeat("a", 200) + `"`
	srv := traceServer(t, big, &rpcRequest{})
	c, err := New(Options{URL: srv.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var out string
	err = c.callLimited(context.Background(), "eth_x", &out, 32) // cap below the body size
	var tooLarge *ResponseTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("want ResponseTooLargeError, got %v", err)
	}
}

func TestCallFrameValueBig(t *testing.T) {
	cases := map[string]string{"": "0", "0x": "0", "0x0": "0", "0xde0b6b3a7640000": "1000000000000000000"}
	for in, want := range cases {
		v, err := (CallFrame{Value: in}).ValueBig()
		if err != nil {
			t.Fatalf("ValueBig(%q): %v", in, err)
		}
		if v.String() != want {
			t.Errorf("ValueBig(%q) = %s, want %s", in, v.String(), want)
		}
	}
}
