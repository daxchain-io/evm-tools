package cli

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestStreamValidateIncludeInternalRequiresNative verifies include_internal set
// without native_transfers.enabled is rejected at validate time.
func TestStreamValidateIncludeInternalRequiresNative(t *testing.T) {
	cfg := writeStreamConfig(t, `
[rpc]
url = "http://node:8545"

[[stream.contracts]]
name = "usdc"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
events = ["Transfer"]

[stream.native_transfers]
enabled = false
include_internal = true
`)
	_, err := runWithCtx(context.Background(), t, ToolStream, "validate", "--config", cfg)
	if err == nil || !strings.Contains(err.Error(), "include_internal requires") {
		t.Fatalf("expected an include_internal-requires-native error, got: %v", err)
	}
}

// TestStreamValidateIncludeInternalOK verifies include_internal with native
// enabled validates cleanly.
func TestStreamValidateIncludeInternalOK(t *testing.T) {
	cfg := writeStreamConfig(t, `
[rpc]
url = "http://node:8545"

[stream.native_transfers]
enabled = true
include_internal = true
`)
	out, err := runWithCtx(context.Background(), t, ToolStream, "validate", "--config", cfg)
	if err != nil {
		t.Fatalf("validate: %v\n%s", err, out)
	}
}

// TestStreamIncludeInternalFlagRequiresNative verifies the --include-internal flag
// alone (without --native-transfers) is rejected.
func TestStreamIncludeInternalFlagRequiresNative(t *testing.T) {
	cfg := writeStreamConfig(t, "[rpc]\nurl = \"http://node:8545\"\n")
	_, err := runWithCtx(context.Background(), t, ToolStream, "validate", "--config", cfg,
		"--contract", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "--include-internal")
	if err == nil || !strings.Contains(err.Error(), "include_internal requires") {
		t.Fatalf("expected include_internal-requires-native error, got: %v", err)
	}

	// With --native-transfers it validates.
	out, err := runWithCtx(context.Background(), t, ToolStream, "validate", "--config", cfg,
		"--native-transfers", "--include-internal")
	if err != nil {
		t.Fatalf("validate with --native-transfers --include-internal: %v\n%s", err, out)
	}
}

// traceProbeServer answers chainId/blockNumber and, for debug_traceBlockByNumber,
// either a valid empty trace (supported) or a method-not-found error.
func traceProbeServer(t *testing.T, traceSupported bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     uint64 `json:"id"`
			Method string `json:"method"`
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case "eth_chainId":
			writeResult(w, req.ID, `"0x1"`)
		case "eth_blockNumber":
			writeResult(w, req.ID, `"0x10"`)
		case "debug_traceBlockByNumber":
			if traceSupported {
				writeResult(w, req.ID, `[]`)
			} else {
				writeMethodNotFound(w, req.ID, req.Method)
			}
		case "trace_block", "debug_traceTransaction":
			// Parity / per-tx fallbacks: also unsupported on this node, so the
			// cascade reaches a clean "no trace method" verdict.
			writeMethodNotFound(w, req.ID, req.Method)
		default:
			writeResult(w, req.ID, `"0x0"`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func writeResult(w http.ResponseWriter, id uint64, result string) {
	_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(id) + `,"result":` + result + `}`))
}

func writeMethodNotFound(w http.ResponseWriter, id uint64, method string) {
	_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(id) + `,"error":{"code":-32601,"message":"method ` + method + ` does not exist"}}`))
}

type checkProbeOut struct {
	OK             bool   `json:"ok"`
	TraceSupported *bool  `json:"trace_supported"`
	TraceBackend   string `json:"trace_backend"`
}

// TestCheckRPCTraceProbeParity verifies the probe reports supported via the parity
// backend when the node lacks debug_traceBlockByNumber but serves trace_block.
func TestCheckRPCTraceProbeParity(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     uint64 `json:"id"`
			Method string `json:"method"`
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case "eth_chainId":
			writeResult(w, req.ID, `"0x1"`)
		case "eth_blockNumber":
			writeResult(w, req.ID, `"0x10"`)
		case "debug_traceBlockByNumber":
			writeMethodNotFound(w, req.ID, req.Method) // no debug_ namespace (anvil-like)
		case "trace_block":
			writeResult(w, req.ID, `[]`) // parity tracer works
		default:
			writeResult(w, req.ID, `"0x0"`)
		}
	}))
	t.Cleanup(srv.Close)
	cfg := writeStreamConfig(t, `
[rpc]
url = "`+srv.URL+`"

[stream.native_transfers]
enabled = true
include_internal = true
`)
	out, err := runWithCtx(context.Background(), t, ToolStream, "check", "rpc", "--config", cfg)
	if err != nil {
		t.Fatalf("check rpc: %v\n%s", err, out)
	}
	var res checkProbeOut
	if jerr := json.Unmarshal([]byte(out), &res); jerr != nil {
		t.Fatalf("parse: %v\n%s", jerr, out)
	}
	if res.TraceSupported == nil || !*res.TraceSupported {
		t.Errorf("trace_supported should be true via parity:\n%s", out)
	}
	if res.TraceBackend != "trace_block" {
		t.Errorf("trace_backend = %q, want trace_block", res.TraceBackend)
	}
}

// TestCheckRPCTraceProbe verifies `check rpc` probes and reports trace support
// when include_internal is configured.
func TestCheckRPCTraceProbe(t *testing.T) {
	for _, supported := range []bool{true, false} {
		srv := traceProbeServer(t, supported)
		cfg := writeStreamConfig(t, `
[rpc]
url = "`+srv.URL+`"

[stream.native_transfers]
enabled = true
include_internal = true
`)
		out, err := runWithCtx(context.Background(), t, ToolStream, "check", "rpc", "--config", cfg)
		if err != nil {
			t.Fatalf("check rpc (supported=%v): %v\n%s", supported, err, out)
		}
		var res checkProbeOut
		if jerr := json.Unmarshal([]byte(out), &res); jerr != nil {
			t.Fatalf("parse check output: %v\n%s", jerr, out)
		}
		if !res.OK {
			t.Errorf("supported=%v: check should be OK (reachable)", supported)
		}
		if res.TraceSupported == nil {
			t.Fatalf("supported=%v: trace_supported should be reported", supported)
		}
		if *res.TraceSupported != supported {
			t.Errorf("trace_supported = %v, want %v", *res.TraceSupported, supported)
		}
	}
}

// TestCheckRPCTraceProbeInconclusive verifies a transient probe failure (here a
// 500 on the parity fallback after block-level is method-not-found) reports
// inconclusive — trace_supported unset + a trace_error — never a false "no".
func TestCheckRPCTraceProbeInconclusive(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     uint64 `json:"id"`
			Method string `json:"method"`
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &req)
		switch req.Method {
		case "eth_chainId":
			w.Header().Set("Content-Type", "application/json")
			writeResult(w, req.ID, `"0x1"`)
		case "eth_blockNumber":
			w.Header().Set("Content-Type", "application/json")
			writeResult(w, req.ID, `"0x10"`)
		case "debug_traceBlockByNumber":
			w.Header().Set("Content-Type", "application/json")
			writeMethodNotFound(w, req.ID, req.Method)
		case "trace_block":
			http.Error(w, "upstream timeout", http.StatusInternalServerError) // transient
		default:
			w.Header().Set("Content-Type", "application/json")
			writeResult(w, req.ID, `"0x0"`)
		}
	}))
	t.Cleanup(srv.Close)
	cfg := writeStreamConfig(t, `
[rpc]
url = "`+srv.URL+`"

[stream.native_transfers]
enabled = true
include_internal = true
`)
	out, err := runWithCtx(context.Background(), t, ToolStream, "check", "rpc", "--config", cfg)
	if err != nil {
		t.Fatalf("check rpc: %v\n%s", err, out)
	}
	if strings.Contains(out, `"trace_supported"`) {
		t.Errorf("trace_supported should be unset (inconclusive), not a false no:\n%s", out)
	}
	if !strings.Contains(out, `"trace_error"`) {
		t.Errorf("expected a trace_error explaining the inconclusive probe:\n%s", out)
	}
}

// TestCheckRPCNoTraceProbeWithoutInclude verifies the trace probe is skipped (and
// the field omitted) when include_internal is off.
func TestCheckRPCNoTraceProbeWithoutInclude(t *testing.T) {
	srv := traceProbeServer(t, false)
	cfg := writeStreamConfig(t, `
[rpc]
url = "`+srv.URL+`"

[stream.native_transfers]
enabled = true
`)
	out, err := runWithCtx(context.Background(), t, ToolStream, "check", "rpc", "--config", cfg)
	if err != nil {
		t.Fatalf("check rpc: %v\n%s", err, out)
	}
	if strings.Contains(out, "trace_supported") {
		t.Errorf("trace_supported should be omitted when include_internal is off:\n%s", out)
	}
}
