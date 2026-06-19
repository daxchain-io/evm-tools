package rpc

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// rpcServer is an httptest server that answers JSON-RPC requests from a handler
// keyed by method.
func rpcServer(t *testing.T, b *certBundle, handlers map[string]func() (any, *rpcError)) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req rpcRequest
		_ = json.Unmarshal(body, &req)
		resp := rpcResponse{JSONRPC: "2.0", ID: req.ID}
		h, ok := handlers[req.Method]
		if !ok {
			resp.Error = &rpcError{Code: -32601, Message: "method not found"}
		} else {
			result, rerr := h()
			if rerr != nil {
				resp.Error = rerr
			} else {
				raw, _ := json.Marshal(result)
				resp.Result = raw
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{b.serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    b.clientCAPool(),
		MinVersion:   tls.VersionTLS12,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func mtlsClient(t *testing.T, b *certBundle, url string) *Client {
	t.Helper()
	c, err := New(Options{
		URL: url,
		TLS: TLSConfig{
			ClientCert: b.clientCertPath,
			ClientKey:  b.clientKeyPath,
			CACert:     b.caCertPath,
			ServerName: b.serverName,
		},
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("New client: %v", err)
	}
	return c
}

func TestClientChainIDAndBlockNumberOverMTLS(t *testing.T) {
	b := genCerts(t)
	srv := rpcServer(t, b, map[string]func() (any, *rpcError){
		OpChainID:     func() (any, *rpcError) { return "0x1092", nil }, // 4242
		OpBlockNumber: func() (any, *rpcError) { return "0x121eac1", nil },
	})
	c := mtlsClient(t, b, srv.URL)

	id, err := c.ChainID(context.Background())
	if err != nil {
		t.Fatalf("ChainID: %v", err)
	}
	if id != 4242 {
		t.Errorf("chain id = %d, want 4242", id)
	}

	n, err := c.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf("BlockNumber: %v", err)
	}
	if n != 0x121eac1 {
		t.Errorf("block number = %d, want %d", n, 0x121eac1)
	}
}

func TestClientGetLogs(t *testing.T) {
	b := genCerts(t)
	srv := rpcServer(t, b, map[string]func() (any, *rpcError){
		OpGetLogs: func() (any, *rpcError) {
			return []Log{{
				Address:     "0xabc",
				Topics:      []string{"0xddf2"},
				Data:        "0x",
				BlockNumber: "0xa",
				TxHash:      "0xtx",
				LogIndex:    "0x1",
			}}, nil
		},
	})
	c := mtlsClient(t, b, srv.URL)
	logs, err := c.GetLogs(context.Background(), LogFilter{FromBlock: 1, ToBlock: 10})
	if err != nil {
		t.Fatalf("GetLogs: %v", err)
	}
	if len(logs) != 1 || logs[0].Address != "0xabc" {
		t.Fatalf("unexpected logs: %+v", logs)
	}
}

// TestClientBalanceAt verifies eth_getBalance decodes a 256-bit wei value beyond
// the float64 safe range.
func TestClientBalanceAt(t *testing.T) {
	b := genCerts(t)
	// 0xde0b6b3a7640000 = 1e18 wei (1 ETH).
	srv := rpcServer(t, b, map[string]func() (any, *rpcError){
		OpGetBalance: func() (any, *rpcError) { return "0xde0b6b3a7640000", nil },
	})
	c := mtlsClient(t, b, srv.URL)
	v, err := c.BalanceAt(context.Background(), "0xabc", "latest")
	if err != nil {
		t.Fatalf("BalanceAt: %v", err)
	}
	if v.String() != "1000000000000000000" {
		t.Errorf("balance = %s, want 1000000000000000000", v.String())
	}
}

// TestClientCall verifies eth_call passes the {to,data} message and returns the
// raw 0x-hex result for the caller to decode.
func TestClientCall(t *testing.T) {
	b := genCerts(t)
	srv := rpcServer(t, b, map[string]func() (any, *rpcError){
		OpCall: func() (any, *rpcError) {
			// decimals() == 18
			return "0x0000000000000000000000000000000000000000000000000000000000000012", nil
		},
	})
	c := mtlsClient(t, b, srv.URL)
	res, err := c.Call(context.Background(), CallMsg{To: "0xtoken", Data: "0x313ce567"}, "latest")
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if !strings.HasSuffix(res, "12") {
		t.Errorf("call result = %q, want trailing 12 (18 decimal)", res)
	}
}

func TestClientRPCError(t *testing.T) {
	b := genCerts(t)
	srv := rpcServer(t, b, map[string]func() (any, *rpcError){
		OpChainID: func() (any, *rpcError) { return nil, &rpcError{Code: -32000, Message: "boom"} },
	})
	c := mtlsClient(t, b, srv.URL)
	_, err := c.ChainID(context.Background())
	if err == nil {
		t.Fatal("expected rpc error")
	}
	if Classify(err) != ErrorRPC {
		t.Errorf("Classify = %q, want %q", Classify(err), ErrorRPC)
	}
}

// TestMTLSRequiredFailFast verifies an HTTPS URL with no client cert/key fails
// fast at construction.
func TestMTLSRequiredFailFast(t *testing.T) {
	_, err := New(Options{URL: "https://rpc.example.com:8545"})
	if err == nil {
		t.Fatal("expected mTLS-required error")
	}
	if err != ErrMTLSRequired {
		t.Errorf("want ErrMTLSRequired, got %v", err)
	}
}

// TestHTTPAllowedWithoutMTLS verifies plain HTTP needs no certs (local dev).
func TestHTTPAllowedWithoutMTLS(t *testing.T) {
	if _, err := New(Options{URL: "http://localhost:8545"}); err != nil {
		t.Fatalf("plain http should not require mTLS: %v", err)
	}
}

// TestMissingCertFilesFailFast verifies pointing at nonexistent cert files is a
// fail-fast error that names the path but not contents.
func TestMissingCertFilesFailFast(t *testing.T) {
	_, err := New(Options{
		URL: "https://rpc.example.com:8545",
		TLS: TLSConfig{ClientCert: "/no/such.crt", ClientKey: "/no/such.key"},
	})
	if err == nil {
		t.Fatal("expected load error")
	}
	if !strings.Contains(err.Error(), "mTLS client cert/key") {
		t.Errorf("error should reference mTLS cert/key load: %v", err)
	}
}

// TestServerRejectsNoClientCert verifies the mTLS handshake actually gates
// access: a plain (non-mTLS) HTTPS client is rejected by the server.
func TestServerRejectsNoClientCert(t *testing.T) {
	b := genCerts(t)
	srv := rpcServer(t, b, map[string]func() (any, *rpcError){
		OpChainID: func() (any, *rpcError) { return "0x1", nil },
	})
	// A client without a client cert (but trusting the CA) must be rejected.
	pool := b.clientCAPool()
	httpClient := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: b.serverName}},
		Timeout:   3 * time.Second,
	}
	resp, err := httpClient.Get(srv.URL)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected TLS handshake failure without client cert")
	}
}

func TestObserverInvoked(t *testing.T) {
	b := genCerts(t)
	srv := rpcServer(t, b, map[string]func() (any, *rpcError){
		OpChainID: func() (any, *rpcError) { return "0x1", nil },
	})
	var gotOp string
	var gotErr ErrorType
	c, err := New(Options{
		URL: srv.URL,
		TLS: TLSConfig{ClientCert: b.clientCertPath, ClientKey: b.clientKeyPath, CACert: b.caCertPath, ServerName: b.serverName},
		Observer: func(op string, _ time.Duration, et ErrorType) {
			gotOp = op
			gotErr = et
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := c.ChainID(context.Background()); err != nil {
		t.Fatalf("ChainID: %v", err)
	}
	if gotOp != OpChainID || gotErr != ErrorNone {
		t.Errorf("observer got op=%q err=%q", gotOp, gotErr)
	}
}

func TestClassifyTimeout(t *testing.T) {
	if Classify(context.DeadlineExceeded) != ErrorTimeout {
		t.Errorf("deadline should classify as timeout")
	}
	if Classify(nil) != ErrorNone {
		t.Errorf("nil should classify as none")
	}
}

// TestConnectionErrorRedactedAndClassified drives a real transport-level
// failure (a dial to a closed port) through a token-bearing https URL and
// asserts both that the surfaced error redacts the token and that the error
// still classifies as the coarse connection_error category. This pins the
// redaction-must-preserve-the-error-chain fix: rebuilding the error string with
// errors.New would strip the wrapped *net.OpError and force ErrorUnknown, so
// blockchain_rpc_error_total{error_type="connection_error"} would never fire on
// the most common transport failure.
func TestConnectionErrorRedactedAndClassified(t *testing.T) {
	b := genCerts(t)
	// Start a TLS server to capture a real loopback address, then close it so
	// the next dial is refused. The token rides in the query string exactly as a
	// real token-bearing HTTPS endpoint would format it.
	srv := rpcServer(t, b, map[string]func() (any, *rpcError){
		OpChainID: func() (any, *rpcError) { return "0x1", nil },
	})
	const token = "supersecrettoken123"
	url := srv.URL + "/?token=" + token
	srv.Close() // refuse subsequent connections

	c := mtlsClient(t, b, url)
	_, err := c.ChainID(context.Background())
	if err == nil {
		t.Fatal("expected connection error against a closed server")
	}
	if strings.Contains(err.Error(), token) {
		t.Errorf("transport error leaked the token: %v", err)
	}
	if got := Classify(err); got != ErrorConnection {
		t.Errorf("Classify = %q, want %q (err: %v)", got, ErrorConnection, err)
	}
}
