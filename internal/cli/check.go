package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// checkResult is the short JSON status object printed by `check rpc`. It carries
// only safe fields — the endpoint is redacted, never the raw URL/token.
type checkResult struct {
	OK          bool   `json:"ok"`
	Endpoint    string `json:"endpoint"`
	ChainID     int64  `json:"chain_id,omitempty"`
	BlockNumber uint64 `json:"block_number,omitempty"`
	Error       string `json:"error,omitempty"`
	ErrorType   string `json:"error_type,omitempty"`
	// TraceSupported is set only when a trace-capability probe was requested
	// (evm-stream with native_transfers.include_internal): true if the endpoint
	// serves a trace method usable for internal transfers. TraceBackend names which
	// one. Reachability (OK) does not depend on trace — it is an optional capability.
	TraceSupported *bool  `json:"trace_supported,omitempty"`
	TraceBackend   string `json:"trace_backend,omitempty"`
	TraceError     string `json:"trace_error,omitempty"`
}

// runCheckRPC performs the one-shot RPC health check shared by both tools. It
// connects using the same mTLS transport as a normal run, performs lightweight
// eth_chainId + eth_blockNumber probes, prints a short JSON status to stdout,
// and returns an error (non-zero exit) when the endpoint is unreachable.
func runCheckRPC(cmd *cobra.Command, rpcCfg rpcMaterial, probeTrace bool) error {
	client, err := rpc.New(rpc.Options{
		URL:     rpcCfg.URL,
		TLS:     rpcCfg.TLS,
		Timeout: 15 * time.Second,
	})
	if err != nil {
		// Construction failure (e.g. missing mTLS material): report and exit
		// non-zero, but still print a redacted status object.
		return emitCheckResult(cmd, checkResult{
			OK:       false,
			Endpoint: rpc.RedactURL(rpcCfg.URL),
			Error:    err.Error(),
		})
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), 20*time.Second)
	defer cancel()

	res := checkResult{Endpoint: client.RedactedURL()}

	chainID, err := client.ChainID(ctx)
	if err != nil {
		res.Error = err.Error()
		res.ErrorType = string(rpc.Classify(err))
		return emitCheckResult(cmd, res)
	}
	res.ChainID = chainID

	blockNum, err := client.BlockNumber(ctx)
	if err != nil {
		res.Error = err.Error()
		res.ErrorType = string(rpc.Classify(err))
		return emitCheckResult(cmd, res)
	}
	res.BlockNumber = blockNum
	res.OK = true

	// Optional trace-capability probe (evm-stream + include_internal): does the
	// endpoint serve a trace method usable for internal transfers? It mirrors the
	// runtime cascade (block-level then parity) and distinguishes a real capability
	// gap from a transient failure, so it never mislabels a healthy node as
	// unsupported. A probe failure does not flip OK — trace is optional.
	if probeTrace {
		probeTraceSupport(ctx, client, blockNum, &res)
	}
	return emitCheckResult(cmd, res)
}

// probeTraceSupport mirrors the runtime trace-backend cascade for `check rpc`:
// it tries the cheap block-level probe (onlyTopCall), then the parity tracer, and
// records the first that responds. A method-rejecting error advances the cascade;
// a transient error is reported as such (trace_error) rather than a false
// "unsupported". The per-tx backend is not probed (it needs a specific tx); a
// node serving only that reports unsupported here but still works at runtime.
func probeTraceSupport(ctx context.Context, client *rpc.Client, blockNum uint64, res *checkResult) {
	// Tri-state: trace_supported is set true (a backend responded), false (a backend
	// definitively rejected the method), or left unset = inconclusive (a transient/
	// heavy-block probe failure, reported via trace_error) — never a false "no".
	yes := func(backend string) {
		t := true
		res.TraceSupported, res.TraceBackend = &t, backend
	}
	no := func() {
		f := false
		res.TraceSupported = &f
	}

	// Cheap block-level probe (onlyTopCall) — won't be derailed by a heavy block.
	if err := client.ProbeTraceBlock(ctx, blockNum); err == nil {
		yes(rpc.OpDebugTraceBlock)
		return
	} else if !rpc.IsMethodUnsupported(err) {
		res.TraceError = err.Error() // inconclusive; leave trace_supported unset
		return
	}
	// Parity fallback. trace_block has no onlyTopCall, so a transient/heavy-block
	// failure is reported as inconclusive rather than a false "unsupported".
	if _, err := client.TraceBlockParity(ctx, blockNum); err == nil {
		yes(rpc.OpTraceBlock)
		return
	} else if !rpc.IsMethodUnsupported(err) {
		res.TraceError = err.Error()
		return
	}
	// Both single-call backends definitively rejected the method. (The per-tx
	// backend is not probed here; a node serving only it reports unsupported but
	// still works at runtime.)
	no()
}

// emitCheckResult prints the JSON status to stdout and returns a non-nil error
// when the check failed, so the process exits non-zero.
func emitCheckResult(cmd *cobra.Command, res checkResult) error {
	enc := json.NewEncoder(cmd.OutOrStdout())
	if encErr := enc.Encode(res); encErr != nil {
		return encErr
	}
	if !res.OK {
		return fmt.Errorf("rpc check failed: %s", res.Endpoint)
	}
	return nil
}

// rpcMaterial is the resolved RPC URL plus mTLS material a command needs.
type rpcMaterial struct {
	URL string
	TLS rpc.TLSConfig
}
