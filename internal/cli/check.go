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
}

// runCheckRPC performs the one-shot RPC health check shared by both tools. It
// connects using the same mTLS transport as a normal run, performs lightweight
// eth_chainId + eth_blockNumber probes, prints a short JSON status to stdout,
// and returns an error (non-zero exit) when the endpoint is unreachable.
func runCheckRPC(cmd *cobra.Command, rpcCfg rpcMaterial) error {
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
	return emitCheckResult(cmd, res)
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
