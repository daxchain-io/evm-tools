//go:build !windows

package transport

import (
	"context"
	"errors"
	"io"
)

// errPipeUnsupported reports that the named-pipe transport is Windows-only.
var errPipeUnsupported = errors.New(`transport: "pipe:" is only supported on Windows; use "unix:/path" on Linux/macOS`)

func listenPipe(context.Context, string, bool) (io.WriteCloser, error) {
	return nil, errPipeUnsupported
}

func dialPipe(context.Context, string) (io.ReadCloser, error) {
	return nil, errPipeUnsupported
}
