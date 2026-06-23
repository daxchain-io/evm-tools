//go:build windows

package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	winio "github.com/Microsoft/go-winio"
)

// pipeSecurityDescriptor restricts the named pipe to its owner, SYSTEM, and the
// local Administrators group — the Windows analogue of the Unix socket's 0600
// mode. SDDL: a protected (P) DACL granting GENERIC_ALL to SYSTEM (SY), builtin
// Administrators (BA), and the owner (OW).
const pipeSecurityDescriptor = "D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;OW)"

// pipePath normalizes a --output/--input value into a full \\.\pipe\ name: a bare
// name becomes \\.\pipe\<name>; a value already starting with \\ is used as-is.
func pipePath(name string) string {
	if strings.HasPrefix(name, `\\`) {
		return name
	}
	return `\\.\pipe\` + name
}

// listenPipe listens on a Windows named pipe and returns a fan-out writer that
// delivers each record to every connected consumer (the same machinery as the
// Unix backend), with pipe ACLs as the access control.
func listenPipe(ctx context.Context, name string, blockUntilConsumer bool) (io.WriteCloser, error) {
	if name == "" {
		return nil, errors.New("transport: empty pipe name in --output")
	}
	p := pipePath(name)
	ln, err := winio.ListenPipe(p, &winio.PipeConfig{SecurityDescriptor: pipeSecurityDescriptor})
	if err != nil {
		return nil, fmt.Errorf("transport: listen pipe %s: %w", p, err)
	}
	return newFanoutWriter(ctx, ln, blockUntilConsumer)
}

// dialPipe returns a reconnecting reader over a producer's Windows named pipe.
func dialPipe(ctx context.Context, name string) (io.ReadCloser, error) {
	if name == "" {
		return nil, errors.New("transport: empty pipe name in --input")
	}
	p := pipePath(name)
	return newReconnectingReader(ctx, func(c context.Context) (net.Conn, error) {
		return winio.DialPipeContext(c, p)
	})
}
