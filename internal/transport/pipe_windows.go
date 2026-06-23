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
	"golang.org/x/sys/windows"
)

// pipeSecurityDescriptor builds an SDDL that restricts the named pipe to the
// launching user, plus SYSTEM and the local Administrators group — the Windows
// analogue of the Unix socket's 0600 mode. The user's SID is bound explicitly
// (as the owner and via an allow-ACE) rather than relying on the dynamic OWNER
// RIGHTS (OW) alias, which collapses to the Administrators group when the process
// token's default owner is a group (the elevated/service case).
func pipeSecurityDescriptor() (string, error) {
	// GetCurrentProcessToken returns a pseudo-handle for the process token; it
	// needs no Close.
	u, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return "", fmt.Errorf("get token user: %w", err)
	}
	sid := u.User.Sid.String()
	// Owner + primary group = the user; a protected (P) DACL granting GENERIC_ALL
	// to SYSTEM (SY), builtin Administrators (BA), and the user's SID.
	return fmt.Sprintf("O:%[1]sG:%[1]sD:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;%[1]s)", sid), nil
}

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
// Unix backend), with the pipe's ACL as the access control.
func listenPipe(ctx context.Context, name string, blockUntilConsumer bool) (io.WriteCloser, error) {
	if name == "" {
		return nil, errors.New("transport: empty pipe name in --output")
	}
	sddl, err := pipeSecurityDescriptor()
	if err != nil {
		return nil, fmt.Errorf("transport: pipe security descriptor: %w", err)
	}
	p := pipePath(name)
	ln, err := winio.ListenPipe(p, &winio.PipeConfig{SecurityDescriptor: sddl})
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
