package rpc

import (
	"strings"
	"testing"
	"time"
)

// TestNewHTTPClientMalformedURLDoesNotLeakToken guards the secret-handling rule
// on the malformed-URL construction path: url.Parse echoes the offending URL
// (strconv.Quoted, so even the redaction substring-match cannot catch it), so
// the wrapped error must not surface a token embedded in rpc.url. The error must
// name only the redacted URL and a generic reason.
func TestNewHTTPClientMalformedURLDoesNotLeakToken(t *testing.T) {
	// A control character forces url.Parse to fail while a token rides along.
	const token = "SUPERSECRETTOKEN"
	raw := "https://rpc.internal:8545?token=" + token + "\x7f"

	_, err := newHTTPClient(raw, TLSConfig{}, time.Second)
	if err == nil {
		t.Fatal("expected an error for a malformed URL")
	}
	msg := err.Error()
	if strings.Contains(msg, token) {
		t.Fatalf("construction error leaked the token: %q", msg)
	}
	if strings.Contains(msg, "token=") {
		t.Fatalf("construction error leaked the query string: %q", msg)
	}
	// The redacted form (scheme/host/port) should still be present for ops.
	if !strings.Contains(msg, "[unparseable-url]") {
		t.Fatalf("construction error should name the redacted URL, got %q", msg)
	}
}

func TestRedactURL(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"strips query token", "https://rpc.example.com:8545?token=secret", "https://rpc.example.com:8545"},
		{"strips userinfo", "https://user:pass@rpc.example.com:8545/path", "https://rpc.example.com:8545/path"},
		{"keeps scheme host port path", "https://rpc.example.com:8545/v1/rpc", "https://rpc.example.com:8545/v1/rpc"},
		{"http allowed", "http://localhost:8545", "http://localhost:8545"},
		{"unparseable", "://bad url with spaces and \x00", "[unparseable-url]"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := RedactURL(c.in)
			if got != c.want {
				t.Errorf("RedactURL(%q) = %q, want %q", c.in, got, c.want)
			}
			// A redacted URL must never contain a token or userinfo.
			if got != "" && got != "[unparseable-url]" {
				if containsAny(got, "token=", "secret", "pass@", "user:pass") {
					t.Errorf("redacted URL leaked secret material: %q", got)
				}
			}
		})
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
	}
	return false
}
