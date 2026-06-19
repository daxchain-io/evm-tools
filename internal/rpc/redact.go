package rpc

import "net/url"

// RedactURL returns a log-safe form of a raw RPC URL: scheme, host, port, and
// path only, with the query string and any userinfo stripped. A token embedded
// in rpc.url (the canonical "?token=..." example in docs/design.md) must never
// reach stderr, error messages, banners, or metrics, so every diagnostic that
// names the endpoint runs it through this first.
//
// A URL that does not parse is reduced to a fixed placeholder rather than echoed
// back verbatim, so malformed-but-secret-bearing input still cannot leak.
func RedactURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "[unparseable-url]"
	}
	redacted := url.URL{
		Scheme: u.Scheme,
		Host:   u.Host, // host:port, no userinfo
		Path:   u.Path,
	}
	return redacted.String()
}
