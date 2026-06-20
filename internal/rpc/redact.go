package rpc

import "net/url"

// RedactURL returns a log-safe form of a raw RPC URL: scheme and host:port only,
// with the query string, any userinfo, and the path stripped. Secrets ride in
// rpc.url in more than one place — the canonical "?token=..." query (docs/design.md)
// and, for public providers, the path itself (Alchemy "/v2/<key>", Infura
// "/v3/<id>", QuickNode "/<key>/", Ankr "/eth/<key>"). A non-root path is therefore
// treated as potentially secret and replaced with a fixed marker; the host still
// identifies the provider and chain for diagnostics. Every diagnostic that names
// the endpoint runs it through this first, so a key never reaches stderr, error
// messages, banners, or metrics.
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
	if u.Path != "" && u.Path != "/" {
		// The path may itself be the API key; never echo it. A single marker is
		// safe regardless of where in the path a given provider places the secret.
		// Assemble manually so url.URL.String does not percent-encode the marker.
		return u.Scheme + "://" + u.Host + "/[redacted]"
	}
	redacted := url.URL{
		Scheme: u.Scheme,
		Host:   u.Host, // host:port, no userinfo
		Path:   u.Path,
	}
	return redacted.String()
}
