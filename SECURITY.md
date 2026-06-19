# Security Policy

## Supported versions

Security fixes are applied to the latest released version only. Older versions
are not maintained.

## Reporting a vulnerability

Please report security vulnerabilities **privately**. Do **not** open a public
issue or pull request, and do not disclose the issue publicly until a fix is
released.

Use GitHub's private vulnerability reporting: open the repository's **Security**
tab and click **"Report a vulnerability"**, or go directly to

<https://github.com/daxchain-io/evm-tools/security/advisories/new>

This creates a private advisory visible only to the maintainers.

## What to expect

- We aim to acknowledge a valid report promptly and keep you updated as we
  investigate.
- We will work on a fix and coordinate a disclosure timeline with you.
- Credit is offered to reporters who follow this process, unless you prefer to
  remain anonymous.

## Scope worth special attention

Because these tools are security-sensitive by design, reports in these areas are
especially welcome:

- **RPC transport** — mTLS handling, certificate/key validation, server-name
  verification.
- **Secret handling** — leakage of tokens, mTLS keys, or interpolated/`_cmd`
  values into logs, error messages, or metrics; shell injection via `_cmd`.
- **Release supply chain** — the GoReleaser pipeline, checksum signing, the
  Homebrew tap, and the `curl | sh` installer.
