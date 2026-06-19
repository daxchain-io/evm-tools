// Package buildinfo holds version metadata stamped into the binaries at build
// time via -ldflags -X. GoReleaser populates these values for tagged releases;
// for local "go build" they fall back to development defaults.
package buildinfo

import "runtime"

// These variables are overridden at link time with:
//
//	-ldflags "-X github.com/daxchain-io/evm-tools/internal/buildinfo.Version=v1.2.3 ..."
//
// They are vars (not consts) precisely so the linker can set them.
var (
	// Version is the semantic version, e.g. "v0.1.0". "dev" for local builds.
	Version = "dev"
	// Commit is the git commit hash the binary was built from.
	Commit = "none"
	// Date is the build timestamp (RFC 3339) recorded at link time.
	Date = "unknown"
)

// Info is a snapshot of the build metadata, suitable for printing or
// serializing to JSON.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	Date      string `json:"date"`
	GoVersion string `json:"go_version"`
}

// Get returns the current build metadata, including the Go toolchain version
// the binary was compiled with.
func Get() Info {
	return Info{
		Version:   Version,
		Commit:    Commit,
		Date:      Date,
		GoVersion: runtime.Version(),
	}
}
