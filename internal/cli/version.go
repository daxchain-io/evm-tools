package cli

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/daxchain-io/evm-tools/internal/buildinfo"
)

// newVersionCommand builds the fully-functional `version` subcommand. It prints
// the semantic version, git commit, build date, and Go version, and accepts
// --json for machine-readable output.
func newVersionCommand() *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print version, commit, build date, and Go version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			info := buildinfo.Get()
			out := cmd.OutOrStdout()
			if asJSON {
				enc := json.NewEncoder(out)
				enc.SetIndent("", "  ")
				return enc.Encode(info)
			}
			_, err := fmt.Fprintf(out,
				"version: %s\ncommit:  %s\ndate:    %s\ngo:      %s\n",
				info.Version, info.Commit, info.Date, info.GoVersion,
			)
			return err
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print version metadata as JSON")
	return cmd
}
