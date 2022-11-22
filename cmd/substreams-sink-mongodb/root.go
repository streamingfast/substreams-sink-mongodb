package main

import (
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:          "substreams-sink-mongodb",
	SilenceUsage: true,
}

func init() {
}
