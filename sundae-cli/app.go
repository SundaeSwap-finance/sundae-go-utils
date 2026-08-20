// Package sundaecli provides common CLI utilities and boilerplate for building
// command-line applications and Lambda functions.
//
// This package includes standardized service configuration, common CLI flags,
// structured logging setup, and build information tracking.
package sundaecli

import (
	"fmt"
	"runtime/debug"

	"github.com/urfave/cli/v2"
)

func App(service Service, action cli.ActionFunc, flags ...cli.Flag) *cli.App {
	return &cli.App{
		Name:                 service.Name,
		Usage:                fmt.Sprintf("%v API Server", service.Name),
		Version:              service.Version,
		EnableBashCompletion: true,
		Before:               InitCommonOpts,
		Action:               action,
		Flags:                flags,
	}
}

// InitCommonOpts initializes default values for CommonOpts after flag parsing.
// If Network is not specified, it falls back to Env for backward compatibility.
func InitCommonOpts(c *cli.Context) error {
	if !c.IsSet("network") && c.IsSet("env") {
		return c.Set("network", c.String("env"))
	}
	return nil
}

func CommitHash() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				return setting.Value
			}
		}
		return info.Main.Version
	}
	return "unknown"
}
