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
		Action:               action,
		Flags:                flags,
	}
}

func CommitHash() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				return setting.Value
			}
		}
	}
	return ""
}
