package flags

import "github.com/urfave/cli/v2"

var ConsoleFlag = cli.BoolFlag{
	Name:    "console",
	Usage:   "whether to run in console mode or lambda mode",
	Value:   false,
	EnvVars: []string{"CONSOLE"},
}
var EnvFlag = cli.StringFlag{
	Name:    "env",
	Usage:   "environment",
	Value:   "local",
	EnvVars: []string{"ENV"},
}

var CommonFlags = []cli.Flag{
	&ConsoleFlag,
	&EnvFlag,
}
