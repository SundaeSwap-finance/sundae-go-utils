package sundaecli

import "github.com/urfave/cli/v2"

var CommonOpts struct {
	Console    bool
	Dry        bool
	Env        string
	SlotOffset uint64
	Port       int
}

var ConsoleFlag = cli.BoolFlag{
	Name:        "console",
	Usage:       "whether to run in console mode or lambda mode",
	Value:       false,
	EnvVars:     []string{"CONSOLE"},
	Destination: &CommonOpts.Console,
}
var DryFlag = cli.BoolFlag{
	Name:        "dry",
	Usage:       "whether to actually persist any records or not",
	Value:       false,
	EnvVars:     []string{"DRY"},
	Destination: &CommonOpts.Dry,
}
var EnvFlag = cli.StringFlag{
	Name:        "env",
	Usage:       "environment",
	Value:       "local",
	EnvVars:     []string{"ENV"},
	Destination: &CommonOpts.Env,
}
var SlotOffset = cli.Uint64Flag{
	Name:        "slot-offset",
	Usage:       "the environment offset between slots and unix time",
	Value:       0,
	EnvVars:     []string{"SLOT_OFFSET"},
	Destination: &CommonOpts.SlotOffset,
}
var PortFlag = func(p int) *cli.IntFlag {
	return &cli.IntFlag{
		Name:        "port",
		Usage:       "Port to listen to, if running locally",
		Value:       p,
		EnvVars:     []string{"PORT"},
		Destination: &CommonOpts.Port,
	}
}

var CommonFlags = []cli.Flag{
	&ConsoleFlag,
	&DryFlag,
	&EnvFlag,
	&SlotOffset,
}
