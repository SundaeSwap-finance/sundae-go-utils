package sundaecli

import (
	"strings"

	"github.com/urfave/cli/v2"
)

var CommonOpts struct {
	Console    bool
	Dry        bool
	Env        string
	Network    string
	SlotOffset uint64
	Port       int
}

var ConsoleFlag = BoolFlag("console", "whether to run in console mode or lambda mode", &CommonOpts.Console)
var DryFlag = BoolFlag("dry", "whether to actually persist any records or not", &CommonOpts.Dry)
var EnvFlag = StringFlag("env", "the deployment environment", &CommonOpts.Env)
var NetworkFlag = StringFlag("network", "the cardano network (preview, mainnet)", &CommonOpts.Network)
var SlotOffset = Uint64Flag("slot-offset", "the offset for this environment between slots and unix time", &CommonOpts.SlotOffset)
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
	ConsoleFlag,
	DryFlag,
	EnvFlag,
	NetworkFlag,
	SlotOffset,
}

func ToUNDER_CAPS(s string) string {
	return strings.ToUpper(strings.Replace(s, "-", "_", -1))
}

func StringFlag(name, usage string, dest *string, value ...string) *cli.StringFlag {
	var v string
	if len(value) > 0 {
		v = value[0]
	}
	return &cli.StringFlag{
		Name:        name,
		Usage:       usage,
		Value:       v,
		EnvVars:     []string{ToUNDER_CAPS(name)},
		Destination: dest,
	}
}

func StringSliceFlag(name, usage string, aliases []string, dest *cli.StringSlice) *cli.StringSliceFlag {
	return &cli.StringSliceFlag{
		Name:        name,
		Aliases:     aliases,
		Usage:       usage,
		EnvVars:     []string{ToUNDER_CAPS(name)},
		Destination: dest,
	}
}

func TimestampFlag(name, layout, usage string, dest *cli.Timestamp) *cli.TimestampFlag {
	return &cli.TimestampFlag{
		Name:        name,
		Layout:      layout,
		Usage:       usage,
		EnvVars:     []string{ToUNDER_CAPS(name)},
		Destination: dest,
	}
}

func IntFlag(name, usage string, dest *int, value ...int) *cli.IntFlag {
	var v int
	if len(value) > 0 {
		v = value[0]
	}
	return &cli.IntFlag{
		Name:        name,
		Usage:       usage,
		Value:       v,
		EnvVars:     []string{ToUNDER_CAPS(name)},
		Destination: dest,
	}
}

func BoolFlag(name, usage string, dest *bool, value ...bool) *cli.BoolFlag {
	var v bool
	if len(value) > 0 {
		v = value[0]
	}
	return &cli.BoolFlag{
		Name:        name,
		Usage:       usage,
		Value:       v,
		EnvVars:     []string{ToUNDER_CAPS(name)},
		Destination: dest,
	}
}

func Uint64Flag(name, usage string, dest *uint64, value ...uint64) *cli.Uint64Flag {
	var v uint64
	if len(value) > 0 {
		v = value[0]
	}
	return &cli.Uint64Flag{
		Name:        name,
		Usage:       usage,
		Value:       v,
		EnvVars:     []string{ToUNDER_CAPS(name)},
		Destination: dest,
	}
}
