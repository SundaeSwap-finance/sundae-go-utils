package main

import (
	"fmt"
	"log"
	"os"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	"github.com/urfave/cli/v2"
)

var opts struct {
	Suffix string
}

var service = sundaecli.Service{
	Name:    "example-cli",
	Version: sundaecli.CommitHash(),
}

func main() {
	app := sundaecli.App(
		service,
		action,
		append(
			sundaecli.CommonFlags,
			sundaecli.PortFlag(5001),
			&cli.StringFlag{
				Name:        "suffix",
				Usage:       "suffix to append to the greeting",
				Value:       "world",
				Required:    false,
				EnvVars:     []string{"SUFFIX"},
				Destination: &opts.Suffix,
			},
		)...,
	)
	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln(err)
	}
}

func action(ctx *cli.Context) error {
	fmt.Printf("Hello, %v!\n", opts.Suffix)
	return nil
}
