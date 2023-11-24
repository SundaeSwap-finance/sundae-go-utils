package main

import (
	"log"
	"os"

	_ "embed"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	sundaegql "github.com/SundaeSwap-finance/sundae-go-utils/sundae-gql"
	"github.com/urfave/cli/v2"
)

var service = sundaecli.Service{
	Name:    "example-api",
	Version: sundaecli.CommitHash(),
}

func main() {
	app := sundaecli.App(
		service,
		action,
		append(
			sundaecli.CommonFlags,
			sundaecli.PortFlag(5001),
		)...,
	)
	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln(err)
	}
}

func action(ctx *cli.Context) error {
	return sundaegql.Webserver(&Resolver{})
}
