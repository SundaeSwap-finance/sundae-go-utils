package main

import (
	"log"
	"os"

	_ "embed"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	sundaegql "github.com/SundaeSwap-finance/sundae-go-utils/sundae-gql"
	"github.com/urfave/cli/v2"
)

var service = sundaecli.NewService("example-gql")

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

//go:embed example.gql
var schema string

type Resolver struct {
}

func (r *Resolver) Schema() string {
	return schema
}

func (r *Resolver) Config() *sundaegql.BaseConfig {
	return &sundaegql.BaseConfig{
		Logger:  sundaecli.Logger(service),
		Service: service,
	}
}

func (r *Resolver) Hello() string {
	return "world!"
}

func (r *Resolver) World() string {
	return "Hello"
}
