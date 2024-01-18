package main

import (
	"context"
	"log"
	"os"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	sundaereport "github.com/SundaeSwap-finance/sundae-go-utils/sundae-report"
	"github.com/urfave/cli/v2"
)

var service = sundaecli.NewService("example-report")

func main() {
	app := sundaecli.App(
		service,
		action,
		append(
			sundaecli.CommonFlags,
			sundaereport.ReportFlags...,
		)...,
	)
	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln(err)
	}
}

func action(_ *cli.Context) error {
	handler := sundaereport.NewHandler(service, "sample", generate)

	return handler.Start()
}

func generate(ctx context.Context) (interface{}, error) {
	var report struct {
		Hello  string   `json:"hello"`
		Things []string `json:"things"`
	}
	report.Hello = "World"
	report.Things = []string{"one", "two", "three"}

	return report, nil
}
