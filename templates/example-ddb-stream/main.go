package main

import (
	"context"
	"fmt"
	"log"
	"os"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	sundaeddb "github.com/SundaeSwap-finance/sundae-go-utils/sundae-ddb"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/urfave/cli/v2"
)

var service = sundaecli.NewService("example-ddb-stream")

func main() {
	app := sundaecli.App(
		service,
		action,
		append(
			sundaecli.CommonFlags,
			sundaeddb.DDBFlags...,
		)...,
	)
	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln(err)
	}
}

func onInsert(ctx context.Context, newValue map[string]*dynamodb.AttributeValue) error {
	var obj struct {
		PK   string `dynamodbav:"pk"`
		Slot uint64 `dynamodbav:"slot"`
	}
	if err := sundaeddb.ParseItem(newValue, &obj); err != nil {
		return err
	}

	fmt.Printf("object %v inserted (slot %v)\n", obj.PK, obj.Slot)
	return nil
}

func onUpdate(ctx context.Context, oldValue, newValue map[string]*dynamodb.AttributeValue) error {
	var obj struct {
		PK   string `dynamodbav:"pk"`
		Slot uint64 `dynamodbav:"slot"`
	}
	if err := sundaeddb.ParseItem(newValue, &obj); err != nil {
		return err
	}

	fmt.Printf("object %v updated (slot %v)\n", obj.PK, obj.Slot)
	return nil
}

func onDelete(ctx context.Context, oldValue map[string]*dynamodb.AttributeValue) error {
	var obj struct {
		PK   string `dynamodbav:"pk"`
		Slot uint64 `dynamodbav:"slot"`
	}
	if err := sundaeddb.ParseItem(oldValue, &obj); err != nil {
		return err
	}

	fmt.Printf("object %v deleted (slot %v)\n", obj.PK, obj.Slot)
	return nil
}

func action(_ *cli.Context) error {
	handler := sundaeddb.NewHandler(service, onInsert, onUpdate, onDelete)

	return handler.Start()
}
