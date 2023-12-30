package main

import (
	"context"
	"log"
	"os"

	"github.com/SundaeSwap-finance/ogmigo/v6/ouroboros/chainsync"
	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	sundaekinesis "github.com/SundaeSwap-finance/sundae-go-utils/sundae-kinesis"
	"github.com/rs/zerolog"
	"github.com/urfave/cli/v2"
)

var service = sundaecli.NewService("example-kinesis")

func main() {
	app := sundaecli.App(
		service,
		action,
		append(
			sundaecli.CommonFlags,
			sundaekinesis.KinesisFlags...,
		)...,
	)
	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln(err)
	}
}

func action(_ *cli.Context) error {
	handler := sundaekinesis.NewTxHandler(service, rollForwardTx, rollBackward)

	return handler.Start()
}

func rollForwardTx(ctx context.Context, logger zerolog.Logger, point chainsync.PointStruct, tx chainsync.Tx) error {
	logger.Info().Str("txHash", tx.ID).Str("point", point.Point().String()).Msg("Roll forward")
	return nil
}

func rollBackward(ctx context.Context, logger zerolog.Logger, block uint64, txs ...string) error {
	logger.Info().Msg("Roll backward")
	return nil
}
