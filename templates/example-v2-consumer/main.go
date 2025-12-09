package main

import (
	"context"
	"log"
	"os"
	"slices"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	syncV2Consumer "github.com/SundaeSwap-finance/sundae-go-utils/sundae-sync-v2-consumer"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/rs/zerolog"
	"github.com/urfave/cli/v2"
)

var service = sundaecli.NewService("example-v2-consumer")

type HelloHandler struct {
	logger zerolog.Logger
	fwMsg  string
	rbMsg  string
}

func main() {
	app := sundaecli.App(
		service,
		action,
		slices.Concat(
			sundaecli.CommonFlags,
			syncV2Consumer.CommonFlags,
		)...,
	)
	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln(err)
	}
}

func action(c *cli.Context) error {
	var handler HelloHandler = HelloHandler{
		logger: zerolog.New(os.Stdout),
		fwMsg:  "Hello tx!",
		rbMsg:  "Rolling back tx!",
	}

	var consumer = syncV2Consumer.New(
		handler.rollForward,
		handler.rollBack,
		&handler.logger,
	)
	return consumer.Start(c)

}

func (h *HelloHandler) rollBack(ctx context.Context, tx ledger.Transaction, slot uint64, txIndex int) error {
	h.logger.Info().Str("txId", tx.Hash().String()).Msg(h.rbMsg)
	return nil
}

func (h *HelloHandler) rollForward(ctx context.Context, tx ledger.Transaction, slot uint64, txIndex int) error {
	h.logger.Info().Str("txId", tx.Hash().String()).Msg(h.fwMsg)
	return nil
}
