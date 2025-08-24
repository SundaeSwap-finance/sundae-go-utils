package main

import (
	"context"
	"log"
	"os"
	"slices"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	syncV2Consumer "github.com/SundaeSwap-finance/sundae-go-utils/sundae-sync-v2-consumer"
	"github.com/SundaeSwap-finance/sundae-go-utils/sundae-sync-v2-consumer/dao/txdao"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/rs/zerolog"
	"github.com/urfave/cli/v2"
)

var service = sundaecli.NewService("example-v2-consumer")

var opts syncV2Consumer.ConsumerOpts

type HelloHandler struct {
	logger zerolog.Logger
	fwMsg  string
	rbMsg  string
}

func main() {
	app := sundaecli.App(
		service,
		action,
		append(
			slices.Concat(
				sundaecli.CommonFlags,
			),
			sundaecli.StringFlag("transaction", "Replay just one transaction", &opts.Transaction),
			sundaecli.StringFlag("kinesis-stream", "The stream name or arn to connect to", &opts.Stream),
			sundaecli.StringFlag("aws-account", "The AWS Account number, for interpolating S3 buckets", &opts.Account),
			sundaecli.TimestampFlag("kinesis-timestamp", "2006-01-02 15:04:05", "The timestamp to start syncing from", &opts.Timestamp),
		)...,
	)
	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln(err)
	}
}

func action(c *cli.Context) error {

	var (
		s      = session.Must(session.NewSession(aws.NewConfig()))
		s3     = s3.New(s)
		db     = dynamodb.New(s)
		txs    = txdao.Build(db)
		logger = zerolog.New(os.Stdout)
	)

	var handler HelloHandler = HelloHandler{
		logger: logger,
		fwMsg:  "Hello tx!",
		rbMsg:  "Rolling back tx!",
	}

	var consumer syncV2Consumer.SyncV2Consumer = syncV2Consumer.SyncV2Consumer{
		Logger:  logger,
		S3:      s3,
		Tx:      txs,
		Undo:    handler.rollBack,
		Advance: handler.rollForward,
		Opts:    opts,
	}

	return consumer.Action(c)

}

func (h *HelloHandler) rollBack(ctx context.Context, tx ledger.Transaction) error {
	h.logger.Info().Str("txId", tx.Hash().String()).Msg(h.rbMsg)
	return nil
}

func (h *HelloHandler) rollForward(ctx context.Context, tx ledger.Transaction, slot int, txIndex int) error {
	h.logger.Info().Str("txId", tx.Hash().String()).Msg(h.fwMsg)
	return nil
}
