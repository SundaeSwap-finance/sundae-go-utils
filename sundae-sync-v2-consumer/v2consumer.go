package syncV2Consumer

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/blinklabs-io/gouroboros/ledger"
	consumer "github.com/harlow/kinesis-consumer"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	"github.com/SundaeSwap-finance/sundae-go-utils/sundae-sync-v2-consumer/dao/txdao"
	"github.com/urfave/cli/v2"
)

var SyncV2ConsumerOpts struct {
	Transaction string
	Stream      string
	Account     string
	Timestamp   cli.Timestamp
}

var TransactionFlag = sundaecli.StringFlag("transaction", "Replay just one transaction", &SyncV2ConsumerOpts.Transaction)
var StreamFlag = sundaecli.StringFlag("kinesis-stream", "The stream name or arn to connect to", &SyncV2ConsumerOpts.Stream)
var AccountFlag = sundaecli.StringFlag("aws-account", "The AWS Account number, for interpolating S3 buckets", &SyncV2ConsumerOpts.Account)
var TsFlag = sundaecli.TimestampFlag("kinesis-timestamp", "2006-01-02 15:04:05", "The timestamp to start syncing from", &SyncV2ConsumerOpts.Timestamp)

var CommonFlags = []cli.Flag{
	TransactionFlag,
	StreamFlag,
	AccountFlag,
	TsFlag,
}

type SyncV2Consumer struct {
	Logger  zerolog.Logger
	S3      s3iface.S3API
	Tx      *txdao.DAO
	Undo    UndoFunc
	Advance AdvanceFunc
}

func New(advance AdvanceFunc, undo UndoFunc, logger *zerolog.Logger) SyncV2Consumer {
	var (
		s   = session.Must(session.NewSession(aws.NewConfig()))
		s3  = s3.New(s)
		db  = dynamodb.New(s)
		txs = txdao.Build(db)
	)

	if logger == nil {
		newLogger := zerolog.New(os.Stdout)
		logger = &newLogger
	}

	var consumer SyncV2Consumer = SyncV2Consumer{
		Logger:  *logger,
		S3:      s3,
		Tx:      txs,
		Undo:    undo,
		Advance: advance,
	}

	return consumer
}

func (h *SyncV2Consumer) Start(c *cli.Context) error {
	if !sundaecli.CommonOpts.Console {
		h.Logger.Info().Msg("Starting lambda handler")
		return h.StartLambda(c)
	} else if SyncV2ConsumerOpts.Stream != "" {
		h.Logger.Info().Msg("Starting kinesis handler")
		return h.StartKinesis(c)
	} else if SyncV2ConsumerOpts.Transaction != "" {
		h.Logger.Info().Msg("Replaying specific transaction")
		return h.RunOne(c)
	} else {
		return fmt.Errorf("Must run as a lambda, or specify --steam or --utxorpc-url")
	}
}

func (h *SyncV2Consumer) StartLambda(c *cli.Context) error {

	eventStream := make(chan Message)
	group, ctx := errgroup.WithContext(c.Context)

	downloader := S3Downloader{
		Logger:  h.Logger,
		S3:      h.S3,
		Env:     sundaecli.CommonOpts.Env,
		Account: SyncV2ConsumerOpts.Account,
	}
	syncer := Syncer{
		Logger:     h.Logger,
		Downloader: &downloader,
		Events:     eventStream,
		Group:      group,
	}

	syncer.SpawnSyncFunc(group, ctx, h.Undo, h.Advance)

	lambda.Start(func(ctx context.Context, event events.KinesisEvent) error {
		ctx = h.Logger.WithContext(ctx)
		for _, r := range event.Records {
			if err := <-syncer.HandleOne(r.Kinesis.Data); err != nil {
				return err
			}
		}
		return nil
	})

	if err := group.Wait(); err != nil {
		return fmt.Errorf("failure processing events: %w", err)
	}

	return nil
}

func (h *SyncV2Consumer) StartKinesis(c *cli.Context) error {
	var options []consumer.Option
	ts := SyncV2ConsumerOpts.Timestamp.Value()
	if ts == nil {
		h.Logger.Info().Msg("Starting at latest message")
		options = append(options, consumer.WithShardIteratorType("LATEST"))
	} else {
		h.Logger.Info().Str("timestamp", ts.Format("2006-01-02 15:04:05")).Msg("Starting at timestamp")
		options = append(options, consumer.WithShardIteratorType("AT_TIMESTAMP"), consumer.WithTimestamp(*ts))
	}
	k, err := consumer.New(SyncV2ConsumerOpts.Stream, options...)
	if err != nil {
		return err
	}

	events := make(chan Message)
	group, ctx := errgroup.WithContext(c.Context)

	downloader := S3Downloader{
		Logger:  h.Logger,
		S3:      h.S3,
		Env:     sundaecli.CommonOpts.Env,
		Account: SyncV2ConsumerOpts.Account,
	}
	syncer := Syncer{
		Logger:     h.Logger,
		Downloader: &downloader,
		Events:     events,
		Group:      group,
	}

	syncer.SpawnSyncFunc(group, ctx, h.Undo, h.Advance)

	err = k.Scan(ctx, func(r *consumer.Record) error {
		return <-syncer.HandleOne(r.Data)
	})
	if err != nil {
		return fmt.Errorf("failure reading from kinesis: %w", err)
	}

	if err := group.Wait(); err != nil {
		return fmt.Errorf("failure processing events: %w", err)
	}

	return nil
}

func (h *SyncV2Consumer) RunOne(c *cli.Context) error {
	ctx := c.Context
	downloader := S3Downloader{
		Logger:  h.Logger,
		S3:      h.S3,
		Env:     sundaecli.CommonOpts.Env,
		Account: SyncV2ConsumerOpts.Account,
	}

	tx, err := h.Tx.Get(ctx, SyncV2ConsumerOpts.Transaction)
	if err != nil {
		return fmt.Errorf("transaction not found: %w", err)
	}
	blockHash, err := hex.DecodeString(tx.Block)
	if err != nil {
		return fmt.Errorf("invalid block hash: %w", err)
	}
	blockContents, err := downloader.DownloadBlockSync(blockHash)
	if err != nil {
		return fmt.Errorf("failed to download block: %w", err)
	}
	blockType := uint(blockContents[1])
	block, err := ledger.NewBlockFromCbor(blockType, blockContents[2:])
	if err != nil {
		return fmt.Errorf("failed to parse block: %w", err)
	}

	found := false
	for idx, tx := range block.Transactions() {
		if tx.Hash().String() == SyncV2ConsumerOpts.Transaction {
			found = true
			if err := h.Advance(ctx, tx, int(block.SlotNumber()), idx); err != nil {
				return fmt.Errorf("failed to advance tx: %w", err)
			}
			break
		}
	}
	if !found {
		return fmt.Errorf("unable to find transaction %v in block %v", SyncV2ConsumerOpts.Transaction, tx.Block)
	}
	return nil
}
