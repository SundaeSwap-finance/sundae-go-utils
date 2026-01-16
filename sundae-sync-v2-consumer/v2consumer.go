// Package syncV2Consumer provides blockchain synchronization utilities for processing
// Cardano transactions from Kinesis streams with S3 block storage.
//
// This package includes S3-backed block data retrieval, transaction advance/undo callbacks,
// parallel block downloading, and DynamoDB transaction tracking.
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
	"github.com/SundaeSwap-finance/sundae-go-utils/sundae-sync-v2-consumer/replay"
	"github.com/urfave/cli/v2"
)

var SyncV2ConsumerOpts struct {
	Transaction string
	Stream      string
	Account     string
	Timestamp   cli.Timestamp
	Bucket      string
	StartHeight uint64
}

var TransactionFlag = sundaecli.StringFlag("transaction", "Replay just one transaction", &SyncV2ConsumerOpts.Transaction)
var StreamFlag = sundaecli.StringFlag("kinesis-stream", "The stream name or arn to connect to", &SyncV2ConsumerOpts.Stream)
var AccountFlag = sundaecli.StringFlag("aws-account", "The AWS Account number, for interpolating S3 buckets", &SyncV2ConsumerOpts.Account)
var TsFlag = sundaecli.TimestampFlag("kinesis-timestamp", "2006-01-02 15:04:05", "The timestamp to start syncing from", &SyncV2ConsumerOpts.Timestamp)

// BucketFlag overrides the default `{network}-sundae-sync-v2-{account}-us-east-2`
// bucket. Accepts a bare bucket name or `s3://name[/prefix]`. When combined
// with --start-height the consumer runs in an offline replay mode that pulls
// archived blocks directly from this bucket via the S3Downloader.
var BucketFlag = sundaecli.StringFlag("bucket", "S3 bucket override (e.g. `preview-sundae-sync-v2-…-us-east-2` or `s3://…`). Combine with --start-height to drive an offline replay.", &SyncV2ConsumerOpts.Bucket)

// StartHeightFlag, when non-zero, switches the consumer into offline replay
// mode: iterate height records in the sync-v2 lookup table starting at this
// height, download each block from the configured (or default) S3 bucket, and
// dispatch through the same Advance callback the live Kinesis path uses. Pair
// with --dry to log-and-skip writes inside the Advance handler. Replays run
// until the lookup table runs out of consecutive heights.
var StartHeightFlag = sundaecli.Uint64Flag("start-height", "Offline replay: first block height to process. Iterates blocks from --bucket (or the default sync-v2 bucket) and dispatches through the same Advance callback as the live consumer.", &SyncV2ConsumerOpts.StartHeight)

var CommonFlags = []cli.Flag{
	TransactionFlag,
	StreamFlag,
	AccountFlag,
	TsFlag,
	BucketFlag,
	StartHeightFlag,
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
	} else if SyncV2ConsumerOpts.StartHeight > 0 {
		h.Logger.Info().Uint64("startHeight", SyncV2ConsumerOpts.StartHeight).Msg("Replaying from S3 archive")
		return h.StartReplay(c)
	} else {
		return fmt.Errorf("Must run as a lambda, or specify --kinesis-stream / --transaction / --start-height")
	}
}

func (h *SyncV2Consumer) StartLambda(c *cli.Context) error {

	eventStream := make(chan Message)
	group, ctx := errgroup.WithContext(c.Context)

	downloader := S3Downloader{
		Logger:  h.Logger,
		S3:      h.S3,
		Network: sundaecli.CommonOpts.Network,
		Account: SyncV2ConsumerOpts.Account,
	}
	syncer := Syncer{
		Logger:     h.Logger,
		Downloader: &downloader,
		Events:     eventStream,
		Group:      group,
	}

	syncer.SpawnSyncFunc(group, ctx, h.Undo, h.Advance)

	lambda.Start(func(_ context.Context, event events.KinesisEvent) error {
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
		Network: sundaecli.CommonOpts.Network,
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

// StartReplay drives the existing replay framework with the consumer's
// Advance callback. The block source is the S3 bucket given by --bucket
// (defaulting to the conventional `{network}-sundae-sync-v2-{account}-us-east-2`
// bucket); replay itself handles the height iteration, parallel workers, and
// per-tx dispatch.
//
// Combine with --dry in the host binary: the consumer framework itself does
// not enforce read-only behaviour, but Advance callbacks are expected to
// inspect sundaecli.CommonOpts.Dry and skip side effects when set.
func (h *SyncV2Consumer) StartReplay(c *cli.Context) error {
	bucket := SyncV2ConsumerOpts.Bucket
	if bucket == "" {
		bucket = fmt.Sprintf("%v-sundae-sync-v2-%v-us-east-2",
			sundaecli.CommonOpts.Network, SyncV2ConsumerOpts.Account)
	}

	// SharedConfigEnable so AWS_PROFILE works for `--start-height` runs invoked
	// from a developer's shell. The Kinesis/Lambda paths use the host runtime's
	// role and never hit this code. h.S3 was built without SharedConfig in
	// New(), so it also gets a fresh client here.
	awsSess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	api := dynamodb.New(awsSess)
	s3client := s3.New(awsSess)

	cfg := replay.Config{
		BlockSource: &replay.S3BlockSource{S3: s3client, Bucket: bucket},
		LookupTable: txdao.TableName(sundaecli.CommonOpts.Network),
		StartHeight: SyncV2ConsumerOpts.StartHeight,
	}
	r := replay.New(api, cfg, replay.AdvanceFunc(h.Advance), h.Logger)
	return r.Run(c.Context)
}

func (h *SyncV2Consumer) RunOne(c *cli.Context) error {
	ctx := c.Context
	downloader := S3Downloader{
		Logger:  h.Logger,
		S3:      h.S3,
		Network: sundaecli.CommonOpts.Network,
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
			if err := h.Advance(ctx, tx, uint64(block.SlotNumber()), idx); err != nil {
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
