// Package sundaekinesis provides utilities for building AWS Kinesis consumers
// that process blockchain synchronization events.
//
// This package includes Kinesis stream event handling, Ogmios chainsync integration,
// automatic rollback/rollforward handling, and cursor management for resumable processing.
package sundaekinesis

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"github.com/SundaeSwap-finance/ogmigo/v6"
	ogmigolog "github.com/SundaeSwap-finance/ogmigo/v6/logger/zerolog"
	"github.com/SundaeSwap-finance/ogmigo/v6/ouroboros/chainsync"
	"github.com/SundaeSwap-finance/ogmigo/v6/ouroboros/chainsync/compatibility"
	"github.com/SundaeSwap-finance/sundae-go-utils/cardano"
	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	"github.com/SundaeSwap-finance/sundae-go-utils/sundae-kinesis/cursordao"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	consumer "github.com/harlow/kinesis-consumer"
	"github.com/rs/zerolog"
	"github.com/urfave/cli/v2"
)

const usage = "sundae-kinesis"

type HandleMessageCallback func(ctx context.Context, record events.KinesisEventRecord) error
type RollForwardBlockCallback func(ctx context.Context, block *chainsync.Block) error
type RollForwardTxCallback func(ctx context.Context, logger zerolog.Logger, point chainsync.PointStruct, tx chainsync.Tx) error
type RollBackwardCallback func(ctx context.Context, logger zerolog.Logger, block uint64, txs ...string) error

type Handler struct {
	Service     sundaecli.Service
	Logger      zerolog.Logger
	cursor      *cursordao.DAO
	cursorUsage string

	handleMessage HandleMessageCallback

	rollForwardBlock RollForwardBlockCallback
	rollForwardTx    RollForwardTxCallback
	rollBackward     RollBackwardCallback
}

func NewGenericHandler(
	service sundaecli.Service,
	handleMessage HandleMessageCallback,
) *Handler {
	return &Handler{
		Service:       service,
		Logger:        sundaecli.Logger(service),
		handleMessage: handleMessage,
	}
}

func NewHandler(
	service sundaecli.Service,
	rollForwardBlock RollForwardBlockCallback,
	rollForwardTx RollForwardTxCallback,
	rollBackward RollBackwardCallback,
) *Handler {
	session := session.Must(session.NewSession(aws.NewConfig()))
	api := dynamodb.New(session)
	return &Handler{
		Service:          service,
		Logger:           sundaecli.Logger(service),
		cursor:           cursordao.Build(api, sundaecli.CommonOpts.Env),
		cursorUsage:      service.Name,
		rollForwardBlock: rollForwardBlock,
		rollForwardTx:    rollForwardTx,
		rollBackward:     rollBackward,
	}
}

func NewBlockHandler(
	service sundaecli.Service,
	rollForward RollForwardBlockCallback,
	rollBackward RollBackwardCallback,
) *Handler {
	return NewHandler(service, rollForward, nil, rollBackward)
}

func NewTxHandler(
	service sundaecli.Service,
	rollForward RollForwardTxCallback,
	rollBackward RollBackwardCallback,
) *Handler {
	return NewHandler(service, nil, rollForward, rollBackward)
}

func (h *Handler) SetCursorUsage(usage string) {
	h.cursorUsage = usage
}

func (h *Handler) Start(ctx *cli.Context) error {
	if !sundaecli.CommonOpts.Console {
		lambda.Start(h.HandleKinesisEvent)
	}

	if ctx.IsSet(OgmiosFlag.Name) {
		return h.replayWithOgmios()
	} else {
		return h.handleRealtime()
	}
}

func (h *Handler) HandleKinesisEvent(ctx context.Context, event events.KinesisEvent) (err error) {
	ctx = h.Logger.WithContext(ctx)
	for _, r := range event.Records {
		if err := h.handleSingleEvent(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

type KinesisSequenceNumberKeyType string

var KinesisSequenceNumberKey = KinesisSequenceNumberKeyType("kinesisSequenceNumber")

func (h *Handler) handleSingleEvent(ctx context.Context, r events.KinesisEventRecord) (err error) {
	ctx = context.WithValue(ctx, KinesisSequenceNumberKey, r.Kinesis.SequenceNumber)

	// Sometimes we just want full access, but still do the fancy ogmios / kinesis thing
	if h.handleMessage != nil {
		return h.handleMessage(ctx, r)
	}

	var result compatibility.CompatibleResult
	if err := json.Unmarshal(r.Kinesis.Data, &result); err != nil {
		return fmt.Errorf("failed to unmarshal kinesis record: %w", err)
	}

	switch {
	case result.FindIntersection != nil:
		if ps, ok := result.FindIntersection.Intersection.PointStruct(); ok {
			return h.onRollBackward(ctx, ps)
		}

	case result.NextBlock != nil:
		if result.NextBlock.Direction == chainsync.RollBackwardString {
			if ps, ok := result.NextBlock.Point.PointStruct(); ok {
				return h.onRollBackward(ctx, ps)
			}
		} else if result.NextBlock.Direction == chainsync.RollForwardString {
			return h.onRollForward(ctx, result.NextBlock.Block)
		}
	}

	return nil
}

func (h *Handler) onRollForward(ctx context.Context, block *chainsync.Block) (err error) {
	slotTime, err := cardano.SlotToDateTimeEnv(block.Slot, "")
	if err != nil {
		return fmt.Errorf("failed to convert slot to datetime: %w", err)
	}
	h.Logger.Info().Uint64("slot", block.Slot).Time("blockTime", slotTime.Instant).Str("blockHash", block.ID).Msg("Roll forward")

	if !sundaecli.CommonOpts.Dry && !KinesisOpts.PatchReplay {
		if err := h.cursor.Save(ctx, block.PointStruct(), h.cursorUsage, block.Transactions...); err != nil {
			h.Logger.Warn().Err(err).Uint64("slot", block.Slot).Msg("failed to save point")
			return err
		}
	}

	if h.rollForwardBlock != nil {
		if err := h.rollForwardBlock(ctx, block); err != nil {
			return err
		}
	}

	if h.rollForwardTx != nil {
		for _, tx := range block.Transactions {
			if err := h.rollForwardTx(ctx, h.Logger, block.PointStruct(), tx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *Handler) onRollBackward(ctx context.Context, ps *chainsync.PointStruct) (err error) {
	h.Logger.Info().Uint64("slot", ps.Slot).Str("block", ps.ID).Msg("rolling backward")
	if sundaecli.CommonOpts.Dry || KinesisOpts.PatchReplay {
		if h.rollBackward != nil {
			// TODO?
			if err := h.rollBackward(ctx, h.Logger, 0); err != nil {
				return err
			}
		}
		return nil
	} else {
		return h.cursor.Rollback(ctx, ps.Slot, h.cursorUsage, func(ctx context.Context, block uint64, txs ...string) error {
			if h.rollBackward != nil {
				return h.rollBackward(ctx, h.Logger, block, txs...)
			}
			return nil
		})
	}
}

type Store struct {
	cursor *cursordao.DAO
}

func (s *Store) Save(ctx context.Context, point chainsync.Point) error {
	return nil
}

func (s *Store) Load(ctx context.Context) (chainsync.Points, error) {
	fallbackPoints, err := parsePoints(KinesisOpts.Point)
	if err != nil {
		return nil, fmt.Errorf("unable to parse points: %w", err)
	}

	ps, err := s.cursor.FindCursor(ctx, cursordao.BlockHighWater, usage)
	if err != nil {
		return fallbackPoints, nil
	}

	return chainsync.Points{ps.Point()}, nil
}

func wrapCursorDAO(cursor *cursordao.DAO) *Store {
	return &Store{cursor: cursor}
}

func parsePoints(pp ...string) ([]chainsync.Point, error) {
	var (
		re     = regexp.MustCompile(`^(\d+)/([^/]+)$`)
		points []chainsync.Point
	)
	for _, p := range pp {
		for _, s := range strings.Split(p, ",") {
			match := re.FindStringSubmatch(s)
			if len(match) != 3 {
				return nil, fmt.Errorf("failed to parse point, %v: expected {slot}/{blockHash}", s)
			}

			slot, _ := strconv.ParseUint(match[1], 10, 64)
			point := chainsync.PointStruct{
				ID:   match[2],
				Slot: slot,
			}
			points = append(points, point.Point())
		}
	}

	return points, nil
}
func (h *Handler) handleRealtime() error {
	streamName := KinesisOpts.StreamName
	if streamName == "" {
		streamName = fmt.Sprintf("%v-sundae-sync--tx", sundaecli.CommonOpts.Env)
	}
	var options []consumer.Option
	if KinesisOpts.Replay {
		if KinesisOpts.ReplayFrom.Value() != nil {
			options = append(options, consumer.WithShardIteratorType("AT_TIMESTAMP"))
			options = append(options, consumer.WithTimestamp(*KinesisOpts.ReplayFrom.Value()))
		} else {
			options = append(options, consumer.WithShardIteratorType("TRIM_HORIZON"))
		}
	} else {
		options = append(options, consumer.WithShardIteratorType("LATEST"))
	}
	c, err := consumer.New(streamName, options...)
	if err != nil {
		return err
	}

	ctx := h.Logger.WithContext(context.Background())
	callback := func(record *consumer.Record) error {
		er := events.KinesisEventRecord{
			Kinesis: events.KinesisRecord{Data: record.Data},
		}
		return h.handleSingleEvent(ctx, er)
	}
	fmt.Println("Listening...")
	return c.Scan(ctx, callback)
}

func (h *Handler) replayWithOgmios() error {
	ctx := h.Logger.WithContext(context.Background())
	ogmigoClient := ogmigo.New(
		ogmigo.WithPipeline(50),
		ogmigo.WithInterval(1000),
		ogmigo.WithEndpoint(KinesisOpts.Ogmios),
		ogmigo.WithLogger(ogmigolog.Wrap(h.Logger)),
	)
	h.Logger.Info().Msg("connecting to ogmios stream for local replay")
	var callback ogmigo.ChainSyncFunc = func(ctx context.Context, data []byte) (err error) {
		defer func() {
			if err != nil {
				h.Logger.Info().Err(err).Msg("handler failed")
			}
		}()
		var response compatibility.CompatibleResponsePraos
		if err := json.Unmarshal(data, &response); err != nil {
			return fmt.Errorf("failed to parse chainsync Response: %w", err)
		}

		if response.Result == nil {
			return nil
		}

		result := compatibility.CompatibleResult{}
		if r, ok := response.Result.(chainsync.ResultFindIntersectionPraos); ok {
			c := compatibility.CompatibleResultFindIntersection(r)
			result.FindIntersection = &c
		} else if r, ok := response.Result.(chainsync.ResultNextBlockPraos); ok {
			c := compatibility.CompatibleResultNextBlock(r)
			result.NextBlock = &c
		} else {
			return fmt.Errorf("unexpected result type: %T", response.Result)
		}

		return h.handleChainsyncResult(ctx, result)
	}
	chainSync, err := ogmigoClient.ChainSync(ctx, callback,
		ogmigo.WithReconnect(true),
		ogmigo.WithStore(wrapCursorDAO(h.cursor)),
	)
	if err != nil {
		h.Logger.Warn().Err(err).Msg("failed to connect to ogmios")
		return err
	}
	defer chainSync.Close()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case ogmigoErr := <-chainSync.Err():
		h.Logger.Info().Err(ogmigoErr).Msg("chainsync error")
	case <-chainSync.Done():
		h.Logger.Info().Msg("chainsync done")
	case <-ctx.Done():
		h.Logger.Info().Msg("context done")
	case <-stop:
		h.Logger.Info().Msg("caught SIGINT")
		return nil
	}

	return nil
}

func (h *Handler) handleChainsyncResult(ctx context.Context, result compatibility.CompatibleResult) error {
	switch {
	case result.FindIntersection != nil:
		if ps, ok := result.FindIntersection.Intersection.PointStruct(); ok {
			return h.onRollBackward(ctx, ps)
		}
	case result.NextBlock != nil:
		if result.NextBlock.Direction == chainsync.RollBackwardString {
			if ps, ok := result.NextBlock.Point.PointStruct(); ok {
				return h.onRollBackward(ctx, ps)
			}
		} else if result.NextBlock.Direction == chainsync.RollForwardString {
			return h.onRollForward(ctx, result.NextBlock.Block)
		}
	}

	return nil
}
