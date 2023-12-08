package sundaekinesis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/SundaeSwap-finance/ogmigo/v6/ouroboros/chainsync"
	"github.com/SundaeSwap-finance/ogmigo/v6/ouroboros/chainsync/compatibility"
	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	"github.com/SundaeSwap-finance/sundae-sync/dao/cursordao"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	consumer "github.com/harlow/kinesis-consumer"
	"github.com/rs/zerolog"
)

type RollForwardBlockCallback func(ctx context.Context, block *chainsync.Block) error
type RollForwardTxCallback func(ctx context.Context, logger zerolog.Logger, tx chainsync.Tx) error
type RollBackwardCallback func(ctx context.Context, logger zerolog.Logger, block uint64, txs ...string) error

type Handler struct {
	service sundaecli.Service
	logger  zerolog.Logger
	cursor  *cursordao.DAO

	rollForwardBlock RollForwardBlockCallback
	rollForwardTx    RollForwardTxCallback
	rollBackward     RollBackwardCallback
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
		service:          service,
		logger:           sundaecli.Logger(service),
		cursor:           cursordao.Build(api, sundaecli.CommonOpts.Env),
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

func (h *Handler) Start() error {
	switch {
	case sundaecli.CommonOpts.Console:
		return h.handleRealtime()

	default:
		lambda.Start(h.HandleKinesisEvent)
	}
	return nil
}

func (h *Handler) HandleKinesisEvent(ctx context.Context, event events.KinesisEvent) (err error) {
	ctx = h.logger.WithContext(ctx)
	for _, r := range event.Records {
		if err := h.handleSingleEvent(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) handleSingleEvent(ctx context.Context, r events.KinesisEventRecord) (err error) {
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
	h.logger.Info().Uint64("slot", block.Slot).Msg("Roll forward")

	if !sundaecli.CommonOpts.Dry {
		if err := h.cursor.Save(ctx, block.PointStruct(), h.service.Name, block.Transactions...); err != nil {
			h.logger.Warn().Err(err).Uint64("slot", block.Slot).Msg("failed to save point")
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
			if err := h.rollForwardTx(ctx, h.logger, tx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *Handler) onRollBackward(ctx context.Context, ps *chainsync.PointStruct) (err error) {
	h.logger.Info().Uint64("slot", ps.Slot).Str("block", ps.ID).Msg("rolling backward")
	if sundaecli.CommonOpts.Dry {
		if h.rollBackward != nil {
			// TODO?
			h.rollBackward(ctx, h.logger, 0)
		}
		return nil
	} else {
		return h.cursor.Rollback(ctx, ps.Slot, h.service.Name, func(ctx context.Context, block uint64, txs ...string) error {
			if h.rollBackward != nil {
				return h.rollBackward(ctx, h.logger, block, txs...)
			}
			return nil
		})
	}
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

	ctx := h.logger.WithContext(context.Background())
	callback := func(record *consumer.Record) error {
		er := events.KinesisEventRecord{
			Kinesis: events.KinesisRecord{Data: record.Data},
		}
		return h.handleSingleEvent(ctx, er)
	}
	fmt.Println("Listening...")
	return c.Scan(ctx, callback)
}
