// Package sundaeddb provides DynamoDB and DAX client utilities with common patterns
// for stream processing and data access.
package sundaeddb

import (
	"context"
	"encoding/json"
	"fmt"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/rs/zerolog"
	"github.com/savaki/ddb"
	"golang.org/x/sync/errgroup"
)

type BatchCallback func(ctx context.Context, event ddb.Event) error
type InsertCallback func(ctx context.Context, newValue map[string]*dynamodb.AttributeValue) error
type UpdateCallback func(ctx context.Context, oldValue, newValue map[string]*dynamodb.AttributeValue) error
type DeleteCallback func(ctx context.Context, oldValue map[string]*dynamodb.AttributeValue) error

type Handler struct {
	service sundaecli.Service
	Logger  zerolog.Logger

	onBatch  BatchCallback
	onInsert InsertCallback
	onUpdate UpdateCallback
	onDelete DeleteCallback
}

func NewHandler(
	service sundaecli.Service,
	onInsert InsertCallback,
	onUpdate UpdateCallback,
	onDelete DeleteCallback,
) *Handler {
	return &Handler{
		service:  service,
		Logger:   sundaecli.Logger(service),
		onInsert: onInsert,
		onUpdate: onUpdate,
		onDelete: onDelete,
	}
}

func NewBatchHandler(
	service sundaecli.Service,
	onBatch BatchCallback,
) *Handler {
	return &Handler{
		service: service,
		Logger:  sundaecli.Logger(service),
		onBatch: onBatch,
	}
}

func (h *Handler) Start() error {
	switch {
	case sundaecli.CommonOpts.Console:
		return h.handleRealtime()

	default:
		lambda.Start(h.HandleEvent)
	}
	return nil
}

func (h *Handler) HandleEvent(ctx context.Context, event ddb.Event) error {
	h.Logger.Trace().Int("count", len(event.Records)).Msg("handling a batch of events")
	if h.onBatch != nil {
		return h.onBatch(ctx, event)
	}
	for _, record := range event.Records {
		if err := h.HandleSingleRecord(ctx, record); err != nil {
			h.Logger.Error().Err(err).Str("event", record.EventID).Msg("unable to handle record")
			return fmt.Errorf("unable to handle record: %w", err)
		}
	}
	return nil
}

func (h *Handler) HandleSingleRecord(ctx context.Context, record ddb.Record) error {
	if h.onBatch != nil {
		return h.onBatch(ctx, ddb.Event{Records: []ddb.Record{record}})
	}
	switch record.EventName {
	case "INSERT":
		if h.onInsert != nil {
			return h.onInsert(ctx, record.Change.NewImage)
		}

	case "MODIFY":
		if h.onUpdate != nil {
			return h.onUpdate(ctx, record.Change.OldImage, record.Change.NewImage)
		}

	case "REMOVE":
		if h.onDelete != nil {
			return h.onDelete(ctx, record.Change.OldImage)
		}
	}
	return nil
}

func (h *Handler) handleRealtime() error {
	session := session.Must(session.NewSession(aws.NewConfig()))
	streams := dynamodbstreams.New(session)
	ss, err := streams.ListStreams(&dynamodbstreams.ListStreamsInput{
		TableName: aws.String(DDBOpts.TableName),
	})
	if err != nil {
		return fmt.Errorf("unable to list streams for table %v: %w", DDBOpts.TableName, err)
	}
	if len(ss.Streams) != 1 {
		return fmt.Errorf("too few or too many streams (%v) for table %v", len(ss.Streams), DDBOpts.TableName)
	}
	stream := ss.Streams[0]

	var shards []*dynamodbstreams.Shard
	var lastShard *string
	for {
		ss, err := streams.DescribeStream(&dynamodbstreams.DescribeStreamInput{
			StreamArn:             stream.StreamArn,
			ExclusiveStartShardId: lastShard,
		})
		if err != nil {
			return fmt.Errorf("unable to describe stream %v: %w", *stream.StreamArn, err)
		}
		shards = append(shards, ss.StreamDescription.Shards...)
		if ss.StreamDescription.LastEvaluatedShardId == nil {
			break
		}
		lastShard = ss.StreamDescription.LastEvaluatedShardId
	}
	group, ctx := errgroup.WithContext(context.Background())
	group.SetLimit(256)

	h.Logger.Info().Str("tableName", DDBOpts.TableName).Int("shardCount", len(shards)).Msg("responding to stream events")

	for _, shard_ := range shards {
		shard := shard_
		group.Go(func() error {
			it, err := streams.GetShardIteratorWithContext(ctx, &dynamodbstreams.GetShardIteratorInput{
				StreamArn:         stream.StreamArn,
				ShardId:           shard.ShardId,
				ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeTrimHorizon),
			})
			if err != nil {
				return fmt.Errorf("unable to get shard iterator: %w", err)
			}

			for it.ShardIterator != nil {
				records, err := streams.GetRecordsWithContext(ctx, &dynamodbstreams.GetRecordsInput{
					ShardIterator: it.ShardIterator,
				})
				if err != nil {
					return fmt.Errorf("unable to get records: %w", err)
				}
				for _, record := range records.Records {
					// Reserialize to the ddb event type, as it's nicer to work with
					raw, err := json.Marshal(record)
					if err != nil {
						return fmt.Errorf("unable to marshal record: %w", err)
					}
					var ddbr ddb.Record
					if err := json.Unmarshal(raw, &ddbr); err != nil {
						return fmt.Errorf("unable to unmarshal record: %w", err)
					}
					if err := h.HandleSingleRecord(ctx, ddbr); err != nil {
						return fmt.Errorf("error processing record %v: %w", ddbr.EventID, err)
					}
				}
				it.ShardIterator = records.NextShardIterator
			}
			return nil
		})
	}
	return group.Wait()
}

func ParseItem(item map[string]*dynamodb.AttributeValue, v interface{}) error {
	if err := dynamodbattribute.UnmarshalMap(item, &v); err != nil {
		return fmt.Errorf("unable to unmarshal item: %w", err)
	}
	return nil
}
