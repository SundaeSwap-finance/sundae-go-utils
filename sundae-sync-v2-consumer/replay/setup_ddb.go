package replay

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// SetupConfig configures throwaway DDB tables for a distributed replay.
type SetupConfig struct {
	Prefix      string // table name prefix; tables become {Prefix}-claims and {Prefix}-tx
	StartHeight uint64 // first height of the replay range
	EndHeight   uint64 // exclusive; one past the last height to process
	ChunkSize   uint64 // heights per chunk; defaults to 10000
}

// SetupDDB creates the two coordinator tables (claims, tx), waits for them to
// become ACTIVE, and seeds the chunk records (one per chunk plus a "cursor"
// row holding the total chunk count).
//
// Idempotent at the table level: if a table already exists with the right
// name, SetupDDB will reuse it and re-seed any missing chunk records. This
// lets you resume an interrupted setup without dropping the table.
func SetupDDB(ctx context.Context, api dynamodbiface.DynamoDBAPI, cfg SetupConfig) error {
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = 10000
	}
	if cfg.EndHeight <= cfg.StartHeight {
		return fmt.Errorf("EndHeight (%d) must be > StartHeight (%d)", cfg.EndHeight, cfg.StartHeight)
	}

	claimsTable := cfg.Prefix + "-claims"
	txTable := cfg.Prefix + "-tx"

	if err := createTableIfMissing(ctx, api, claimsTable); err != nil {
		return fmt.Errorf("create %s: %w", claimsTable, err)
	}
	if err := createTableIfMissing(ctx, api, txTable); err != nil {
		return fmt.Errorf("create %s: %w", txTable, err)
	}
	if err := waitForActive(ctx, api, claimsTable); err != nil {
		return err
	}
	if err := waitForActive(ctx, api, txTable); err != nil {
		return err
	}

	// Compute chunks. Round up so the final chunk covers any remainder.
	totalChunks := (cfg.EndHeight - cfg.StartHeight + cfg.ChunkSize - 1) / cfg.ChunkSize

	// Seed chunk rows. PutItem with ConditionExpression(attribute_not_exists(pk))
	// makes this safe to re-run.
	for idx := uint64(0); idx < totalChunks; idx++ {
		chunkStart := cfg.StartHeight + idx*cfg.ChunkSize
		chunkEnd := chunkStart + cfg.ChunkSize
		if chunkEnd > cfg.EndHeight {
			chunkEnd = cfg.EndHeight
		}
		_, err := api.PutItemWithContext(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(claimsTable),
			Item: map[string]*dynamodb.AttributeValue{
				"pk":     {S: aws.String(strconv.FormatUint(idx, 10))},
				"start":  {N: aws.String(strconv.FormatUint(chunkStart, 10))},
				"end":    {N: aws.String(strconv.FormatUint(chunkEnd, 10))},
				"status": {S: aws.String(chunkStatusPending)},
			},
			ConditionExpression: aws.String("attribute_not_exists(pk)"),
		})
		if err != nil {
			if e, ok := err.(awserr.Error); ok && e.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				continue // chunk already exists, fine
			}
			return fmt.Errorf("seed chunk %d: %w", idx, err)
		}
	}

	// Seed cursor row. ConditionExpression makes re-runs safe.
	_, err := api.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(claimsTable),
		Item: map[string]*dynamodb.AttributeValue{
			"pk":       {S: aws.String(cursorRowID)},
			"total":    {N: aws.String(strconv.FormatUint(totalChunks, 10))},
			"next_idx": {N: aws.String("0")},
		},
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
	})
	if err != nil {
		if e, ok := err.(awserr.Error); !ok || e.Code() != dynamodb.ErrCodeConditionalCheckFailedException {
			return fmt.Errorf("seed cursor: %w", err)
		}
		// Cursor exists. If the previous run had a different total, fail loud —
		// resuming with a different chunking would corrupt progress.
		out, gerr := api.GetItemWithContext(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(claimsTable),
			Key:       map[string]*dynamodb.AttributeValue{"pk": {S: aws.String(cursorRowID)}},
		})
		if gerr != nil {
			return fmt.Errorf("read existing cursor: %w", gerr)
		}
		if existing := out.Item["total"]; existing != nil && existing.N != nil {
			n, _ := strconv.ParseUint(*existing.N, 10, 64)
			if n != totalChunks {
				return fmt.Errorf("cursor.total mismatch: existing=%d, new=%d — drop %s and re-run setup", n, totalChunks, claimsTable)
			}
		}
	}

	return nil
}

// createTableIfMissing creates a single-pk on-demand DDB table.
func createTableIfMissing(ctx context.Context, api dynamodbiface.DynamoDBAPI, name string) error {
	_, err := api.DescribeTableWithContext(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
	if err == nil {
		return nil
	}
	if e, ok := err.(awserr.Error); !ok || e.Code() != dynamodb.ErrCodeResourceNotFoundException {
		return err
	}
	_, err = api.CreateTableWithContext(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(name),
		BillingMode: aws.String(dynamodb.BillingModePayPerRequest),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: aws.String(dynamodb.KeyTypeHash)},
		},
		Tags: []*dynamodb.Tag{
			{Key: aws.String("Purpose"), Value: aws.String("replay-coordinator-throwaway")},
		},
	})
	return err
}

// waitForActive blocks until a table reaches ACTIVE status.
func waitForActive(ctx context.Context, api dynamodbiface.DynamoDBAPI, name string) error {
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		out, err := api.DescribeTableWithContext(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
		if err == nil && out.Table != nil && aws.StringValue(out.Table.TableStatus) == dynamodb.TableStatusActive {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
	return fmt.Errorf("table %s not ACTIVE after 2 minutes", name)
}

// TeardownDDB deletes the two coordinator tables. Use after a successful
// replay, or to reset before re-running with a different range.
func TeardownDDB(ctx context.Context, api dynamodbiface.DynamoDBAPI, prefix string) error {
	for _, name := range []string{prefix + "-claims", prefix + "-tx"} {
		_, err := api.DeleteTableWithContext(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(name)})
		if err != nil {
			if e, ok := err.(awserr.Error); !ok || e.Code() != dynamodb.ErrCodeResourceNotFoundException {
				return fmt.Errorf("delete %s: %w", name, err)
			}
		}
	}
	return nil
}
