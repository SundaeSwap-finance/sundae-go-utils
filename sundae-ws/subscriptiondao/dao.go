package subscriptiondao

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/savaki/ddb"
)

// DAO provides access to the WebSocket subscriptions table.
type DAO struct {
	table     *ddb.Table
	api       dynamodbiface.DynamoDBAPI
	tableName string
}

// New creates a new subscriptions DAO.
func New(api dynamodbiface.DynamoDBAPI, tableName string) *DAO {
	return &DAO{
		table:     ddb.New(api).MustTable(tableName, Subscription{}),
		api:       api,
		tableName: tableName,
	}
}

// Put stores a subscription record.
func (d *DAO) Put(ctx context.Context, sub Subscription) error {
	return d.table.Put(sub).RunWithContext(ctx)
}

// Delete removes a subscription record by ID.
func (d *DAO) Delete(ctx context.Context, subscriptionID string) error {
	return d.table.Delete(subscriptionID).RunWithContext(ctx)
}

// QueryByTopic returns all subscriptions for a given topic using the TopicIndex GSI.
func (d *DAO) QueryByTopic(ctx context.Context, topic string) ([]Subscription, error) {
	var subs []Subscription
	err := d.table.Query("#Topic = ?", topic).
		IndexName("TopicIndex").
		FindAllWithContext(ctx, &subs)
	if err != nil {
		return nil, fmt.Errorf("failed to query subscriptions by topic %v: %w", topic, err)
	}
	return subs, nil
}

// QueryByConnection returns all subscriptions for a given connection using the ConnectionIndex GSI.
func (d *DAO) QueryByConnection(ctx context.Context, connectionID string) ([]Subscription, error) {
	var subs []Subscription
	err := d.table.Query("#ConnectionID = ?", connectionID).
		IndexName("ConnectionIndex").
		FindAllWithContext(ctx, &subs)
	if err != nil {
		return nil, fmt.Errorf("failed to query subscriptions by connection %v: %w", connectionID, err)
	}
	return subs, nil
}

// DeleteByConnection removes all subscriptions for a given connection.
func (d *DAO) DeleteByConnection(ctx context.Context, connectionID string) error {
	subs, err := d.QueryByConnection(ctx, connectionID)
	if err != nil {
		return err
	}

	// Batch delete in chunks of 25 (DynamoDB limit)
	const batchSize = 25
	for i := 0; i < len(subs); i += batchSize {
		end := i + batchSize
		if end > len(subs) {
			end = len(subs)
		}
		chunk := subs[i:end]

		writeRequests := make([]*dynamodb.WriteRequest, len(chunk))
		for j, sub := range chunk {
			key, err := dynamodbattribute.MarshalMap(map[string]string{"pk": sub.SubscriptionID})
			if err != nil {
				return fmt.Errorf("failed to marshal key for subscription %v: %w", sub.SubscriptionID, err)
			}
			writeRequests[j] = &dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{Key: key},
			}
		}

		unprocessed := map[string][]*dynamodb.WriteRequest{
			d.tableName: writeRequests,
		}

		const maxRetries = 5
		for attempt := 0; attempt < maxRetries; attempt++ {
			output, err := d.api.BatchWriteItemWithContext(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: unprocessed,
			})
			if err != nil {
				return fmt.Errorf("failed to batch delete subscriptions for connection %v: %w", connectionID, err)
			}
			if len(output.UnprocessedItems) == 0 {
				break
			}
			unprocessed = output.UnprocessedItems
			if attempt < maxRetries-1 {
				backoff := time.Duration(1<<attempt) * 100 * time.Millisecond
				timer := time.NewTimer(backoff)
				select {
				case <-ctx.Done():
					timer.Stop()
					return fmt.Errorf("context cancelled during retry for connection %v: %w", connectionID, ctx.Err())
				case <-timer.C:
				}
			} else {
				return fmt.Errorf("failed to delete all subscriptions for connection %v: %d items unprocessed after %d retries", connectionID, len(unprocessed[d.tableName]), maxRetries)
			}
		}
	}

	return nil
}

// Count returns the number of subscriptions for a given topic.
func (d *DAO) Count(ctx context.Context, topic string) (int64, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(d.tableName),
		IndexName:              aws.String("TopicIndex"),
		KeyConditionExpression: aws.String("topic = :topic"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":topic": {S: aws.String(topic)},
		},
		Select: aws.String("COUNT"),
	}

	output, err := d.api.QueryWithContext(ctx, input)
	if err != nil {
		return 0, fmt.Errorf("failed to count subscriptions for topic %v: %w", topic, err)
	}

	return *output.Count, nil
}
