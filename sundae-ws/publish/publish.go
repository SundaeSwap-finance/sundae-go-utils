package publish

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

// Envelope is the message format published to the WebSocket events stream.
// MessageID is a caller-provided idempotency key (e.g., transaction hash,
// slot+pool ID) that is passed through to WebSocket clients for deduplication.
type Envelope struct {
	Topic     string          `json:"topic"`
	MessageID string          `json:"messageId"`
	Payload   json.RawMessage `json:"payload"`
}

// Publisher publishes events to the WebSocket Kinesis stream.
type Publisher struct {
	client     kinesisiface.KinesisAPI
	streamName string
}

// New creates a new Publisher.
func New(client kinesisiface.KinesisAPI, streamName string) *Publisher {
	return &Publisher{
		client:     client,
		streamName: streamName,
	}
}

// Build creates a new Publisher using the standard stream name for the given
// environment.
func Build(env string) *Publisher {
	sess := session.Must(session.NewSession(aws.NewConfig()))
	client := kinesis.New(sess)
	return New(client, StreamName(env))
}

// StreamName returns the Kinesis stream name for the given environment.
func StreamName(env string) string {
	return env + "-sundae-ws-events"
}

// Send publishes an event to the WebSocket events stream. The messageID should
// be a stable idempotency key derived from the event's natural identity (e.g.,
// transaction hash, slot number + pool ID) so that retries produce the same ID.
// The topic is used as the Kinesis partition key to preserve ordering within a
// topic.
func (p *Publisher) Send(ctx context.Context, topic string, messageID string, payload interface{}) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshalling payload: %w", err)
	}

	envelope := Envelope{
		Topic:     topic,
		MessageID: messageID,
		Payload:   payloadBytes,
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshalling envelope: %w", err)
	}

	_, err = p.client.PutRecordWithContext(ctx, &kinesis.PutRecordInput{
		StreamName:   aws.String(p.streamName),
		PartitionKey: aws.String(topic),
		Data:         data,
	})
	if err != nil {
		return fmt.Errorf("publishing to kinesis stream %v: %w", p.streamName, err)
	}

	return nil
}
