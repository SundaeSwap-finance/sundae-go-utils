package sundaews

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/SundaeSwap-finance/sundae-go-utils/sundae-ws/connectiondao"
	"github.com/SundaeSwap-finance/sundae-go-utils/sundae-ws/subscriptiondao"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/apigatewaymanagementapi"
	"github.com/aws/aws-sdk-go/service/apigatewaymanagementapi/apigatewaymanagementapiiface"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"

	"github.com/SundaeSwap-finance/sundae-go-utils/sundae-ws/publish"
)

// Dispatcher fans out Kinesis events to WebSocket subscribers.
type Dispatcher struct {
	Connections *connectiondao.DAO
	Subs        *subscriptiondao.DAO
	Logger      zerolog.Logger
	Concurrency int // max concurrent PostToConnection calls (default 50)

	// mgmtClients caches API Gateway Management API clients by endpoint
	mgmtMu      sync.RWMutex
	mgmtClients map[string]apigatewaymanagementapiiface.ApiGatewayManagementApiAPI
}

// HandleKinesisEvent processes a batch of Kinesis records and fans out events
// to matching WebSocket subscribers.
func (d *Dispatcher) HandleKinesisEvent(ctx context.Context, event events.KinesisEvent) error {
	for _, record := range event.Records {
		if err := d.processRecord(ctx, record); err != nil {
			d.Logger.Error().Err(err).
				Str("event_id", record.EventID).
				Msg("failed to process kinesis record")
			// Continue processing other records rather than failing the whole batch
		}
	}
	return nil
}

func (d *Dispatcher) processRecord(ctx context.Context, record events.KinesisEventRecord) error {
	var envelope publish.Envelope
	if err := json.Unmarshal(record.Kinesis.Data, &envelope); err != nil {
		return fmt.Errorf("unmarshalling kinesis record: %w", err)
	}

	if envelope.Topic == "" {
		d.Logger.Warn().Msg("kinesis record has empty topic, skipping")
		return nil
	}

	subs, err := d.Subs.QueryByTopic(ctx, envelope.Topic)
	if err != nil {
		return fmt.Errorf("querying subscriptions for topic %v: %w", envelope.Topic, err)
	}

	if len(subs) == 0 {
		return nil
	}

	d.Logger.Debug().
		Str("topic", envelope.Topic).
		Int("subscribers", len(subs)).
		Msg("dispatching event")

	concurrency := d.Concurrency
	if concurrency <= 0 {
		concurrency = 50
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for _, sub := range subs {
		sub := sub
		g.Go(func() error {
			return d.sendToSubscriber(ctx, sub, envelope.Payload)
		})
	}

	return g.Wait()
}

func (d *Dispatcher) sendToSubscriber(ctx context.Context, sub subscriptiondao.Subscription, payload json.RawMessage) error {
	msg, err := NextMessage(sub.ClientSubID, json.RawMessage(payload))
	if err != nil {
		return fmt.Errorf("building next message: %w", err)
	}

	client := d.getManagementClient(sub.Endpoint)
	_, sendErr := client.PostToConnectionWithContext(ctx, &apigatewaymanagementapi.PostToConnectionInput{
		ConnectionId: aws.String(sub.ConnectionID),
		Data:         msg,
	})

	if sendErr != nil {
		if isGoneException(sendErr) {
			d.Logger.Info().
				Str("connection_id", sub.ConnectionID).
				Msg("connection gone, cleaning up")
			d.cleanupConnection(ctx, sub.ConnectionID)
			return nil
		}
		return fmt.Errorf("posting to connection %v: %w", sub.ConnectionID, sendErr)
	}

	return nil
}

func (d *Dispatcher) cleanupConnection(ctx context.Context, connID string) {
	if err := d.Subs.DeleteByConnection(ctx, connID); err != nil {
		d.Logger.Error().Err(err).Str("connection_id", connID).Msg("failed to delete subscriptions for gone connection")
	}
	if err := d.Connections.Delete(ctx, connID); err != nil {
		d.Logger.Error().Err(err).Str("connection_id", connID).Msg("failed to delete gone connection")
	}
}

func (d *Dispatcher) getManagementClient(endpoint string) apigatewaymanagementapiiface.ApiGatewayManagementApiAPI {
	d.mgmtMu.RLock()
	if client, ok := d.mgmtClients[endpoint]; ok {
		d.mgmtMu.RUnlock()
		return client
	}
	d.mgmtMu.RUnlock()

	d.mgmtMu.Lock()
	defer d.mgmtMu.Unlock()

	// Double-check after acquiring write lock
	if client, ok := d.mgmtClients[endpoint]; ok {
		return client
	}

	if d.mgmtClients == nil {
		d.mgmtClients = make(map[string]apigatewaymanagementapiiface.ApiGatewayManagementApiAPI)
	}

	sess := session.Must(session.NewSession(aws.NewConfig().WithEndpoint(endpoint)))
	client := apigatewaymanagementapi.New(sess)
	d.mgmtClients[endpoint] = client
	return client
}

// isGoneException checks if the error is a GoneException (HTTP 410),
// indicating the WebSocket connection no longer exists.
func isGoneException(err error) bool {
	return strings.Contains(err.Error(), "GoneException") ||
		strings.Contains(err.Error(), "410")
}
