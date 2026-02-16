package sundaews

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/SundaeSwap-finance/sundae-go-utils/sundae-ws/connectiondao"
	"github.com/SundaeSwap-finance/sundae-go-utils/sundae-ws/subscriptiondao"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/apigatewaymanagementapi"
	"github.com/aws/aws-sdk-go/service/apigatewaymanagementapi/apigatewaymanagementapiiface"
	"github.com/rs/zerolog"
)

// TopicResolver resolves a subscription field name and arguments to a topic string.
type TopicResolver interface {
	ComputeTopic(fieldName string, args map[string]interface{}) (string, error)
	ValidateField(fieldName string) error
}

// Handler handles WebSocket API Gateway events for the graphql-ws protocol.
type Handler struct {
	Connections  *connectiondao.DAO
	Subs         *subscriptiondao.DAO
	Topics       TopicResolver
	ExtractField SubscriptionFieldExtractor
	Logger       zerolog.Logger
	ConnTTL      time.Duration // TTL for connection records (default 2 hours)

	mgmtMu      sync.RWMutex
	mgmtClients map[string]apigatewaymanagementapiiface.ApiGatewayManagementApiAPI
}

// NewHandler creates a Handler with all required dependencies.
func NewHandler(
	connections *connectiondao.DAO,
	subs *subscriptiondao.DAO,
	topics TopicResolver,
	logger zerolog.Logger,
) *Handler {
	return &Handler{
		Connections:  connections,
		Subs:         subs,
		Topics:       topics,
		ExtractField: SimpleExtractSubscriptionField,
		Logger:       logger,
		ConnTTL:      2 * time.Hour,
		mgmtClients:  make(map[string]apigatewaymanagementapiiface.ApiGatewayManagementApiAPI),
	}
}

// HandleEvent routes an API Gateway WebSocket event to the appropriate handler.
func (h *Handler) HandleEvent(ctx context.Context, req events.APIGatewayWebsocketProxyRequest) (events.APIGatewayProxyResponse, error) {
	logger := h.Logger.With().
		Str("connection_id", req.RequestContext.ConnectionID).
		Str("route", req.RequestContext.RouteKey).
		Logger()

	switch req.RequestContext.RouteKey {
	case "$connect":
		return h.handleConnect(ctx, logger, req)
	case "$disconnect":
		return h.handleDisconnect(ctx, logger, req)
	case "$default":
		return h.handleMessage(ctx, logger, req)
	default:
		logger.Warn().Str("route", req.RequestContext.RouteKey).Msg("unknown route")
		return events.APIGatewayProxyResponse{StatusCode: 400}, nil
	}
}

func (h *Handler) handleConnect(ctx context.Context, logger zerolog.Logger, req events.APIGatewayWebsocketProxyRequest) (events.APIGatewayProxyResponse, error) {
	connID := req.RequestContext.ConnectionID
	endpoint := fmt.Sprintf("https://%s/%s", req.RequestContext.DomainName, req.RequestContext.Stage)

	ttl := h.ConnTTL
	if ttl == 0 {
		ttl = 2 * time.Hour
	}

	conn := connectiondao.Connection{
		ConnectionID: connID,
		Endpoint:     endpoint,
		ConnectedAt:  time.Now().Unix(),
		TTL:          time.Now().Add(ttl).Unix(),
	}

	if err := h.Connections.Put(ctx, conn); err != nil {
		logger.Error().Err(err).Msg("failed to store connection")
		return events.APIGatewayProxyResponse{StatusCode: 500}, nil
	}

	logger.Info().Msg("connection established")
	return events.APIGatewayProxyResponse{StatusCode: 200}, nil
}

func (h *Handler) handleDisconnect(ctx context.Context, logger zerolog.Logger, req events.APIGatewayWebsocketProxyRequest) (events.APIGatewayProxyResponse, error) {
	connID := req.RequestContext.ConnectionID

	if err := h.Subs.DeleteByConnection(ctx, connID); err != nil {
		logger.Error().Err(err).Msg("failed to delete subscriptions")
	}

	if err := h.Connections.Delete(ctx, connID); err != nil {
		logger.Error().Err(err).Msg("failed to delete connection")
	}

	logger.Info().Msg("connection closed")
	return events.APIGatewayProxyResponse{StatusCode: 200}, nil
}

func (h *Handler) handleMessage(ctx context.Context, logger zerolog.Logger, req events.APIGatewayWebsocketProxyRequest) (events.APIGatewayProxyResponse, error) {
	msg, err := ParseMessage(req.Body)
	if err != nil {
		logger.Warn().Err(err).Msg("invalid message")
		return events.APIGatewayProxyResponse{StatusCode: 400}, nil
	}

	connID := req.RequestContext.ConnectionID
	endpoint := fmt.Sprintf("https://%s/%s", req.RequestContext.DomainName, req.RequestContext.Stage)

	switch msg.Type {
	case MsgConnectionInit:
		return h.handleConnectionInit(ctx, logger, connID, endpoint)
	case MsgSubscribe:
		return h.handleSubscribe(ctx, logger, connID, endpoint, msg)
	case MsgComplete:
		return h.handleComplete(ctx, logger, connID, msg)
	case MsgPing:
		if err := h.postToConnection(ctx, endpoint, connID, PongMessage()); err != nil {
			logger.Error().Err(err).Msg("failed to send pong")
		}
		return events.APIGatewayProxyResponse{StatusCode: 200}, nil
	default:
		logger.Warn().Str("type", msg.Type).Msg("unhandled message type")
		return events.APIGatewayProxyResponse{StatusCode: 200}, nil
	}
}

func (h *Handler) handleConnectionInit(ctx context.Context, logger zerolog.Logger, connID, endpoint string) (events.APIGatewayProxyResponse, error) {
	if err := h.postToConnection(ctx, endpoint, connID, AckMessage()); err != nil {
		logger.Error().Err(err).Msg("failed to send connection_ack")
		return events.APIGatewayProxyResponse{StatusCode: 500}, nil
	}
	logger.Debug().Msg("connection_ack sent")
	return events.APIGatewayProxyResponse{StatusCode: 200}, nil
}

func (h *Handler) handleSubscribe(ctx context.Context, logger zerolog.Logger, connID, endpoint string, msg *GraphQLWSMessage) (events.APIGatewayProxyResponse, error) {
	var payload SubscribePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		logger.Warn().Err(err).Msg("invalid subscribe payload")
		if sendErr := h.postToConnection(ctx, endpoint, connID, ErrorMessage(msg.ID, "invalid subscribe payload")); sendErr != nil {
			logger.Error().Err(sendErr).Msg("failed to send error")
		}
		return events.APIGatewayProxyResponse{StatusCode: 200}, nil
	}

	// Extract the subscription field name and args from the query.
	extractor := h.ExtractField
	if extractor == nil {
		extractor = SimpleExtractSubscriptionField
	}
	fieldName, args, err := extractor(payload)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to extract subscription field")
		if sendErr := h.postToConnection(ctx, endpoint, connID, ErrorMessage(msg.ID, err.Error())); sendErr != nil {
			logger.Error().Err(sendErr).Msg("failed to send error")
		}
		return events.APIGatewayProxyResponse{StatusCode: 200}, nil
	}

	// Validate the field exists
	if err := h.Topics.ValidateField(fieldName); err != nil {
		logger.Warn().Err(err).Str("field", fieldName).Msg("unknown subscription field")
		if sendErr := h.postToConnection(ctx, endpoint, connID, ErrorMessage(msg.ID, err.Error())); sendErr != nil {
			logger.Error().Err(sendErr).Msg("failed to send error")
		}
		return events.APIGatewayProxyResponse{StatusCode: 200}, nil
	}

	// Compute the topic
	topic, err := h.Topics.ComputeTopic(fieldName, args)
	if err != nil {
		logger.Warn().Err(err).Str("field", fieldName).Msg("failed to compute topic")
		if sendErr := h.postToConnection(ctx, endpoint, connID, ErrorMessage(msg.ID, err.Error())); sendErr != nil {
			logger.Error().Err(sendErr).Msg("failed to send error")
		}
		return events.APIGatewayProxyResponse{StatusCode: 200}, nil
	}

	ttl := h.ConnTTL
	if ttl == 0 {
		ttl = 2 * time.Hour
	}

	sub := subscriptiondao.Subscription{
		SubscriptionID: connID + "#" + msg.ID,
		ConnectionID:   connID,
		Topic:          topic,
		Endpoint:       endpoint,
		ClientSubID:    msg.ID,
		TTL:            time.Now().Add(ttl).Unix(),
	}

	if err := h.Subs.Put(ctx, sub); err != nil {
		logger.Error().Err(err).Msg("failed to store subscription")
		if sendErr := h.postToConnection(ctx, endpoint, connID, ErrorMessage(msg.ID, "internal error")); sendErr != nil {
			logger.Error().Err(sendErr).Msg("failed to send error")
		}
		return events.APIGatewayProxyResponse{StatusCode: 500}, nil
	}

	logger.Info().
		Str("sub_id", msg.ID).
		Str("field", fieldName).
		Str("topic", topic).
		Msg("subscription created")

	return events.APIGatewayProxyResponse{StatusCode: 200}, nil
}

func (h *Handler) handleComplete(ctx context.Context, logger zerolog.Logger, connID string, msg *GraphQLWSMessage) (events.APIGatewayProxyResponse, error) {
	subID := connID + "#" + msg.ID
	if err := h.Subs.Delete(ctx, subID); err != nil {
		logger.Error().Err(err).Str("sub_id", msg.ID).Msg("failed to delete subscription")
	}
	logger.Info().Str("sub_id", msg.ID).Msg("subscription completed")
	return events.APIGatewayProxyResponse{StatusCode: 200}, nil
}

func (h *Handler) postToConnection(ctx context.Context, endpoint, connID string, data []byte) error {
	client := h.getManagementClient(endpoint)
	_, err := client.PostToConnectionWithContext(ctx, &apigatewaymanagementapi.PostToConnectionInput{
		ConnectionId: aws.String(connID),
		Data:         data,
	})
	return err
}

func (h *Handler) getManagementClient(endpoint string) apigatewaymanagementapiiface.ApiGatewayManagementApiAPI {
	h.mgmtMu.RLock()
	if client, ok := h.mgmtClients[endpoint]; ok {
		h.mgmtMu.RUnlock()
		return client
	}
	h.mgmtMu.RUnlock()

	h.mgmtMu.Lock()
	defer h.mgmtMu.Unlock()

	if client, ok := h.mgmtClients[endpoint]; ok {
		return client
	}

	if h.mgmtClients == nil {
		h.mgmtClients = make(map[string]apigatewaymanagementapiiface.ApiGatewayManagementApiAPI)
	}

	sess := session.Must(session.NewSession(aws.NewConfig().WithEndpoint(endpoint)))
	client := apigatewaymanagementapi.New(sess)
	h.mgmtClients[endpoint] = client
	return client
}
