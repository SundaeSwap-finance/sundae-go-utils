package sundaews

import (
	"encoding/json"
	"fmt"
)

// graphql-ws protocol message types
// See: https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md
const (
	MsgConnectionInit = "connection_init"
	MsgConnectionAck  = "connection_ack"
	MsgPing           = "ping"
	MsgPong           = "pong"
	MsgSubscribe      = "subscribe"
	MsgNext           = "next"
	MsgError          = "error"
	MsgComplete       = "complete"
)

// GraphQLWSMessage is a message in the graphql-ws protocol.
type GraphQLWSMessage struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// SubscribePayload is the payload of a "subscribe" message.
type SubscribePayload struct {
	Query         string                 `json:"query"`
	Variables     map[string]interface{} `json:"variables,omitempty"`
	OperationName string                `json:"operationName,omitempty"`
}

// ParseMessage parses a graphql-ws protocol message from a JSON string.
func ParseMessage(body string) (*GraphQLWSMessage, error) {
	var msg GraphQLWSMessage
	if err := json.Unmarshal([]byte(body), &msg); err != nil {
		return nil, fmt.Errorf("invalid graphql-ws message: %w", err)
	}
	if msg.Type == "" {
		return nil, fmt.Errorf("missing message type")
	}
	return &msg, nil
}

// AckMessage returns a connection_ack message.
func AckMessage() []byte {
	b, _ := json.Marshal(GraphQLWSMessage{Type: MsgConnectionAck})
	return b
}

// PongMessage returns a pong message.
func PongMessage() []byte {
	b, _ := json.Marshal(GraphQLWSMessage{Type: MsgPong})
	return b
}

// NextMessage returns a "next" message with the given subscription ID and payload.
func NextMessage(id string, payload interface{}) ([]byte, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshalling next payload: %w", err)
	}
	b, err := json.Marshal(GraphQLWSMessage{
		ID:      id,
		Type:    MsgNext,
		Payload: payloadBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("marshalling next message: %w", err)
	}
	return b, nil
}

// ErrorMessage returns an "error" message with the given subscription ID and error.
func ErrorMessage(id string, errMsg string) []byte {
	payload, _ := json.Marshal([]map[string]string{{"message": errMsg}})
	b, _ := json.Marshal(GraphQLWSMessage{
		ID:      id,
		Type:    MsgError,
		Payload: payload,
	})
	return b
}

// CompleteMessage returns a "complete" message for the given subscription ID.
func CompleteMessage(id string) []byte {
	b, _ := json.Marshal(GraphQLWSMessage{ID: id, Type: MsgComplete})
	return b
}
