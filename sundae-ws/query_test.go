package sundaews

import (
	"testing"

	"github.com/tj/assert"
)

func TestSimpleExtractSubscriptionField(t *testing.T) {
	t.Run("basic subscription", func(t *testing.T) {
		field, args, err := SimpleExtractSubscriptionField(SubscribePayload{
			Query:     `subscription { poolUpdated(id: "abc") { poolId quantityA } }`,
			Variables: map[string]interface{}{"id": "abc"},
		})
		assert.NoError(t, err)
		assert.Equal(t, "poolUpdated", field)
		assert.Equal(t, "abc", args["id"])
	})

	t.Run("named subscription", func(t *testing.T) {
		field, _, err := SimpleExtractSubscriptionField(SubscribePayload{
			Query: `subscription WatchPool { heartbeat { timestamp } }`,
		})
		assert.NoError(t, err)
		assert.Equal(t, "heartbeat", field)
	})

	t.Run("implicit subscription (just braces)", func(t *testing.T) {
		field, _, err := SimpleExtractSubscriptionField(SubscribePayload{
			Query: `{ heartbeat { timestamp } }`,
		})
		assert.NoError(t, err)
		assert.Equal(t, "heartbeat", field)
	})

	t.Run("with variables", func(t *testing.T) {
		field, args, err := SimpleExtractSubscriptionField(SubscribePayload{
			Query:     `subscription($id: ID!) { poolUpdated(id: $id) { poolId } }`,
			Variables: map[string]interface{}{"id": "pool123"},
		})
		assert.NoError(t, err)
		assert.Equal(t, "poolUpdated", field)
		assert.Equal(t, "pool123", args["id"])
	})

	t.Run("empty query fails", func(t *testing.T) {
		_, _, err := SimpleExtractSubscriptionField(SubscribePayload{Query: ""})
		assert.Error(t, err)
	})
}

func TestProtocol(t *testing.T) {
	t.Run("ParseMessage", func(t *testing.T) {
		msg, err := ParseMessage(`{"type":"connection_init"}`)
		assert.NoError(t, err)
		assert.Equal(t, MsgConnectionInit, msg.Type)
	})

	t.Run("ParseMessage missing type", func(t *testing.T) {
		_, err := ParseMessage(`{"id":"1"}`)
		assert.Error(t, err)
	})

	t.Run("AckMessage", func(t *testing.T) {
		ack := AckMessage()
		msg, err := ParseMessage(string(ack))
		assert.NoError(t, err)
		assert.Equal(t, MsgConnectionAck, msg.Type)
	})

	t.Run("NextMessage", func(t *testing.T) {
		data, err := NextMessage("1", map[string]string{"poolId": "abc"})
		assert.NoError(t, err)
		msg, err := ParseMessage(string(data))
		assert.NoError(t, err)
		assert.Equal(t, MsgNext, msg.Type)
		assert.Equal(t, "1", msg.ID)
	})

	t.Run("ErrorMessage", func(t *testing.T) {
		errMsg := ErrorMessage("1", "something went wrong")
		msg, err := ParseMessage(string(errMsg))
		assert.NoError(t, err)
		assert.Equal(t, MsgError, msg.Type)
		assert.Equal(t, "1", msg.ID)
	})
}
