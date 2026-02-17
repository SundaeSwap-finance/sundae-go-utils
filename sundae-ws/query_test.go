package sundaews

import (
	"testing"

	"github.com/tj/assert"
)

func TestSimpleExtractSubscriptionField(t *testing.T) {
	t.Run("basic subscription with variables", func(t *testing.T) {
		field, args, err := SimpleExtractSubscriptionField(SubscribePayload{
			Query:     `subscription($id: ID!) { poolUpdated(id: $id) { poolId quantityA } }`,
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

	t.Run("inline string argument", func(t *testing.T) {
		field, args, err := SimpleExtractSubscriptionField(SubscribePayload{
			Query: `subscription { poolUpdated(poolId: "abc123") { tvl } }`,
		})
		assert.NoError(t, err)
		assert.Equal(t, "poolUpdated", field)
		assert.Equal(t, "abc123", args["poolId"])
	})

	t.Run("inline number argument", func(t *testing.T) {
		field, args, err := SimpleExtractSubscriptionField(SubscribePayload{
			Query: `subscription { poolUpdated(limit: 10) { tvl } }`,
		})
		assert.NoError(t, err)
		assert.Equal(t, "poolUpdated", field)
		assert.Equal(t, int64(10), args["limit"])
	})

	t.Run("inline boolean argument", func(t *testing.T) {
		field, args, err := SimpleExtractSubscriptionField(SubscribePayload{
			Query: `subscription { poolUpdated(active: true) { tvl } }`,
		})
		assert.NoError(t, err)
		assert.Equal(t, "poolUpdated", field)
		assert.Equal(t, true, args["active"])
	})

	t.Run("inline list argument", func(t *testing.T) {
		field, args, err := SimpleExtractSubscriptionField(SubscribePayload{
			Query: `subscription { poolUpdated(pools: ["poolA", "poolB", "poolC"]) { tvl } }`,
		})
		assert.NoError(t, err)
		assert.Equal(t, "poolUpdated", field)
		assert.Equal(t, []interface{}{"poolA", "poolB", "poolC"}, args["pools"])
	})

	t.Run("multiple inline arguments", func(t *testing.T) {
		field, args, err := SimpleExtractSubscriptionField(SubscribePayload{
			Query: `subscription { orders(poolId: "abc", status: FILLED) { id } }`,
		})
		assert.NoError(t, err)
		assert.Equal(t, "orders", field)
		assert.Equal(t, "abc", args["poolId"])
		assert.Equal(t, "FILLED", args["status"])
	})

	t.Run("inline args override variables", func(t *testing.T) {
		field, args, err := SimpleExtractSubscriptionField(SubscribePayload{
			Query:     `subscription { poolUpdated(poolId: "inline") { tvl } }`,
			Variables: map[string]interface{}{"poolId": "from-variables"},
		})
		assert.NoError(t, err)
		assert.Equal(t, "poolUpdated", field)
		assert.Equal(t, "inline", args["poolId"])
	})

	t.Run("variable reference resolved", func(t *testing.T) {
		field, args, err := SimpleExtractSubscriptionField(SubscribePayload{
			Query:     `subscription { poolUpdated(poolId: $pid) { tvl } }`,
			Variables: map[string]interface{}{"pid": "resolved-value"},
		})
		assert.NoError(t, err)
		assert.Equal(t, "poolUpdated", field)
		assert.Equal(t, "resolved-value", args["poolId"])
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

	t.Run("NextMessage without messageID", func(t *testing.T) {
		data, err := NextMessage("1", map[string]string{"poolId": "abc"}, "")
		assert.NoError(t, err)
		msg, err := ParseMessage(string(data))
		assert.NoError(t, err)
		assert.Equal(t, MsgNext, msg.Type)
		assert.Equal(t, "1", msg.ID)
		// Payload should have data but no extensions
		assert.Contains(t, string(msg.Payload), `"data"`)
		assert.NotContains(t, string(msg.Payload), `"extensions"`)
	})

	t.Run("NextMessage with messageID", func(t *testing.T) {
		data, err := NextMessage("1", map[string]string{"poolId": "abc"}, "msg-123")
		assert.NoError(t, err)
		msg, err := ParseMessage(string(data))
		assert.NoError(t, err)
		assert.Equal(t, MsgNext, msg.Type)
		assert.Equal(t, "1", msg.ID)
		assert.Contains(t, string(msg.Payload), `"messageId":"msg-123"`)
	})

	t.Run("ErrorMessage", func(t *testing.T) {
		errMsg := ErrorMessage("1", "something went wrong")
		msg, err := ParseMessage(string(errMsg))
		assert.NoError(t, err)
		assert.Equal(t, MsgError, msg.Type)
		assert.Equal(t, "1", msg.ID)
	})
}
