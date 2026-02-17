package latestdao

// Latest holds the most recently published payload for a topic.
// The ws-handler reads this on subscribe to send an initial message.
type Latest struct {
	Topic     string `dynamodbav:"pk" ddb:"hash"`
	Payload   string `dynamodbav:"payload"`    // JSON-encoded payload
	MessageID string `dynamodbav:"message_id"` // idempotency key
	TTL       int64  `dynamodbav:"ttl"`
}
