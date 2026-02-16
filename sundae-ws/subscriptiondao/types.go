package subscriptiondao

// Subscription represents a WebSocket GraphQL subscription stored in DynamoDB.
// SubscriptionID is "{connectionId}#{clientSubscriptionId}".
type Subscription struct {
	SubscriptionID string `dynamodbav:"pk" ddb:"hash"`
	ConnectionID   string `dynamodbav:"connection_id" ddb:"gsi_hash:ConnectionIndex"`
	Topic          string `dynamodbav:"topic" ddb:"gsi_hash:TopicIndex"`
	Endpoint       string `dynamodbav:"endpoint"`
	ClientSubID    string `dynamodbav:"client_sub_id"`
	TTL            int64  `dynamodbav:"ttl"`
}
