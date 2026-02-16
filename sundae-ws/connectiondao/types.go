package connectiondao

// Connection represents a WebSocket connection stored in DynamoDB.
type Connection struct {
	ConnectionID string `dynamodbav:"pk" ddb:"hash"`
	Endpoint     string `dynamodbav:"endpoint"`
	ConnectedAt  int64  `dynamodbav:"connected_at"`
	TTL          int64  `dynamodbav:"ttl"`
}
