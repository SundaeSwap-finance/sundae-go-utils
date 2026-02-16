package connectiondao

import "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

// Build creates a new connections DAO using the standard table name for the
// given environment.
func Build(api dynamodbiface.DynamoDBAPI, env string) *DAO {
	return New(api, TableName(env))
}

// TableName returns the DynamoDB table name for the given environment.
func TableName(env string) string {
	return env + "-sundae-api--ws-connections"
}
