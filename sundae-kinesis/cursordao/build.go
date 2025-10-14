package cursordao

import (
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Build protocol dao pointing to local db
func Build(api dynamodbiface.DynamoDBAPI, env string) *DAO {
	return New(api, TableName(env))
}

func TableName(env string) string {
	tableName := env + "-sundae-sync--cursor"
	return tableName
}
