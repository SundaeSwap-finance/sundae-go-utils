package cursordao

import (
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Build protocol dao pointing to local db
func Build(api dynamodbiface.DynamoDBAPI, network string) *DAO {
	return New(api, TableName(network))
}

func TableName(network string) string {
	tableName := network + "-sundae-sync--cursor"
	return tableName
}
