package txdao

import (
	"os"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/rs/zerolog"
)

func Build(api dynamodbiface.DynamoDBAPI) *DAO {
	return New(api, TableName(sundaecli.CommonOpts.Network), zerolog.New(os.Stdout), sundaecli.CommonOpts.Dry)
}

func TableName(network string) string {
	tableName := network + "-sundae-sync-v2--lookup"
	return tableName
}
