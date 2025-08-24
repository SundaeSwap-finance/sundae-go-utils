package txdao

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/rs/zerolog"
	"github.com/savaki/ddb"
)

type DAO struct {
	logger zerolog.Logger
	table  *ddb.Table
	cache  map[string]Tx
	dry    bool
}

func New(api dynamodbiface.DynamoDBAPI, tableName string, logger zerolog.Logger, dry bool) *DAO {
	return &DAO{table: ddb.New(api).MustTable(tableName, &Tx{}), logger: logger, cache: make(map[string]Tx), dry: dry}
}

func (dao *DAO) Get(ctx context.Context, hash string) (Tx, error) {
	if tx, ok := dao.cache[hash]; ok {
		return tx, nil
	}
	var tx Tx
	err := dao.table.Get(fmt.Sprintf("tx:%v", hash)).Range("tx").ScanWithContext(ctx, &tx)
	if err != nil {
		return Tx{}, fmt.Errorf("failed to fetch transaction: %w", err)
	}
	dao.cache[hash] = tx
	return tx, err
}

func (dao *DAO) GetOutput(ctx context.Context, txId string, idx int) (UTxO, error) {
	tx, err := dao.Get(ctx, txId)
	if err != nil {
		return UTxO{}, err
	}
	if tx.Successful {
		if idx < len(tx.Utxos) {
			return tx.Utxos[idx], nil
		} else {
			return UTxO{}, fmt.Errorf("index too high")
		}
	} else {
		if idx == len(tx.Utxos) {
			return tx.Collateral, nil
		} else {
			return UTxO{}, fmt.Errorf("index too low")
		}
	}
}
