package txdao

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/rs/zerolog"
	"github.com/savaki/ddb"
)

type DAO struct {
	logger   zerolog.Logger
	table    *ddb.Table
	cache    sync.Map
	cacheOff bool
	dry      bool
}

func New(api dynamodbiface.DynamoDBAPI, tableName string, logger zerolog.Logger, dry bool) *DAO {
	return &DAO{table: ddb.New(api).MustTable(tableName, &Tx{}), logger: logger, dry: dry}
}

// NewUncached returns a DAO that does not cache fetched Tx records. Use this
// for replay-style workloads that fetch many distinct txs across a long run —
// the cache (unbounded sync.Map of full Tx records with embedded UTxO/datum
// payloads) otherwise grows without limit and OOMs the process.
func NewUncached(api dynamodbiface.DynamoDBAPI, tableName string, logger zerolog.Logger, dry bool) *DAO {
	d := New(api, tableName, logger, dry)
	d.cacheOff = true
	return d
}

func (dao *DAO) Get(ctx context.Context, hash string) (Tx, error) {
	if !dao.cacheOff {
		if v, ok := dao.cache.Load(hash); ok {
			return v.(Tx), nil
		}
	}

	var tx Tx
	err := dao.table.Get(fmt.Sprintf("tx:%v", hash)).Range("tx").ScanWithContext(ctx, &tx)
	if err != nil {
		return Tx{}, fmt.Errorf("failed to fetch transaction: %w", err)
	}

	if !dao.cacheOff {
		dao.cache.Store(hash, tx)
	}
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
