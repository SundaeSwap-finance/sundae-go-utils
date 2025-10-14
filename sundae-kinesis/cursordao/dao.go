package cursordao

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/SundaeSwap-finance/ogmigo/v6/ouroboros/chainsync"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/rs/zerolog"
	"github.com/savaki/ddb"
)

// NOTE: value is 18446744073709551615
const BlockHighWater = ^uint64(0) // blockHighWater is a virtual block that represents the high watermark of block height

type Record struct {
	Block uint64 `dynamodbav:"block" ddb:"hash"`
	Usage string `dynamodbav:"usage" ddb:"range"`

	LastBlock   uint64                 `dynamodbav:"last_block,omitempty"`
	PointStruct *chainsync.PointStruct `dynamodbav:"point,omitempty"`
	TxHashes    ddb.StringSet          `dynamodbav:"tx_hashes,omitempty"`
	TTL         int64                  `dynamodbav:"ttl,omitempty"`
}

type DAO struct {
	client *ddb.DDB
	table  *ddb.Table
}

func New(api dynamodbiface.DynamoDBAPI, tableName string) *DAO {
	client := ddb.New(api)
	return &DAO{
		client: client,
		table:  client.MustTable(tableName, Record{}),
	}
}

func (d *DAO) FindCursor(ctx context.Context, block uint64, usage string) (chainsync.PointStruct, error) {
	r, err := d.FindFullCursor(ctx, block, usage)
	if err != nil {
		return chainsync.PointStruct{}, err
	}

	if r.PointStruct == nil {
		return chainsync.PointStruct{}, fmt.Errorf("failed to find cursor for block, %v, and usage, %v: no cursor associated", block, usage)
	}

	return *r.PointStruct, nil
}

func (d *DAO) FindFullCursor(ctx context.Context, block uint64, usage string) (Record, error) {
	get := d.table.Get(block).Range(usage).
		ConsistentRead(true)

	var r Record
	if err := get.ScanWithContext(ctx, &r); err != nil {
		return Record{}, fmt.Errorf("failed to find cursor for block, %v, and usage, %v: %w", block, usage, err)
	}
	return r, nil
}

func (d *DAO) Tip(ctx context.Context, usage string) (chainsync.PointStruct, error) {
	get := d.table.Get(BlockHighWater).Range(usage).ConsistentRead(true)
	var r Record
	if err := get.ScanWithContext(ctx, &r); err != nil {
		return chainsync.PointStruct{}, fmt.Errorf("failed to find block highwater mark for usage %v: %w", usage, err)
	}
	if r.PointStruct == nil {
		return chainsync.PointStruct{}, fmt.Errorf("failed to find cursor highwater mark for usage %v: no cursor associated", usage)
	}
	return *r.PointStruct, nil
}

func (d *DAO) SaveRaw(ctx context.Context, r Record) error {
	return d.table.Put(r).RunWithContext(ctx)
}

func (d *DAO) Save(ctx context.Context, ps chainsync.PointStruct, usage string, txs ...chainsync.Tx) (err error) {
	defer func(begin time.Time) {
		var height uint64 = 0
		if ps.Height != nil {
			height = *ps.Height
		}
		zerolog.Ctx(ctx).Info().
			Dur("elapsed", time.Since(begin)).
			Err(err).
			Str("usage", usage).
			Uint64("height", height).
			Msg("saved point")
	}(time.Now())

	var txHashes ddb.StringSet
	for _, tx := range txs {
		txHashes = append(txHashes, tx.ID)
	}

	expiration := time.Now().AddDate(0, 0, 15)
	var height uint64 = 0
	if ps.Height != nil {
		height = *ps.Height
	}
	record := Record{
		Block:       height,
		Usage:       usage,
		PointStruct: &ps,
		TxHashes:    txHashes,
		TTL:         expiration.Unix(),
	}
	pr := d.table.Put(record)

	high := Record{
		Block:       BlockHighWater,
		Usage:       usage,
		PointStruct: &ps,
		LastBlock:   height,
	}
	ph := d.table.Put(high)

	if _, err := d.client.TransactWriteItemsWithContext(ctx, pr, ph); err != nil {
		return fmt.Errorf("failed to save point, %v: %w", height, err)
	}

	return nil
}

func (d *DAO) Rollback(ctx context.Context, slot uint64, usage string, callback func(ctx context.Context, block uint64, txs ...string) error) (err error) {
	ps, err := d.FindCursor(ctx, BlockHighWater, usage)
	if err != nil {
		if ddb.IsItemNotFoundError(err) {
			return nil // no prior blocks saved e.g. nothing to roll back
		}
		return err
	}

	var height uint64 = 0
	if ps.Height != nil {
		height = *ps.Height
	}
	for block := height; ; block-- {
		if err := d.rollbackBlock(ctx, block, slot, usage, callback); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("rollback failed: %w", err)
		}
	}
}

// rollbackBlock rolls back the transaction associated with the provided block IF the slot associated with the block
// is greater than the stopAtSlot.  If the block references a slot that is less than or equal to the stopAtSlot, then
// `rollbackBlock` returns io.EOF
func (d *DAO) rollbackBlock(ctx context.Context, block, stopAtSlot uint64, usage string, callback func(ctx context.Context, block uint64, txs ...string) error) (err error) {
	defer func(begin time.Time) {
		zerolog.Ctx(ctx).Info().
			Dur("elapsed", time.Since(begin)).
			Err(err).
			Uint64("block", block).
			Msg("rolled back block")
	}(time.Now())

	var r Record
	get := d.table.Get(block).Range(usage).
		ConsistentRead(true)

	if err := get.ScanWithContext(ctx, &r); err != nil {
		if ddb.IsItemNotFoundError(err) {
			return nil // rollback points may not be present in the case of long replays
		}
		return fmt.Errorf("failed to rollback block, %v, for usage, %v: get failed: %w", block, usage, err)
	}

	if r.PointStruct.Slot <= stopAtSlot {
		return io.EOF
	}

	// Rollback the transactions *in reverse order*, since one transaction may depend on an earlier one
	var hashes []string
	hashes = append(hashes, r.TxHashes...)
	for i, j := 0, len(hashes)-1; i < j; i, j = i+1, j-1 {
		hashes[i], hashes[j] = hashes[j], hashes[i]
	}

	if err := callback(ctx, r.Block, r.TxHashes...); err != nil {
		return fmt.Errorf("failed to rollback block, %v, for usage, %v: callback failed: %w", block, usage, err)
	}

	last, err := d.FindCursor(ctx, block-1, usage)
	if err != nil {
		return fmt.Errorf("failed to rollback block, %v, for usage, %v: FindCursor failed: %w", block, usage, err)
	}

	var height uint64 = 0
	if last.Height != nil {
		height = *last.Height
	}
	high := Record{
		Block:       BlockHighWater,
		Usage:       usage,
		LastBlock:   height,
		PointStruct: &last,
	}
	del := d.table.Delete(block).Range(usage)
	put := d.table.Put(high)

	if _, err := d.client.TransactWriteItemsWithContext(ctx, put, del); err != nil {
		return fmt.Errorf("failed to rollback block, %v, for usage, %v: tx write failed: %w", block, usage, err)
	}

	return nil
}
