package cursordao

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SundaeSwap-finance/ogmigo/v6/ouroboros/chainsync"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/savaki/ddb"
	"github.com/tj/assert"
)

func withTable(t *testing.T, callback func(ctx context.Context, dao *DAO)) {
	var (
		s = session.Must(session.NewSession(aws.NewConfig().
			WithCredentials(credentials.NewStaticCredentials("blah", "blah", "")).
			WithEndpoint("http://localhost:8000").
			WithRegion("us-west-2")))
		api       = dynamodb.New(s)
		client    = ddb.New(api)
		tableName = fmt.Sprintf("table-%v", time.Now().UnixNano())
		table     = client.MustTable(tableName, Record{})
		dao       = New(api, tableName)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := table.CreateTableIfNotExists(ctx)
	assert.Nil(t, err)
	defer table.DeleteTableIfExists(ctx)

	callback(ctx, dao)
}

func TestDAO(t *testing.T) {
	withTable(t, func(ctx context.Context, dao *DAO) {
		var (
			h1      uint64 = 1
			h2      uint64 = 2
			h3      uint64 = 3
			h4      uint64 = 4
			usage          = "usage"
			b1             = chainsync.PointStruct{Height: &h1, Slot: 10}
			b2             = chainsync.PointStruct{Height: &h2, Slot: 20}
			b3             = chainsync.PointStruct{Height: &h3, Slot: 30}
			b4             = chainsync.PointStruct{Height: &h4, Slot: 40}
			err     error
			counter int64
		)

		err = dao.Save(ctx, b1, usage, chainsync.Tx{ID: "1a"})
		assert.Nil(t, err)

		err = dao.Save(ctx, b2, usage, chainsync.Tx{ID: "2a"})
		assert.Nil(t, err)

		err = dao.Save(ctx, b3, usage, chainsync.Tx{ID: "3a"})
		assert.Nil(t, err)

		err = dao.Save(ctx, b4, usage, chainsync.Tx{ID: "4a"})
		assert.Nil(t, err)

		// verify no rollback if stop at slot beyond high-water mark
		//
		err = dao.Rollback(ctx, b4.Slot+1, usage, func(ctx context.Context, block uint64, txs ...string) error {
			atomic.AddInt64(&counter, 1)
			return nil
		})
		assert.Nil(t, err)
		assert.EqualValues(t, 0, atomic.LoadInt64(&counter))

		// roll back one block
		//
		err = dao.Rollback(ctx, b3.Slot, usage, func(ctx context.Context, block uint64, txs ...string) error {
			assert.Equal(t, *b4.Height, block)
			assert.Equal(t, []string{"4a"}, txs)
			atomic.AddInt64(&counter, 1)
			return nil
		})
		assert.Nil(t, err)
		assert.EqualValues(t, 1, atomic.LoadInt64(&counter))

		// roll back one more block
		//
		var (
			blocks   []uint64
			received []string
		)
		err = dao.Rollback(ctx, b1.Slot, usage, func(ctx context.Context, block uint64, txs ...string) error {
			blocks = append(blocks, block)
			received = append(received, txs...)
			return nil
		})
		assert.Nil(t, err)
		assert.Equal(t, []uint64{*b3.Height, *b2.Height}, blocks)
		assert.Equal(t, []string{"3a", "2a"}, received)
	})
}
