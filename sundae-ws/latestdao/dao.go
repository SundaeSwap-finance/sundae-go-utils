package latestdao

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/savaki/ddb"
)

// DAO provides access to the WebSocket latest-payload cache table.
type DAO struct {
	table     *ddb.Table
	api       dynamodbiface.DynamoDBAPI
	tableName string
}

// New creates a new latest-payload DAO.
func New(api dynamodbiface.DynamoDBAPI, tableName string) *DAO {
	return &DAO{
		table:     ddb.New(api).MustTable(tableName, Latest{}),
		api:       api,
		tableName: tableName,
	}
}

// Put stores or overwrites the latest payload for a topic.
func (d *DAO) Put(ctx context.Context, entry Latest) error {
	return d.table.Put(entry).RunWithContext(ctx)
}

// Get retrieves the latest payload for a topic. Returns nil if not found.
func (d *DAO) Get(ctx context.Context, topic string) (*Latest, error) {
	var entry Latest
	if err := d.table.Get(topic).ScanWithContext(ctx, &entry); err != nil {
		if ddb.IsItemNotFoundError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get latest for topic %v: %w", topic, err)
	}
	return &entry, nil
}
