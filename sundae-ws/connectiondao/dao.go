package connectiondao

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/savaki/ddb"
)

// DAO provides access to the WebSocket connections table.
type DAO struct {
	table     *ddb.Table
	api       dynamodbiface.DynamoDBAPI
	tableName string
}

// New creates a new connections DAO.
func New(api dynamodbiface.DynamoDBAPI, tableName string) *DAO {
	return &DAO{
		table:     ddb.New(api).MustTable(tableName, Connection{}),
		api:       api,
		tableName: tableName,
	}
}

// Put stores a connection record.
func (d *DAO) Put(ctx context.Context, conn Connection) error {
	return d.table.Put(conn).RunWithContext(ctx)
}

// Get retrieves a connection record by ID.
func (d *DAO) Get(ctx context.Context, connectionID string) (*Connection, error) {
	var conn Connection
	if err := d.table.Get(connectionID).ScanWithContext(ctx, &conn); err != nil {
		if ddb.IsItemNotFoundError(err) {
			return nil, fmt.Errorf("connection %v not found", connectionID)
		}
		return nil, fmt.Errorf("failed to get connection %v: %w", connectionID, err)
	}
	return &conn, nil
}

// Delete removes a connection record by ID.
func (d *DAO) Delete(ctx context.Context, connectionID string) error {
	return d.table.Delete(connectionID).RunWithContext(ctx)
}
