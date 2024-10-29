package sundaeddb

import (
	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	"github.com/urfave/cli/v2"
)

var DDBOpts struct {
	DAXCluster string
	TableName  string
}

var DAXClusterFlag = sundaecli.StringFlag("dax-cluster", "The DAX cluster to connect to", &DDBOpts.DAXCluster)
var TableNameFlag = sundaecli.StringFlag("table-name", "The table name to read streams from", &DDBOpts.TableName)

var DDBFlags = []cli.Flag{
	DAXClusterFlag,
	TableNameFlag,
}
