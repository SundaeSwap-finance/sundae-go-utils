package sundaekinesis

import (
	"time"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	"github.com/urfave/cli/v2"
)

var KinesisOpts struct {
	Ogmios      string
	PatchReplay bool
	Point       string
	StreamName  string
	Replay      bool
	ReplayFrom  cli.Timestamp
}

var OgmiosFlag = sundaecli.StringFlag("ogmios", "The ogmios endpoint to connect to", &KinesisOpts.Ogmios, "http://localhost:8000")
var PatchReplayFlag = sundaecli.BoolFlag("patch-replay", "Ignore the first rollback message and replay from the specified point (Ogmios-only)", &KinesisOpts.PatchReplay)
var PointFlag = sundaecli.StringFlag("point", "one or more points to try to start from (in the form: slot/blockHash)", &KinesisOpts.Point)
var StreamNameFlag = sundaecli.StringFlag("stream-name", "The stream name to read records from", &KinesisOpts.StreamName)
var ReplayFlag = sundaecli.BoolFlag("replay", "Whether to replay from the beginning, or start from the next message", &KinesisOpts.Replay)

var ReplayFromFlag = cli.TimestampFlag{
	Name:        "replay-from",
	Usage:       "Timestamp to replay from",
	Value:       cli.NewTimestamp(time.Date(2022, 11, 30, 0, 0, 0, 0, time.UTC)),
	Layout:      "2006-01-02 15:04:05",
	EnvVars:     []string{"REPLAY_FROM"},
	Destination: &KinesisOpts.ReplayFrom,
}

var KinesisFlags = []cli.Flag{
	OgmiosFlag,
	PatchReplayFlag,
	PointFlag,
	StreamNameFlag,
	ReplayFlag,
	&ReplayFromFlag,
}
