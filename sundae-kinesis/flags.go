package sundaekinesis

import (
	"time"

	"github.com/urfave/cli/v2"
)

var KinesisOpts struct {
	Ogmios     string
	StreamName string
	Replay     bool
	ReplayFrom cli.Timestamp
}

var OgmiosFlag = cli.StringFlag{
	Name:        "ogmios",
	Usage:       "The ogmios endpoint to connect to",
	Value:       "http://localhost:8000",
	EnvVars:     []string{"OGMIOS"},
	Destination: &KinesisOpts.Ogmios,
}

var StreamNameFlag = cli.StringFlag{
	Name:        "stream-name",
	Usage:       "The stream name to read records from",
	EnvVars:     []string{"STREAM_NAME"},
	Destination: &KinesisOpts.StreamName,
}

var ReplayFlag = cli.BoolFlag{
	Name:        "replay",
	Usage:       "Whether to replay from the beginning, or start from the current slot",
	Value:       false,
	EnvVars:     []string{"REPLAY"},
	Destination: &KinesisOpts.Replay,
}

var ReplayFromFlag = cli.TimestampFlag{
	Name:        "replay-from",
	Usage:       "Timestamp to replay from",
	Value:       cli.NewTimestamp(time.Date(2022, 11, 30, 0, 0, 0, 0, time.UTC)),
	Layout:      "2006-01-02 15:04:05",
	EnvVars:     []string{"REPLAY_FROM"},
	Destination: &KinesisOpts.ReplayFrom,
}

var KinesisFlags = []cli.Flag{
	&StreamNameFlag,
	&ReplayFlag,
	&ReplayFromFlag,
}
