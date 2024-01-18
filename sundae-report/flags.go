package sundaereport

import (
	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	"github.com/urfave/cli/v2"
)

var ReportOpts struct {
	Bucket string

	OutFile   string
	GetLatest bool
}

var BucketFlag = sundaecli.StringFlag("bucket", "The bucket to write the report to", &ReportOpts.Bucket)
var OutFileFlag = sundaecli.StringFlag("out-file", "The file to write the report to, when running in dry mode", &ReportOpts.OutFile)
var GetLatestFlag = sundaecli.BoolFlag("get-latest", "Get the latest report from the bucket instead of generating a new one", &ReportOpts.GetLatest)

var ReportFlags = []cli.Flag{
	BucketFlag,
	OutFileFlag,
	GetLatestFlag,
}
