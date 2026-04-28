package replay

import (
	"fmt"
	"os"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/rs/zerolog"
	"github.com/urfave/cli/v2"
)

// CommonOpts holds the flag-bound configuration shared by replay-driven
// migration binaries (migrate-pools, migrate-orders, ...). Wire CommonFlags
// into your sundaecli.App and call NewFromCommonOpts to construct a
// fully-configured Replayer that picks the right Coordinator based on which
// flags the caller provided.
var CommonOpts struct {
	BlockDir         string
	StartHeight      uint64
	EndHeight        uint64
	CoordinatorTable string
	WorkerID         string
	Workers          int
}

// CommonFlags is the standard flag set used by replay-driven migrate-*
// binaries. Concatenate it into sundaecli.App's flag list alongside
// sundaecli.CommonFlags.
var CommonFlags = []cli.Flag{
	sundaecli.StringFlag("block-dir",
		"path to mounted S3 bucket with archived blocks", &CommonOpts.BlockDir),
	sundaecli.Uint64Flag("start-height",
		"first block height to process (single-machine mode); ignored when --coordinator-table is set",
		&CommonOpts.StartHeight),
	sundaecli.Uint64Flag("end-height",
		"exclusive upper bound for single-machine mode; 0 = run until chain tip",
		&CommonOpts.EndHeight),
	sundaecli.StringFlag("coordinator-table",
		"DDB table prefix for distributed coordination. When set, claim chunks from this prefix's tables instead of running single-machine.",
		&CommonOpts.CoordinatorTable),
	sundaecli.StringFlag("worker-id",
		"identifier for this worker (default: hostname-pid). Only used in distributed mode.",
		&CommonOpts.WorkerID),
	sundaecli.IntFlag("workers",
		"number of parallel workers", &CommonOpts.Workers, 64),
}

// NewFromCommonOpts builds a Replayer with the right Coordinator for the
// flag combination the caller provided:
//
//   - --coordinator-table set: distributed (DDBCoordinator) — workers claim
//     chunks from a pre-seeded DDB table prefix and exit cleanly when no
//     more chunks remain
//   - --end-height > 0:        single-machine bounded (InMemoryCoordinator)
//   - otherwise:               single-machine open-ended (runs until the
//     lookup table runs out of consecutive heights / chain tip)
//
// Logs a one-line summary of the chosen mode at info level.
func NewFromCommonOpts(api dynamodbiface.DynamoDBAPI, lookupTable string, advance AdvanceFunc, logger zerolog.Logger) *Replayer {
	cfg := Config{
		BlockDir:    CommonOpts.BlockDir,
		LookupTable: lookupTable,
		StartHeight: CommonOpts.StartHeight,
		Workers:     CommonOpts.Workers,
	}

	if CommonOpts.CoordinatorTable != "" {
		workerID := CommonOpts.WorkerID
		if workerID == "" {
			host, _ := os.Hostname()
			workerID = fmt.Sprintf("%s-%d", host, os.Getpid())
		}
		coord := NewDDBCoordinator(api, CommonOpts.CoordinatorTable, workerID, LeaseTTL, logger)
		logger.Info().
			Str("blockDir", cfg.BlockDir).
			Str("coordinatorTable", CommonOpts.CoordinatorTable).
			Str("workerID", workerID).
			Int("workers", cfg.Workers).
			Str("lookupTable", lookupTable).
			Msg("Starting replay (distributed mode)")
		return NewWithCoordinator(api, cfg, advance, coord, logger)
	}

	if CommonOpts.EndHeight > 0 {
		coord := NewInMemoryCoordinator(CommonOpts.StartHeight, CommonOpts.EndHeight)
		logger.Info().
			Str("blockDir", cfg.BlockDir).
			Uint64("startHeight", CommonOpts.StartHeight).
			Uint64("endHeight", CommonOpts.EndHeight).
			Int("workers", cfg.Workers).
			Str("lookupTable", lookupTable).
			Msg("Starting replay (single-machine bounded mode)")
		return NewWithCoordinator(api, cfg, advance, coord, logger)
	}

	logger.Info().
		Str("blockDir", cfg.BlockDir).
		Uint64("startHeight", CommonOpts.StartHeight).
		Int("workers", cfg.Workers).
		Str("lookupTable", lookupTable).
		Msg("Starting replay (single-machine, open-ended)")
	return New(api, cfg, advance, logger)
}
