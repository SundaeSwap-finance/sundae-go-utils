// Package replay provides a framework for replaying archived sundae-sync-v2
// blocks through consumer logic with parallel execution and dependency tracking.
//
// Blocks are read from a local filesystem (typically a mounted S3 bucket) and
// processed in parallel by worker goroutines. If an AdvanceFunc discovers it
// depends on a previous transaction or block having been processed, it calls
// WaitForTx or WaitForHeight to block until that dependency is satisfied.
//
// # Single-machine vs distributed
//
// The original Replayer.Run is single-machine: one process, parallel workers
// over a shared in-memory heightTracker. Use plain New(...) for that.
//
// For distributed replays across many machines, plug in a Coordinator (see
// coordinator.go). The DDB-backed coordinator hands out chunks of the height
// range to workers atomically and exposes a shared transaction-lookup table
// so cross-chunk WaitForTx dependencies resolve. Use NewWithCoordinator(...)
// in that mode.
//
// Heights are processed in order from a queue, and dependencies always go
// backwards (later blocks depend on earlier blocks), so deadlocks cannot occur
// as long as at least one worker is available to process the blocking height.
package replay

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/rs/zerolog"
)

// AdvanceFunc is called for each transaction during replay.
// It has the same signature as the live syncV2Consumer advance function.
// Call WaitForHeight or WaitForTx from within this function to express a
// dependency. For relevant transactions in distributed mode, call
// PublishRelevantTx after work is done so other workers can find it.
type AdvanceFunc func(ctx context.Context, tx ledger.Transaction, slot uint64, txIndex int) error

// Config configures the block replay.
type Config struct {
	BlockDir    string // path to mounted S3 bucket (contains blocks/by-hash/...)
	LookupTable string // DynamoDB lookup table name (e.g. "{env}-sundae-sync-v2--lookup")
	StartHeight uint64 // first height to process (used as the initial open-ended chunk start in single-machine mode)
	Workers     int    // number of parallel workers (default 64)
}

// heightRecord is a row from the lookup table.
type heightRecord struct {
	Height   uint64
	Hash     string // hex-encoded block hash
	Location string // S3 key path (e.g. "blocks/by-hash/a1/a1b2...cbor")
}

// Replayer replays archived blocks through an AdvanceFunc in parallel.
type Replayer struct {
	api         dynamodbiface.DynamoDBAPI
	config      Config
	advance     AdvanceFunc
	logger      zerolog.Logger
	tracker     *heightTracker
	coordinator Coordinator
}

// New creates a single-machine Replayer that processes from StartHeight
// until the lookup table runs out of consecutive height records (chain tip).
//
// Backward-compatible with the original API.
func New(api dynamodbiface.DynamoDBAPI, config Config, advance AdvanceFunc, logger zerolog.Logger) *Replayer {
	if config.Workers <= 0 {
		config.Workers = 64
	}
	return &Replayer{
		api:     api,
		config:  config,
		advance: advance,
		logger:  logger,
	}
}

// NewWithCoordinator creates a Replayer that uses an external Coordinator
// for chunk handout. Use this for distributed replays (DDBCoordinator) or for
// single-machine replays bounded to a specific [start, end) range
// (InMemoryCoordinator with both ends set).
func NewWithCoordinator(api dynamodbiface.DynamoDBAPI, config Config, advance AdvanceFunc, coord Coordinator, logger zerolog.Logger) *Replayer {
	r := New(api, config, advance, logger)
	r.coordinator = coord
	return r
}

// Run processes chunks until the coordinator reports no more work.
//
// In backward-compat mode (no coordinator), Run installs an open-ended
// in-memory coordinator covering [config.StartHeight, ∞) — exactly
// equivalent to the original behavior.
func (r *Replayer) Run(ctx context.Context) error {
	if r.coordinator == nil {
		r.coordinator = NewInMemoryCoordinator(r.config.StartHeight, 0)
	}
	defer r.coordinator.Close()

	var grandTotal atomic.Uint64

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		start, end, ok, err := r.coordinator.ClaimChunk(ctx)
		if err != nil {
			return fmt.Errorf("claim chunk: %w", err)
		}
		if !ok {
			r.logger.Info().Uint64("total", grandTotal.Load()).Msg("Replay complete: no more chunks")
			return nil
		}

		processed, err := r.processChunk(ctx, start, end)
		if err != nil {
			return fmt.Errorf("process chunk [%d, %d): %w", start, end, err)
		}
		grandTotal.Add(processed)

		if err := r.coordinator.CompleteChunk(ctx, start, end); err != nil {
			return fmt.Errorf("complete chunk [%d, %d): %w", start, end, err)
		}
		r.logger.Info().
			Uint64("chunk_start", start).
			Uint64("chunk_end", end).
			Uint64("processed_in_chunk", processed).
			Uint64("grand_total", grandTotal.Load()).
			Msg("Chunk complete")
	}
}

// processChunk runs the parallel-worker pipeline over a single chunk
// [start, end). end=0 means open-ended (run until chain tip / consecutive
// misses) — only used in single-machine in-memory mode.
//
// While the chunk is in progress, a heartbeat goroutine periodically calls
// coordinator.Heartbeat to keep the lease alive.
func (r *Replayer) processChunk(ctx context.Context, start, end uint64) (uint64, error) {
	r.tracker = newHeightTracker(start)

	work := make(chan heightRecord, r.config.Workers*2)

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	// Heartbeat goroutine — every leaseTTL/3, extend the lease.
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(LeaseTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
				if err := r.coordinator.Heartbeat(workerCtx, start, end); err != nil {
					r.logger.Warn().Err(err).Msg("heartbeat failed")
				}
			}
		}
	}()

	// Producer
	var producerErr error
	var producerDone sync.WaitGroup
	producerDone.Add(1)
	go func() {
		defer producerDone.Done()
		defer close(work)
		producerErr = r.produceHeights(workerCtx, work, start, end)
	}()

	// Workers
	var processed atomic.Uint64
	var workerErr error
	var workerErrOnce sync.Once
	var workers sync.WaitGroup

	progressInterval := uint64(100 * r.config.Workers)
	if progressInterval < 100 {
		progressInterval = 100
	}
	if progressInterval > 10000 {
		progressInterval = 10000
	}

	for i := 0; i < r.config.Workers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for rec := range work {
				if workerCtx.Err() != nil {
					return
				}
				if err := r.processHeight(workerCtx, rec); err != nil {
					workerErrOnce.Do(func() {
						workerErr = fmt.Errorf("height %d: %w", rec.Height, err)
						r.logger.Error().Err(workerErr).Uint64("height", rec.Height).Msg("Worker error")
						cancelWorkers()
					})
					return
				}
				n := processed.Add(1)
				if n == 1 || n%progressInterval == 0 {
					ws := r.tracker.WaitStats()
					r.logger.Info().
						Uint64("processed", n).
						Uint64("height", rec.Height).
						Uint64("watermark", r.tracker.Watermark()).
						Uint64("waitTxCalls", ws.TxCalls).
						Uint64("waitTxBlocked", ws.TxBlocked).
						Uint64("waitTxBailouts", ws.TxBailouts).
						Str("waitTxTime", ws.TxWaitTime.Round(time.Millisecond).String()).
						Msg("Replay progress")
				}
			}
		}()
	}

	workers.Wait()
	producerDone.Wait()
	cancelWorkers()
	<-heartbeatDone

	if workerErr != nil {
		return processed.Load(), workerErr
	}
	if producerErr != nil {
		return processed.Load(), producerErr
	}
	return processed.Load(), nil
}

// produceHeights queries the lookup table for consecutive heights using
// BatchGetItem (up to 100 per request) and sends them to the work channel.
//
// chunkStart and chunkEnd bound the range. chunkEnd=0 means open-ended
// (stop after maxConsecutiveMisses heights in a row not found, i.e. chain tip).
func (r *Replayer) produceHeights(ctx context.Context, work chan<- heightRecord, chunkStart, chunkEnd uint64) error {
	const batchSize = 100
	const maxConsecutiveMisses = 100

	consecutiveMisses := 0
	height := chunkStart
	first := true

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Bound the batch by chunkEnd if set
		batchEnd := height + batchSize
		if chunkEnd > 0 && batchEnd > chunkEnd {
			batchEnd = chunkEnd
		}
		if height >= batchEnd {
			// Reached chunk end
			return nil
		}

		keys := make([]map[string]*dynamodb.AttributeValue, 0, batchEnd-height)
		for h := height; h < batchEnd; h++ {
			keys = append(keys, map[string]*dynamodb.AttributeValue{
				"pk": {S: aws.String(fmt.Sprintf("height:%d", h))},
				"sk": {S: aws.String("height")},
			})
		}

		found := make(map[uint64]heightRecord)
		unprocessed := map[string]*dynamodb.KeysAndAttributes{
			r.config.LookupTable: {Keys: keys},
		}
		for len(unprocessed) > 0 {
			kna, ok := unprocessed[r.config.LookupTable]
			if !ok || len(kna.Keys) == 0 {
				break
			}
			out, err := r.api.BatchGetItemWithContext(ctx, &dynamodb.BatchGetItemInput{
				RequestItems: unprocessed,
			})
			if err != nil {
				return fmt.Errorf("BatchGetItem heights %d-%d: %w", height, batchEnd-1, err)
			}
			for _, item := range out.Responses[r.config.LookupTable] {
				rec := heightRecord{}
				if v := item["pk"]; v != nil && v.S != nil {
					fmt.Sscanf(*v.S, "height:%d", &rec.Height)
				}
				if v := item["hash"]; v != nil && v.S != nil {
					rec.Hash = *v.S
				}
				if v := item["location"]; v != nil && v.S != nil {
					rec.Location = *v.S
				}
				found[rec.Height] = rec
			}
			unprocessed = out.UnprocessedKeys
		}

		for h := height; h < batchEnd; h++ {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			rec, ok := found[h]
			if !ok {
				consecutiveMisses++
				// Open-ended chunk: stop when we've hit too many misses in a row
				if chunkEnd == 0 && consecutiveMisses >= maxConsecutiveMisses {
					r.logger.Info().Uint64("lastHeight", h-uint64(maxConsecutiveMisses)).Msg("Reached chain tip")
					return nil
				}
				r.tracker.MarkDone(h)
				continue
			}
			consecutiveMisses = 0

			if first {
				r.logger.Debug().
					Uint64("height", h).
					Str("hash", rec.Hash).
					Str("location", rec.Location).
					Msg("First height found")
				first = false
			}

			select {
			case work <- rec:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		height = batchEnd
	}
}

// processHeight loads a block from the filesystem, deserializes it,
// and calls the advance function for each transaction.
func (r *Replayer) processHeight(ctx context.Context, rec heightRecord) error {
	blockPath := filepath.Join(r.config.BlockDir, rec.Location)
	r.logger.Debug().Uint64("height", rec.Height).Str("path", blockPath).Msg("Loading block")

	contents, err := os.ReadFile(blockPath)
	if err != nil {
		return fmt.Errorf("read block %s: %w", blockPath, err)
	}

	if len(contents) < 2 {
		return fmt.Errorf("block file too short: %s", blockPath)
	}

	blockType := uint(contents[1])
	block, err := ledger.NewBlockFromCbor(blockType, contents[2:])
	if err != nil {
		return fmt.Errorf("decode block %s (era %d): %w", rec.Hash, blockType, err)
	}

	slot := block.SlotNumber()
	txCount := len(block.Transactions())

	r.logger.Debug().
		Uint64("height", rec.Height).
		Uint64("slot", slot).
		Int("txCount", txCount).
		Int("bytes", len(contents)).
		Msg("Block loaded")

	rctx := context.WithValue(ctx, replayContextKey{}, r.tracker)
	rctx = context.WithValue(rctx, replayHeightKey{}, rec.Height)
	rctx = context.WithValue(rctx, replayCoordinatorKey{}, r.coordinator)

	for txIdx, tx := range block.Transactions() {
		if rctx.Err() != nil {
			return rctx.Err()
		}
		txHash := tx.Hash().String()
		if err := r.advance(rctx, tx, slot, txIdx); err != nil {
			return fmt.Errorf("tx %s: %w", txHash, err)
		}
		r.tracker.MarkTxProcessed(txHash, rec.Height)
	}

	r.tracker.MarkDone(rec.Height)
	return nil
}

// --- Public API for AdvanceFuncs ---

type replayContextKey struct{}
type replayHeightKey struct{}
type replayCoordinatorKey struct{}

// WaitForHeight blocks until all blocks at or below the given height have
// been fully processed. Call this from within an AdvanceFunc when you discover
// a dependency on data from a previous block.
//
// If called outside of a replay context (e.g. during live Kinesis consumption),
// this is a no-op and returns nil immediately.
func WaitForHeight(ctx context.Context, height uint64) error {
	tracker, ok := ctx.Value(replayContextKey{}).(*heightTracker)
	if !ok {
		return nil
	}
	return tracker.WaitForHeight(ctx, height)
}

// WaitForTx blocks until the given transaction has been processed by an
// AdvanceFunc. Resolution order:
//
//  1. Local in-memory heightTracker for this chunk (fast, no network).
//  2. Coordinator.FindTx — for distributed replays, looks in the shared
//     DDB tx table to see if another worker has processed and published it.
//  3. If neither finds it and the chunk's local watermark has passed the
//     calling worker's height, the tx is treated as pre-range and we
//     return nil — the caller can fall through to a DDB lookup or treat
//     as not-found.
//
// If called outside of a replay context, this is a no-op and returns nil.
func WaitForTx(ctx context.Context, txHash string) error {
	tracker, ok := ctx.Value(replayContextKey{}).(*heightTracker)
	if !ok {
		return nil
	}
	currentHeight, _ := ctx.Value(replayHeightKey{}).(uint64)
	coord, _ := ctx.Value(replayCoordinatorKey{}).(Coordinator)
	return tracker.WaitForTx(ctx, txHash, currentHeight, coord)
}

// PublishRelevantTx records that a transaction has been processed and is
// "relevant" for cross-chunk dependency resolution. AdvanceFuncs should call
// this for transactions whose effects another worker might call WaitForTx on
// (e.g. SundaeSwap pool/order operations whose outputs become inputs to
// later scoops).
//
// In single-machine mode this is a no-op (the local heightTracker already
// records every tx automatically). In distributed mode it writes to the
// shared coordinator tx table.
//
// If called outside of a replay context, this is a no-op and returns nil.
func PublishRelevantTx(ctx context.Context, txHash string) error {
	coord, ok := ctx.Value(replayCoordinatorKey{}).(Coordinator)
	if !ok || coord == nil {
		return nil
	}
	currentHeight, _ := ctx.Value(replayHeightKey{}).(uint64)
	return coord.PublishTx(ctx, txHash, currentHeight)
}

// IsReplay returns true if the context is running inside a replay.
func IsReplay(ctx context.Context) bool {
	_, ok := ctx.Value(replayContextKey{}).(*heightTracker)
	return ok
}

// --- Height tracker ---

// heightTracker tracks completed heights and processed transactions within
// a single chunk (single process). For cross-chunk lookups, WaitForTx
// consults the coordinator passed to it.
type heightTracker struct {
	mu          sync.Mutex
	completed   map[uint64]bool
	txToHeight  map[string]uint64
	heightTxs   map[uint64][]string
	watermark   uint64
	startHeight uint64
	notify      chan struct{}

	waitTxCalls    atomic.Uint64
	waitTxBlocked  atomic.Uint64
	waitTxBailouts atomic.Uint64
	waitTxNanos    atomic.Int64
}

func newHeightTracker(startHeight uint64) *heightTracker {
	wm := startHeight
	if wm > 0 {
		wm = startHeight - 1
	}
	return &heightTracker{
		completed:   make(map[uint64]bool),
		txToHeight:  make(map[string]uint64),
		heightTxs:   make(map[uint64][]string),
		watermark:   wm,
		startHeight: startHeight,
		notify:      make(chan struct{}),
	}
}

func (ht *heightTracker) broadcast() {
	close(ht.notify)
	ht.notify = make(chan struct{})
}

func (ht *heightTracker) MarkTxProcessed(txHash string, height uint64) {
	ht.mu.Lock()
	ht.txToHeight[txHash] = height
	ht.heightTxs[height] = append(ht.heightTxs[height], txHash)
	ht.broadcast()
	ht.mu.Unlock()
}

func (ht *heightTracker) MarkDone(height uint64) {
	ht.mu.Lock()
	ht.completed[height] = true
	oldWatermark := ht.watermark
	for ht.completed[ht.watermark+1] {
		delete(ht.completed, ht.watermark+1)
		ht.watermark++
	}
	if ht.watermark > oldWatermark {
		for h := oldWatermark + 1; h <= ht.watermark; h++ {
			for _, txHash := range ht.heightTxs[h] {
				delete(ht.txToHeight, txHash)
			}
			delete(ht.heightTxs, h)
		}
		ht.broadcast()
	}
	ht.mu.Unlock()
}

func (ht *heightTracker) Watermark() uint64 {
	ht.mu.Lock()
	defer ht.mu.Unlock()
	return ht.watermark
}

type WaitStats struct {
	TxCalls    uint64
	TxBlocked  uint64
	TxBailouts uint64
	TxWaitTime time.Duration
}

func (ht *heightTracker) WaitStats() WaitStats {
	return WaitStats{
		TxCalls:    ht.waitTxCalls.Load(),
		TxBlocked:  ht.waitTxBlocked.Load(),
		TxBailouts: ht.waitTxBailouts.Load(),
		TxWaitTime: time.Duration(ht.waitTxNanos.Load()),
	}
}

func (ht *heightTracker) WaitForHeight(ctx context.Context, height uint64) error {
	for {
		ht.mu.Lock()
		wm := ht.watermark
		ch := ht.notify
		ht.mu.Unlock()

		if wm >= height {
			return nil
		}

		select {
		case <-ch:
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// WaitForTx waits for a transaction to appear in the local tracker. If the
// local tracker can't find it after the chunk's watermark has caught up, and
// a coordinator is provided, it falls through to coordinator.FindTx for
// cross-chunk lookup. After both checks fail, returns nil (bailout — caller
// treats as pre-range tx already in DynamoDB).
func (ht *heightTracker) WaitForTx(ctx context.Context, txHash string, currentHeight uint64, coord Coordinator) error {
	ht.waitTxCalls.Add(1)
	start := time.Now()
	blocked := false

	for {
		ht.mu.Lock()
		_, found := ht.txToHeight[txHash]
		wm := ht.watermark
		ch := ht.notify
		ht.mu.Unlock()

		if found {
			if blocked {
				ht.waitTxNanos.Add(int64(time.Since(start)))
			}
			return nil
		}

		// Local watermark caught up but tx still not in local tracker.
		// Either it's pre-chunk (already in DDB) or in another worker's
		// chunk (try coordinator.FindTx).
		if currentHeight > 0 && wm >= currentHeight-1 {
			if coord != nil {
				if _, ok, err := coord.FindTx(ctx, txHash); err == nil && ok {
					if blocked {
						ht.waitTxNanos.Add(int64(time.Since(start)))
					}
					return nil
				}
			}
			ht.waitTxBailouts.Add(1)
			if blocked {
				ht.waitTxNanos.Add(int64(time.Since(start)))
			}
			return nil
		}

		if !blocked {
			blocked = true
			ht.waitTxBlocked.Add(1)
		}

		select {
		case <-ch:
			continue
		case <-ctx.Done():
			ht.waitTxNanos.Add(int64(time.Since(start)))
			return ctx.Err()
		}
	}
}

// LogProgress logs replay progress at a fixed interval (utility for callers
// that want their own progress reporting on a custom counter).
func LogProgress(ctx context.Context, logger zerolog.Logger, processed *atomic.Uint64, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			logger.Info().Uint64("processed", processed.Load()).Msg("Replay progress")
		case <-ctx.Done():
			return
		}
	}
}
