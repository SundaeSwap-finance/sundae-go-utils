// Package replay provides a framework for replaying archived sundae-sync-v2
// blocks through consumer logic with parallel execution and dependency tracking.
//
// Blocks are read from a local filesystem (typically a mounted S3 bucket) and
// processed in parallel by worker goroutines. If an AdvanceFunc discovers it
// depends on a previous transaction or block having been processed, it calls
// WaitForTx or WaitForHeight to block until that dependency is satisfied.
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
// Call WaitForHeight from within this function to express a dependency.
type AdvanceFunc func(ctx context.Context, tx ledger.Transaction, slot uint64, txIndex int) error

// Config configures the block replay.
type Config struct {
	BlockDir    string // path to mounted S3 bucket (contains blocks/by-hash/...)
	LookupTable string // DynamoDB lookup table name (e.g. "{env}-sundae-sync-v2--lookup")
	StartHeight uint64 // first height to process
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
	api     dynamodbiface.DynamoDBAPI
	config  Config
	advance AdvanceFunc
	logger  zerolog.Logger
	tracker *heightTracker
}

// New creates a new Replayer.
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

// Run processes all blocks from StartHeight until no more heights are found
// in the lookup table. Returns nil on successful completion.
func (r *Replayer) Run(ctx context.Context) error {
	r.tracker = newHeightTracker(r.config.StartHeight)

	// Channel of height records to process
	work := make(chan heightRecord, r.config.Workers*2)

	// Producer: query heights from DynamoDB and feed into work channel
	var producerErr error
	var producerDone sync.WaitGroup
	producerDone.Add(1)
	go func() {
		defer producerDone.Done()
		defer close(work)
		producerErr = r.produceHeights(ctx, work)
	}()

	// Workers: process blocks in parallel
	var processed atomic.Uint64
	var workerErr error
	var workerErrOnce sync.Once
	var workers sync.WaitGroup

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

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
						cancelWorkers()
					})
					return
				}
				n := processed.Add(1)
				if n%1000 == 0 {
					r.logger.Info().
						Uint64("processed", n).
						Uint64("height", rec.Height).
						Uint64("watermark", r.tracker.Watermark()).
						Msg("Replay progress")
				}
			}
		}()
	}

	workers.Wait()
	producerDone.Wait()

	total := processed.Load()
	r.logger.Info().Uint64("total", total).Msg("Replay complete")

	if workerErr != nil {
		return workerErr
	}
	return producerErr
}

// produceHeights queries the lookup table for consecutive heights and sends
// them to the work channel. Stops when a height is not found (past the tip).
func (r *Replayer) produceHeights(ctx context.Context, work chan<- heightRecord) error {
	consecutiveMisses := 0
	const maxConsecutiveMisses = 100 // allow some gaps

	for height := r.config.StartHeight; ; height++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		pk := fmt.Sprintf("height:%d", height)
		result, err := r.api.GetItemWithContext(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(r.config.LookupTable),
			Key: map[string]*dynamodb.AttributeValue{
				"pk": {S: aws.String(pk)},
				"sk": {S: aws.String("height")},
			},
		})
		if err != nil {
			return fmt.Errorf("GetItem height %d: %w", height, err)
		}

		if result.Item == nil {
			consecutiveMisses++
			if consecutiveMisses >= maxConsecutiveMisses {
				r.logger.Info().Uint64("lastHeight", height-uint64(maxConsecutiveMisses)).Msg("Reached chain tip")
				return nil
			}
			// Mark empty heights as done so waiters don't block forever
			r.tracker.MarkDone(height)
			continue
		}
		consecutiveMisses = 0

		rec := heightRecord{Height: height}
		if v := result.Item["hash"]; v != nil && v.S != nil {
			rec.Hash = *v.S
		}
		if v := result.Item["location"]; v != nil && v.S != nil {
			rec.Location = *v.S
		}

		select {
		case work <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// processHeight loads a block from the filesystem, deserializes it,
// and calls the advance function for each transaction.
func (r *Replayer) processHeight(ctx context.Context, rec heightRecord) error {
	// Load block from filesystem
	blockPath := filepath.Join(r.config.BlockDir, rec.Location)
	contents, err := os.ReadFile(blockPath)
	if err != nil {
		return fmt.Errorf("read block %s: %w", blockPath, err)
	}

	if len(contents) < 2 {
		return fmt.Errorf("block file too short: %s", blockPath)
	}

	// Deserialize: byte 0 = CBOR marker, byte 1 = era, bytes 2+ = block
	blockType := uint(contents[1])
	block, err := ledger.NewBlockFromCbor(blockType, contents[2:])
	if err != nil {
		return fmt.Errorf("decode block %s (era %d): %w", rec.Hash, blockType, err)
	}

	slot := block.SlotNumber()

	// Inject replay context so AdvanceFunc can call WaitForHeight
	rctx := context.WithValue(ctx, replayContextKey{}, r.tracker)

	// Process each transaction
	for txIdx, tx := range block.Transactions() {
		if rctx.Err() != nil {
			return rctx.Err()
		}
		txHash := tx.Hash().String()
		if err := r.advance(rctx, tx, slot, txIdx); err != nil {
			return fmt.Errorf("tx %s: %w", txHash, err)
		}
		// Mark tx as processed so WaitForTx callers in other workers unblock.
		r.tracker.MarkTxProcessed(txHash, rec.Height)
	}

	// Mark this height as done (advances watermark, triggers GC of tx entries)
	r.tracker.MarkDone(rec.Height)
	return nil
}

// --- Public API for AdvanceFuncs ---

type replayContextKey struct{}

// WaitForHeight blocks until all blocks at or below the given height have
// been fully processed. Call this from within an AdvanceFunc when you discover
// a dependency on data from a previous block.
//
// If called outside of a replay context (e.g. during live Kinesis consumption),
// this is a no-op and returns nil immediately.
func WaitForHeight(ctx context.Context, height uint64) error {
	tracker, ok := ctx.Value(replayContextKey{}).(*heightTracker)
	if !ok {
		return nil // not in replay mode, no-op
	}
	return tracker.WaitForHeight(ctx, height)
}

// WaitForTx blocks until the given transaction has been processed by an
// AdvanceFunc. This is the primary dependency primitive for UTXO-based
// consumers: when a transaction consumes an input UTXO, call WaitForTx
// with the input's transaction hash to ensure the producing transaction
// has been fully processed.
//
// If called outside of a replay context (e.g. during live Kinesis consumption),
// this is a no-op and returns nil immediately.
func WaitForTx(ctx context.Context, txHash string) error {
	tracker, ok := ctx.Value(replayContextKey{}).(*heightTracker)
	if !ok {
		return nil // not in replay mode, no-op
	}
	return tracker.WaitForTx(ctx, txHash)
}

// IsReplay returns true if the context is running inside a replay.
// AdvanceFuncs can use this to distinguish live vs replay execution.
func IsReplay(ctx context.Context) bool {
	_, ok := ctx.Value(replayContextKey{}).(*heightTracker)
	return ok
}

// --- Height tracker ---

// heightTracker tracks completed heights and processed transactions.
//
// Heights use a watermark: the highest height such that all heights in
// [startHeight, watermark] have been marked done.
//
// Transactions are tracked individually in a map (txHash → height).
// Entries are garbage-collected when the watermark advances past their height,
// keeping memory bounded to the in-flight processing window.
type heightTracker struct {
	mu          sync.Mutex
	completed   map[uint64]bool
	txToHeight  map[string]uint64   // txHash → height it was processed in
	heightTxs   map[uint64][]string // height → txHashes (for GC on watermark advance)
	watermark   uint64              // all heights <= watermark are done
	startHeight uint64
	notify      chan struct{} // closed and recreated on any progress (broadcast)
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

// broadcast wakes all goroutines waiting on the notify channel.
// Caller must hold ht.mu.
func (ht *heightTracker) broadcast() {
	close(ht.notify)
	ht.notify = make(chan struct{})
}

// MarkTxProcessed records that a transaction at the given height has been
// fully processed by the AdvanceFunc. This unblocks any WaitForTx callers.
func (ht *heightTracker) MarkTxProcessed(txHash string, height uint64) {
	ht.mu.Lock()
	ht.txToHeight[txHash] = height
	ht.heightTxs[height] = append(ht.heightTxs[height], txHash)
	ht.broadcast()
	ht.mu.Unlock()
}

// MarkDone marks a height as fully processed and advances the watermark
// if possible. GCs tx entries for heights that fall below the new watermark.
func (ht *heightTracker) MarkDone(height uint64) {
	ht.mu.Lock()
	ht.completed[height] = true
	oldWatermark := ht.watermark
	for ht.completed[ht.watermark+1] {
		delete(ht.completed, ht.watermark+1)
		ht.watermark++
	}
	// GC tx entries for heights now below watermark
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

// Watermark returns the current watermark (highest contiguously completed height).
func (ht *heightTracker) Watermark() uint64 {
	ht.mu.Lock()
	defer ht.mu.Unlock()
	return ht.watermark
}

// WaitForHeight blocks until the watermark reaches at least the given height.
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

// WaitForTx blocks until the given transaction hash has been processed.
// The tx is considered processed once MarkTxProcessed has been called for it.
//
// If the tx was already processed and GC'd (its height fell below the
// watermark), it will NOT be found in the map. This is safe because if
// the tx was GC'd, its effects are already visible in DynamoDB — the caller's
// subsequent database query will succeed without needing to wait.
func (ht *heightTracker) WaitForTx(ctx context.Context, txHash string) error {
	for {
		ht.mu.Lock()
		_, found := ht.txToHeight[txHash]
		ch := ht.notify
		ht.mu.Unlock()

		if found {
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

// --- Utility ---

// LogProgress logs replay progress at a fixed interval.
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
