package replay

import (
	"context"
	"errors"
	"time"
)

// Coordinator distributes replay work across one or more workers.
//
// A Coordinator hands out chunks (contiguous ranges of block heights) for a
// worker to process locally. Within a chunk, the Replayer uses an in-memory
// heightTracker for fast intra-chunk dependency resolution. For dependencies
// that cross chunks (a tx referenced from one chunk that lives in another),
// the worker calls FindTx — the DDB-backed coordinator looks up the tx in a
// shared "processed" table; the in-memory coordinator falls through.
//
// Two implementations:
//   - InMemoryCoordinator: single-process. Whole replay range is one chunk.
//     Drop-in replacement for the original Replayer behavior.
//   - DDBCoordinator: multi-machine. Chunks are pre-allocated rows in a DDB
//     "claims" table. Workers atomically claim chunks via conditional updates.
//     Tx publication and lookup go through a second DDB table.
//
// Chunk size is the key tuning knob. Mainnet has ~14M blocks but only a
// fraction touch the protocols we index, so each chunk is mostly quick-rejects.
// A large chunk (e.g. 10000 heights) amortizes coordination overhead over many
// no-op blocks.
type Coordinator interface {
	// ClaimChunk atomically claims an unprocessed chunk and returns its
	// inclusive start and exclusive end heights. ok=false means no more
	// chunks are available — the worker should exit cleanly.
	ClaimChunk(ctx context.Context) (start, end uint64, ok bool, err error)

	// CompleteChunk marks the chunk as fully processed. Called once after the
	// worker finishes every height in the chunk.
	CompleteChunk(ctx context.Context, start, end uint64) error

	// Heartbeat extends the lease on a held chunk. Called periodically while
	// processing so a long-running chunk doesn't get reclaimed by another worker.
	Heartbeat(ctx context.Context, start, end uint64) error

	// PublishTx records that a transaction has been processed. For DDB-backed
	// coordinators this writes to a shared "processed" table so other workers
	// can resolve cross-chunk dependencies via FindTx.
	PublishTx(ctx context.Context, txHash string, height uint64) error

	// FindTx returns the height a tx was processed at, or ok=false if it has
	// not (yet) been processed. For in-memory coordinators this is a no-op
	// fallback (returns ok=false) since intra-chunk lookups happen via the
	// heightTracker before reaching the coordinator.
	FindTx(ctx context.Context, txHash string) (height uint64, ok bool, err error)

	// GlobalWatermark returns the highest height H such that all chunks
	// ending at or before H are complete. Used by WaitForTx as a bailout
	// signal: if a tx isn't published and the global watermark has passed
	// the calling worker's height, the tx is from before the replay range
	// (or this worker is a straggler) and the caller can fall through.
	GlobalWatermark(ctx context.Context) (uint64, error)

	// Close releases any resources (e.g. stops a heartbeat goroutine).
	Close() error
}

// ErrNoMoreChunks is the conventional sentinel some implementations may
// return from ClaimChunk; callers should treat ok=false the same way.
var ErrNoMoreChunks = errors.New("no more chunks available")

// LeaseTTL is the default lease duration for DDB-backed coordinators.
// A worker must call Heartbeat at less than this interval; otherwise the
// chunk is considered abandoned and another worker may reclaim it.
const LeaseTTL = 5 * time.Minute
