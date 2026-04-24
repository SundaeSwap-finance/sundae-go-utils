package replay

import (
	"context"
	"sync"
)

// InMemoryCoordinator hands out the entire replay range [start, end) as a
// single chunk. After that one chunk is claimed, ClaimChunk returns ok=false.
//
// Intra-chunk dependency tracking is handled by the existing heightTracker
// inside the Replayer — this coordinator's PublishTx / FindTx / GlobalWatermark
// are all no-op fallbacks (the heightTracker resolves everything before the
// Replayer would ever reach this coordinator).
//
// This preserves the original single-machine behavior with zero coordination
// overhead.
type InMemoryCoordinator struct {
	start, end uint64

	mu      sync.Mutex
	claimed bool
	done    bool
}

// NewInMemoryCoordinator creates a single-chunk coordinator covering [start, end).
// If end is 0, the chunk is open-ended — the Replayer's produceHeights will
// stop when it hits maxConsecutiveMisses (chain tip).
func NewInMemoryCoordinator(start, end uint64) *InMemoryCoordinator {
	return &InMemoryCoordinator{start: start, end: end}
}

func (c *InMemoryCoordinator) ClaimChunk(ctx context.Context) (uint64, uint64, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.claimed {
		return 0, 0, false, nil
	}
	c.claimed = true
	return c.start, c.end, true, nil
}

func (c *InMemoryCoordinator) CompleteChunk(ctx context.Context, start, end uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.done = true
	return nil
}

func (c *InMemoryCoordinator) Heartbeat(ctx context.Context, start, end uint64) error {
	return nil
}

func (c *InMemoryCoordinator) PublishTx(ctx context.Context, txHash string, height uint64) error {
	return nil
}

func (c *InMemoryCoordinator) FindTx(ctx context.Context, txHash string) (uint64, bool, error) {
	return 0, false, nil
}

func (c *InMemoryCoordinator) GlobalWatermark(ctx context.Context) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done {
		// All work is done — a tx not in the in-memory tracker must be pre-range.
		// Return a watermark high enough to trigger bailout in callers.
		if c.end > 0 {
			return c.end - 1, nil
		}
	}
	// Return 0 → callers' bailout uses currentHeight-1 <= globalWatermark,
	// which the heightTracker handles internally without needing this value.
	return 0, nil
}

func (c *InMemoryCoordinator) Close() error {
	return nil
}
