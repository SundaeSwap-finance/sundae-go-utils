package replay

import (
	"context"
	"testing"
)

// TestInMemoryCoordinator_OneShot verifies the in-memory coordinator hands
// out exactly one chunk and then reports no more work — preserving the
// original single-machine Replayer.Run behavior.
func TestInMemoryCoordinator_OneShot(t *testing.T) {
	c := NewInMemoryCoordinator(100, 200)
	defer c.Close()

	start, end, ok, err := c.ClaimChunk(context.Background())
	if err != nil {
		t.Fatalf("first ClaimChunk: %v", err)
	}
	if !ok {
		t.Fatalf("first ClaimChunk should succeed")
	}
	if start != 100 || end != 200 {
		t.Errorf("got chunk [%d, %d), want [100, 200)", start, end)
	}

	_, _, ok, err = c.ClaimChunk(context.Background())
	if err != nil {
		t.Fatalf("second ClaimChunk: %v", err)
	}
	if ok {
		t.Errorf("second ClaimChunk should report no more work")
	}
}

// TestInMemoryCoordinator_OpenEnded verifies open-ended mode (end=0) returns
// end=0 from the chunk so produceHeights can run until chain tip.
func TestInMemoryCoordinator_OpenEnded(t *testing.T) {
	c := NewInMemoryCoordinator(100, 0)
	defer c.Close()

	start, end, ok, err := c.ClaimChunk(context.Background())
	if err != nil || !ok {
		t.Fatalf("ClaimChunk: ok=%v err=%v", ok, err)
	}
	if start != 100 || end != 0 {
		t.Errorf("got chunk [%d, %d), want [100, 0)", start, end)
	}
}

// TestInMemoryCoordinator_Stubs verifies the no-op coordinator methods
// don't error and return sensible zero values.
func TestInMemoryCoordinator_Stubs(t *testing.T) {
	c := NewInMemoryCoordinator(0, 100)
	ctx := context.Background()

	if err := c.PublishTx(ctx, "abc", 5); err != nil {
		t.Errorf("PublishTx: %v", err)
	}
	if _, ok, err := c.FindTx(ctx, "abc"); err != nil || ok {
		t.Errorf("FindTx: ok=%v err=%v (want false, nil)", ok, err)
	}
	if err := c.Heartbeat(ctx, 0, 100); err != nil {
		t.Errorf("Heartbeat: %v", err)
	}
}
