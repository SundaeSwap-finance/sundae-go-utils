package replay

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/rs/zerolog"
)

const (
	musashiTxID    = "ca89430ca01c3202ce2cc8428c53f91a4f34d618421ca510dd39f3d76c540aca"
	musashiSubTxID = "ede49f69c8e99d253d82b0b06d024be6435168cfeb1b960761370fe1bf5f8377"
)

func encodeCBOR(t *testing.T, value any) cbor.RawMessage {
	t.Helper()
	raw, err := cbor.Encode(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func musashiTransactionCBOR(t *testing.T) cbor.RawMessage {
	t.Helper()
	text, err := os.ReadFile("../testdata/musashi-ca89430c.hex")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := hex.DecodeString(strings.TrimSpace(string(text)))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func blockCBOR(t *testing.T, txs ...cbor.RawMessage) []byte {
	t.Helper()
	header := &dijkstra.DijkstraBlockHeader{}
	header.Body.BlockNumber = 101
	header.Body.Slot = 12345
	body := encodeCBOR(t, []any{txs, nil, nil})
	header.Body.BlockBodyHash = common.Blake2b256Hash(body)
	header.Body.BlockBodySize = uint64(len(body))
	block := encodeCBOR(t, []any{header, body})
	return encodeCBOR(t, []any{dijkstra.BlockTypeDijkstra, block})
}

type memoryBlockSource []byte

func (b memoryBlockSource) FetchBlock(context.Context, string, string) ([]byte, error) {
	return b, nil
}

type recordingCoordinator struct {
	Coordinator
	published []string
}

func (c *recordingCoordinator) PublishTx(_ context.Context, hash string, _ uint64) error {
	c.published = append(c.published, hash)
	return nil
}

func fixtureReplayer(block []byte, advance AdvanceFunc) *Replayer {
	r := NewWithCoordinator(nil, Config{BlockSource: memoryBlockSource(block)}, advance,
		&recordingCoordinator{Coordinator: NewInMemoryCoordinator(100, 200)}, zerolog.Nop())
	// Leave height 100 incomplete, so processing 101 cannot advance the
	// watermark. Missing transaction IDs must not pass via the bailout.
	r.tracker = newHeightTracker(100)
	return r
}

func TestProcessHeightUnblocksMusashiProducingIDs(t *testing.T) {
	block := blockCBOR(t, musashiTransactionCBOR(t))
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		r := fixtureReplayer(block, func(ctx context.Context, tx ledger.Transaction, slot uint64, index int) error {
			calls++
			if tx.Hash().String() != musashiTxID || slot != 12345 || index != 0 {
				t.Errorf("wrong callback identity: %s slot=%d index=%d", tx.Hash(), slot, index)
			}
			tracker := ctx.Value(replayContextKey{}).(*heightTracker)
			tracker.mu.Lock()
			defer tracker.mu.Unlock()
			if len(tracker.txToHeight) != 0 {
				t.Error("transaction marked processed before its callback returned")
			}
			return nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ctx = context.WithValue(ctx, replayContextKey{}, r.tracker)
		ctx = context.WithValue(ctx, replayHeightKey{}, uint64(102))
		results := make(chan error, 2)
		for _, id := range []string{musashiTxID, musashiSubTxID} {
			go func() { results <- WaitForTx(ctx, id) }()
		}
		synctest.Wait()
		if len(results) != 0 {
			t.Fatal("waits resolved before processing the transaction")
		}
		if err := r.processHeight(context.Background(), heightRecord{Height: 101}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		for range 2 {
			select {
			case err := <-results:
				if err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatal("producing ID is still waiting after its callback succeeded")
			}
		}
		if calls != 1 {
			t.Errorf("got %d callbacks, want one top-level callback", calls)
		}
		if wm := r.tracker.Watermark(); wm != 99 {
			t.Errorf("watermark advanced to %d", wm)
		}
		if stats := r.tracker.WaitStats(); stats.TxBlocked != 2 || stats.TxBailouts != 0 {
			t.Errorf("waits did not resolve through transaction completion: %+v", stats)
		}
		r.tracker.mu.Lock()
		recorded := len(r.tracker.heightTxs[101])
		r.tracker.mu.Unlock()
		if recorded != 2 {
			t.Errorf("recorded %d IDs, want exactly two despite three outputs", recorded)
		}
		if got := r.coordinator.(*recordingCoordinator).published; len(got) != 0 {
			t.Errorf("local completion unexpectedly published IDs: %v", got)
		}
	})
}

func TestProcessHeightDoesNotCompleteFailedCallback(t *testing.T) {
	block := blockCBOR(t, musashiTransactionCBOR(t))
	sentinel := errors.New("indexer failed")
	for _, callbackErr := range []error{sentinel, context.Canceled} {
		r := fixtureReplayer(block, func(context.Context, ledger.Transaction, uint64, int) error {
			return callbackErr
		})
		err := r.processHeight(context.Background(), heightRecord{Height: 101})
		if !errors.Is(err, callbackErr) {
			t.Fatalf("got %v, want callback error %v", err, callbackErr)
		}
		if len(r.tracker.txToHeight) != 0 || len(r.tracker.completed) != 0 {
			t.Fatal("failed callback marked transaction IDs or height completed")
		}
	}
}

func TestProcessHeightHonorsCancellation(t *testing.T) {
	r := fixtureReplayer(blockCBOR(t, musashiTransactionCBOR(t)),
		func(context.Context, ledger.Transaction, uint64, int) error {
			t.Fatal("callback ran with cancelled context")
			return nil
		})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.processHeight(ctx, heightRecord{Height: 101}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if len(r.tracker.txToHeight) != 0 || len(r.tracker.completed) != 0 {
		t.Fatal("cancelled processing marked transaction IDs or height completed")
	}
}

// mutateTransaction changes only the requested fields, retaining the original
// sub-transaction bytes. These are decoding fixtures, not valid signed spends.
func mutateTransaction(t *testing.T, change func(map[uint]cbor.RawMessage, []cbor.RawMessage)) cbor.RawMessage {
	t.Helper()
	var parts []cbor.RawMessage
	if _, err := cbor.Decode(musashiTransactionCBOR(t), &parts); err != nil {
		t.Fatal(err)
	}
	var body map[uint]cbor.RawMessage
	if _, err := cbor.Decode(parts[0], &body); err != nil {
		t.Fatal(err)
	}
	change(body, parts)
	parts[0] = encodeCBOR(t, body)
	return encodeCBOR(t, parts)
}

func TestProcessHeightCompletionUsesLedgerEffects(t *testing.T) {
	for _, tc := range []struct {
		name        string
		change      func(map[uint]cbor.RawMessage, []cbor.RawMessage)
		outputs     int
		producesSub bool
	}{
		{
			name: "ordinary transaction",
			change: func(body map[uint]cbor.RawMessage, _ []cbor.RawMessage) {
				delete(body, 23)
			},
			outputs: 1,
		},
		{
			name: "parent without outputs",
			change: func(body map[uint]cbor.RawMessage, _ []cbor.RawMessage) {
				body[1] = encodeCBOR(t, []any{})
			},
			outputs: 2, producesSub: true,
		},
		{
			name: "invalid without collateral return",
			change: func(_ map[uint]cbor.RawMessage, parts []cbor.RawMessage) {
				parts[3] = encodeCBOR(t, false)
			},
		},
		{
			name: "invalid with collateral return",
			change: func(body map[uint]cbor.RawMessage, parts []cbor.RawMessage) {
				parts[3] = encodeCBOR(t, false)
				var outputs []cbor.RawMessage
				if _, err := cbor.Decode(body[1], &outputs); err != nil {
					t.Fatal(err)
				}
				body[16] = outputs[0]
				body[13] = body[0]
			},
			outputs: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := mutateTransaction(t, tc.change)
			var parentID string
			r := fixtureReplayer(blockCBOR(t, raw), func(_ context.Context, tx ledger.Transaction, _ uint64, _ int) error {
				parentID = tx.Hash().String()
				if got := len(tx.Produced()); got != tc.outputs {
					t.Errorf("produced %d UTxOs, want %d", got, tc.outputs)
				}
				if !tx.IsValid() && tc.outputs == 1 && tx.Produced()[0].Id.Index() != uint32(len(tx.Outputs())) {
					t.Error("wrong collateral return index")
				}
				return nil
			})
			if err := r.processHeight(context.Background(), heightRecord{Height: 101}); err != nil {
				t.Fatal(err)
			}
			want := map[string]uint64{parentID: 101}
			if tc.producesSub {
				want[musashiSubTxID] = 101
			}
			if !reflect.DeepEqual(r.tracker.txToHeight, want) {
				t.Fatalf("completed IDs %v, want %v", r.tracker.txToHeight, want)
			}
		})
	}
}

func TestWaitForMissingSubTransactionCanBeCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tracker := newHeightTracker(100)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ctx = context.WithValue(ctx, replayContextKey{}, tracker)
		ctx = context.WithValue(ctx, replayHeightKey{}, uint64(102))
		done := make(chan error, 1)
		go func() { done <- WaitForTx(ctx, musashiSubTxID) }()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	})
}

func TestProcessHeightWaitsOnSubTransactionFromSameBlock(t *testing.T) {
	subID, err := hex.DecodeString(musashiSubTxID)
	if err != nil {
		t.Fatal(err)
	}
	spend := mutateTransaction(t, func(body map[uint]cbor.RawMessage, _ []cbor.RawMessage) {
		delete(body, 23)
		delete(body, 18)
		body[0] = encodeCBOR(t, []any{[]any{subID, uint32(0)}})
	})
	block := blockCBOR(t, musashiTransactionCBOR(t), spend)
	synctest.Test(t, func(t *testing.T) {
		var indices []int
		var spendID string
		r := fixtureReplayer(block, func(ctx context.Context, tx ledger.Transaction, _ uint64, index int) error {
			indices = append(indices, index)
			switch index {
			case 0:
				// Distributed publication remains opt-in, using the producing
				// ID rather than the enclosing transaction's hash.
				return PublishRelevantTx(ctx, musashiSubTxID)
			case 1:
				spendID = tx.Hash().String()
				return WaitForTx(ctx, tx.Inputs()[0].Id().String())
			default:
				t.Fatalf("unexpected callback index %d", index)
				return nil
			}
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- r.processHeight(ctx, heightRecord{Height: 101}) }()
		synctest.Wait()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("later transaction blocked on an already processed sub-transaction")
		}
		if !reflect.DeepEqual(indices, []int{0, 1}) {
			t.Errorf("callback indices %v, want [0 1]", indices)
		}
		if got := r.coordinator.(*recordingCoordinator).published; !reflect.DeepEqual(got, []string{musashiSubTxID}) {
			t.Errorf("published IDs %v, want only the explicitly published sub-transaction", got)
		}
		if r.tracker.txToHeight[spendID] != 101 {
			t.Error("later transaction was not marked complete")
		}
		if stats := r.tracker.WaitStats(); stats.TxBlocked != 0 || stats.TxBailouts != 0 {
			t.Errorf("same-block wait did not resolve locally: %+v", stats)
		}
		// Once the gap closes, cleanup must prune parent and child IDs alike.
		r.tracker.MarkDone(100)
		if r.tracker.Watermark() != 101 || len(r.tracker.txToHeight) != 0 || len(r.tracker.heightTxs) != 0 {
			t.Fatal("watermark cleanup retained completed transaction IDs")
		}
	})
}
