package syncV2Consumer

import (
	"context"
	"reflect"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/rs/zerolog"
)

func TestProcessEventPreservesTopLevelCallbackIndices(t *testing.T) {
	tx := loadMusashiTransaction(t)
	// A distinct ordinary transaction beside the original fixture exercises
	// both callback indices. This synthetic block is not consensus-validated.
	ordinary := loadMusashiTransaction(t)
	ordinary.Body.TxSubTransactions = cbor.SetType[dijkstra.DijkstraSubTransaction]{}
	ordinary.Body.TxFee++
	ordinary.Body.SetCbor(nil)
	ordinary.SetCbor(nil)
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{},
		BlockBody: dijkstra.DijkstraBlockBody{
			Transactions: []dijkstra.DijkstraTransaction{*tx, *ordinary},
		},
	}
	fillHeaderPlaceholders(block.BlockHeader)
	block.BlockHeader.Body.Slot = 12345
	raw, err := cbor.Encode([]any{dijkstra.BlockTypeDijkstra, block})
	if err != nil {
		t.Fatal(err)
	}
	contents := func() chan downloadResult {
		ch := make(chan downloadResult, 1)
		ch <- downloadResult{bytes: raw}
		return ch
	}
	event := Message{
		Undo:    []Block{{Contents: contents()}},
		Advance: Block{Contents: contents()},
	}
	var undoIndices, advanceIndices []int
	var advancedIDs, undoneIDs []string
	var ordinaryID string
	syncer := &Syncer{Logger: zerolog.Nop()}
	err = syncer.processEvent(context.Background(), event,
		func(_ context.Context, tx ledger.Transaction, slot uint64, index int) error {
			undoIndices = append(undoIndices, index)
			if slot != 12345 {
				t.Errorf("undo slot %d, want 12345", slot)
			}
			if index == 1 {
				ordinaryID = tx.Hash().String()
			}
			return WalkTransactionReverse(tx, func(view TransactionView) error {
				undoneIDs = append(undoneIDs, view.ID.String())
				return nil
			})
		},
		func(_ context.Context, tx ledger.Transaction, slot uint64, index int) error {
			advanceIndices = append(advanceIndices, index)
			if slot != 12345 {
				t.Errorf("advance slot %d, want 12345", slot)
			}
			return WalkTransaction(tx, func(view TransactionView) error {
				advancedIDs = append(advancedIDs, view.ID.String())
				return nil
			})
		})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(undoIndices, []int{1, 0}) || !reflect.DeepEqual(advanceIndices, []int{0, 1}) {
		t.Fatalf("wrong callback indices: undo=%v advance=%v", undoIndices, advanceIndices)
	}
	want := []string{musashiSubTxID, musashiTxID, ordinaryID}
	if ordinaryID == musashiTxID || !reflect.DeepEqual(advancedIDs, want) {
		t.Fatalf("visited IDs %v, want %v", advancedIDs, want)
	}
	wantUndo := []string{ordinaryID, musashiTxID, musashiSubTxID}
	if !reflect.DeepEqual(undoneIDs, wantUndo) {
		t.Fatalf("undone IDs %v, want %v", undoneIDs, wantUndo)
	}
}

// fillHeaderPlaceholders gives a synthetic header the fixed-length fields
// gouroboros validates when decoding (>= v0.218). The values are zero bytes;
// these blocks are not consensus-validated.
func fillHeaderPlaceholders(h *dijkstra.DijkstraBlockHeader) {
	h.Body.VrfKey = make([]byte, 32)
	h.Body.VrfResult.Output = make([]byte, 64)
	h.Body.VrfResult.Proof = make([]byte, 80)
	h.Body.OpCert.HotVkey = make([]byte, 32)
	h.Body.OpCert.Signature = make([]byte, 64)
}

// TestDecodeBlockSkipsBodyHash guards against the RunOne regression: a block
// whose header body hash doesn't match its body (as with Dolos-served blocks,
// which keep the original header but inline EB transactions) must still
// decode through decodeBlock.
func TestDecodeBlockSkipsBodyHash(t *testing.T) {
	tx := loadMusashiTransaction(t)
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{},
		BlockBody: dijkstra.DijkstraBlockBody{
			Transactions: []dijkstra.DijkstraTransaction{*tx},
		},
	}
	// Placeholder header: its body hash is zeroed, so it can't match the body.
	fillHeaderPlaceholders(block.BlockHeader)
	raw, err := cbor.Encode([]any{dijkstra.BlockTypeDijkstra, block})
	if err != nil {
		t.Fatal(err)
	}

	// Precondition: strict decoding rejects this block, so the test really
	// exercises the body-hash mismatch path.
	if _, err := ledger.NewBlockFromCbor(uint(raw[1]), raw[2:]); err == nil {
		t.Fatal("expected strict decode to fail on mismatched body hash")
	}

	decoded, err := decodeBlock(raw)
	if err != nil {
		t.Fatalf("decodeBlock: %v", err)
	}
	txs := decoded.Transactions()
	if len(txs) != 1 || txs[0].Hash().String() != musashiTxID {
		t.Fatalf("unexpected transactions after decode: %d", len(txs))
	}
}

func TestDecodeBlockRejectsShortPayload(t *testing.T) {
	for _, raw := range [][]byte{nil, {0x82}} {
		if _, err := decodeBlock(raw); err == nil {
			t.Errorf("decodeBlock(%x): expected error, got nil", raw)
		}
	}
}
