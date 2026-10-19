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
	var advancedIDs []string
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
			return nil
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
}
