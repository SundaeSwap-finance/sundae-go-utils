package syncV2Consumer

import (
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

const (
	musashiTxID    = "ca89430ca01c3202ce2cc8428c53f91a4f34d618421ca510dd39f3d76c540aca"
	musashiSubTxID = "ede49f69c8e99d253d82b0b06d024be6435168cfeb1b960761370fe1bf5f8377"
)

func loadMusashiTransaction(t *testing.T) *dijkstra.DijkstraTransaction {
	t.Helper()
	text, err := os.ReadFile("testdata/musashi-ca89430c.hex")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := hex.DecodeString(strings.TrimSpace(string(text)))
	if err != nil {
		t.Fatal(err)
	}
	// The fixture is a block transaction: [body, witnesses, aux, is_valid].
	bodyCBOR, err := cbor.Encode([]any{[]cbor.RawMessage{raw}, nil, nil})
	if err != nil {
		t.Fatal(err)
	}
	var body dijkstra.DijkstraBlockBody
	if _, err := cbor.Decode(bodyCBOR, &body); err != nil {
		t.Fatal(err)
	}
	return &body.Transactions[0]
}

func TestWalkTransactionMusashi(t *testing.T) {
	tx := loadMusashiTransaction(t)
	var views []TransactionView
	if err := WalkTransaction(tx, func(v TransactionView) error {
		views = append(views, v)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(views) != 2 {
		t.Fatalf("got %d views, want sub-transaction then parent", len(views))
	}
	for i, want := range []struct {
		id         string
		inputIndex uint32
		outputs    int
		vkey       string
	}{
		{musashiSubTxID, 3, 2, "512dfddfd9b00134d3c8d61ea10afa6e7677eccb76a422ca6b96b3927363b065"},
		{musashiTxID, 2, 1, "633e181632610cfee84cd947925cd71e1eeaa49e64c1ee1119931c2577522afe"},
	} {
		v := views[i]
		if v.ID.String() != want.id || v.Body.Id() != v.ID {
			t.Errorf("view %d: wrong ID: %s (body %s)", i, v.ID, v.Body.Id())
		}
		inputs := v.Body.Inputs()
		if len(inputs) != 1 || inputs[0].Index() != want.inputIndex {
			t.Errorf("view %d: wrong inputs: %v", i, inputs)
		}
		if len(v.Body.Outputs()) != want.outputs {
			t.Errorf("view %d: got %d outputs, want %d", i, len(v.Body.Outputs()), want.outputs)
		}
		keys := v.Witnesses.Vkey()
		if len(keys) != 1 || hex.EncodeToString(keys[0].Vkey) != want.vkey {
			t.Errorf("view %d: wrong witness set", i)
		}
	}
	if views[0].Body.Outputs()[0].Datum() == nil {
		t.Fatal("sub-transaction order output lost its inline datum")
	}
	var produced []string
	for _, utxo := range tx.Produced() {
		produced = append(produced, utxo.Id.String())
	}
	want := []string{musashiSubTxID + "#0", musashiSubTxID + "#1", musashiTxID + "#0"}
	if !reflect.DeepEqual(produced, want) {
		t.Fatalf("produced %v, want %v", produced, want)
	}
}

func TestWalkTransactionKeepsRedeemersAndDatumsLocal(t *testing.T) {
	tx := loadMusashiTransaction(t)
	subs := tx.Body.TxSubTransactions.Items()
	sub := &subs[0]
	// The real fixture has only key witnesses. Add distinct witness datums
	// and spend redeemers at index 0 to both levels to catch accidental merging.
	for i, ws := range []*dijkstra.DijkstraTransactionWitnessSet{&sub.WitnessSet, &tx.WitnessSet} {
		var datum common.Datum
		if _, err := cbor.Decode([]byte{byte(i + 1)}, &datum); err != nil {
			t.Fatal(err)
		}
		ws.WsPlutusData = cbor.NewSetType([]common.Datum{datum}, true)
		ws.WsRedeemers = dijkstra.DijkstraRedeemers{
			Redeemers: map[common.RedeemerKey]common.RedeemerValue{
				{Tag: common.RedeemerTagSpend, Index: 0}: {Data: datum},
			},
		}
	}
	tx.Body.TxSubTransactions = cbor.NewSetType(subs, true)
	var calls int
	err := WalkTransaction(tx, func(v TransactionView) error {
		want := []byte{byte(calls + 1)}
		datums := v.Witnesses.PlutusData()
		if len(datums) != 1 || !reflect.DeepEqual(datums[0].Cbor(), want) {
			t.Errorf("view %d: wrong witness datums", calls)
		}
		redeemers := v.Witnesses.Redeemers()
		if got := redeemers.Indexes(common.RedeemerTagSpend); !reflect.DeepEqual(got, []uint{0}) {
			t.Errorf("view %d: redeemer indices changed: %v", calls, got)
		}
		if got := redeemers.Value(0, common.RedeemerTagSpend).Data.Cbor(); !reflect.DeepEqual(got, want) {
			t.Errorf("view %d: wrong redeemer datum: %x", calls, got)
		}
		if len(v.Body.Inputs()) != 1 {
			t.Errorf("view %d: inputs were combined", calls)
		}
		calls++
		return nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

// Embedding just the ledger interface hides the optional sub-transaction
// interfaces, as with transactions from eras that do not support them.
type topLevelOnly struct{ ledger.Transaction }

type transactionWithParts struct {
	ledger.Transaction
	bodies    []common.TransactionBody
	witnesses []common.TransactionWitnessSet
}

func (tx transactionWithParts) SubTransactionBodies() []common.TransactionBody {
	return tx.bodies
}

func (tx transactionWithParts) SubTransactionWitnessSets() []common.TransactionWitnessSet {
	return tx.witnesses
}

func TestWalkTransactionWithoutSubTransactions(t *testing.T) {
	tx := topLevelOnly{loadMusashiTransaction(t)}
	calls := 0
	err := WalkTransaction(tx, func(v TransactionView) error {
		calls++
		if v.ID != tx.Hash() || v.Body != tx {
			t.Error("expected the original top-level transaction")
		}
		return nil
	})
	if err != nil || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestWalkTransactionOrderAndErrors(t *testing.T) {
	tx := loadMusashiTransaction(t)
	parts := transactionWithParts{
		Transaction: tx,
		bodies:      []common.TransactionBody{tx.SubTransactionBodies()[0], tx},
		witnesses:   []common.TransactionWitnessSet{tx.SubTransactionWitnessSets()[0], tx.Witnesses()},
	}
	// Synthetic multiple-body provider tests order and every stopping position,
	// including propagation of a top-level visitor error.
	for stop := 0; stop <= 3; stop++ {
		sentinel := errors.New("visitor failed")
		var ids []string
		err := WalkTransaction(parts, func(v TransactionView) error {
			ids = append(ids, v.ID.String())
			if len(ids) == stop {
				return sentinel
			}
			return nil
		})
		want := []string{musashiSubTxID, musashiTxID, musashiTxID}
		if stop > 0 {
			want = want[:stop]
			if err != sentinel {
				t.Errorf("stop %d: got %v, want original visitor error", stop, err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(ids, want) {
			t.Errorf("stop %d: visited %v, want %v", stop, ids, want)
		}
	}
	for _, witnesses := range [][]common.TransactionWitnessSet{nil, {tx.Witnesses(), tx.Witnesses(), tx.Witnesses()}} {
		parts.witnesses = witnesses
		err := WalkTransaction(parts, func(TransactionView) error {
			t.Fatal("visited a transaction with mismatched bodies and witnesses")
			return nil
		})
		if err == nil {
			t.Error("expected mismatched body/witness count error")
		}
	}
	if err := WalkTransaction(nil, nil); err == nil {
		t.Error("expected nil transaction error")
	}
}

func TestWalkTransactionDoesNotFilterInvalidBodies(t *testing.T) {
	tx := loadMusashiTransaction(t)
	tx.TxIsValid = false
	calls := 0
	if err := WalkTransaction(tx, func(TransactionView) error {
		calls++
		return nil
	}); err != nil || calls != 2 {
		t.Fatalf("structural traversal: calls=%d err=%v", calls, err)
	}
	if len(tx.Produced()) != 0 {
		t.Fatal("invalid transaction without collateral return must produce no UTxOs")
	}
}
