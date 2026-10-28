package syncV2Consumer

import (
	"fmt"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
)

// TransactionView pairs one transaction body's ID, inputs and outputs with its
// own witnesses. Redeemer indices are relative to Body.Inputs(), not a combined
// list of inputs from the enclosing transaction and its sub-transactions.
// Witnesses.PlutusData() supplies this body's witness datums; inline datums
// remain on the body's outputs.
type TransactionView struct {
	ID        common.Blake2b256
	Body      common.TransactionBody
	Witnesses common.TransactionWitnessSet
}

// WalkTransaction visits sub-transactions in ledger order, then the top-level
// transaction. Transactions without sub-transactions are visited once. It stops
// at the first visitor error, returning that error unchanged. Inconsistent
// sub-transaction body/witness counts are rejected before any visits.
//
// This is a structural traversal, not a ledger-effects filter: it also visits
// the bodies of phase-2-invalid transactions. Indexers must check tx.IsValid()
// before indexing regular outputs; tx.Produced() and tx.Consumed() describe
// actual ledger effects, including collateral for invalid transactions.
//
// AdvanceFunc and UndoFunc still run once per top-level transaction with the
// original block transaction index. Call this helper inside advance callbacks;
// use WalkTransactionReverse inside undo callbacks for order-dependent effects.
func WalkTransaction(tx ledger.Transaction, visit func(TransactionView) error) error {
	return walkTransaction(tx, visit, false)
}

// WalkTransactionReverse visits the top-level transaction first, then its
// sub-transactions in reverse ledger order: the exact reverse of WalkTransaction.
// Use it inside UndoFunc when undoing order-dependent effects. Transactions
// without sub-transactions are visited once.
//
// Like WalkTransaction, it performs structural traversal without filtering
// invalid transactions, rejects mismatched body/witness counts before any
// visits, and stops at the first visitor error, returning it unchanged.
func WalkTransactionReverse(tx ledger.Transaction, visit func(TransactionView) error) error {
	return walkTransaction(tx, visit, true)
}

func walkTransaction(tx ledger.Transaction, visit func(TransactionView) error, reverse bool) error {
	if tx == nil {
		return fmt.Errorf("walk transaction: nil transaction")
	}
	bodies := common.SubTransactionBodiesFromTransaction(tx)
	witnesses := common.SubTransactionWitnessSetsFromTransaction(tx)
	if len(bodies) != len(witnesses) {
		return fmt.Errorf("transaction %s: %d sub-transaction bodies but %d witness sets",
			tx.Hash(), len(bodies), len(witnesses))
	}
	// Positions [0, len(bodies)) are children; the final position is the parent.
	// Reverse only the positions, keeping each body paired with its witnesses.
	for position := 0; position <= len(bodies); position++ {
		i := position
		if reverse {
			i = len(bodies) - position
		}
		var view TransactionView
		if i == len(bodies) {
			view = TransactionView{
				ID:        tx.Hash(),
				Body:      tx,
				Witnesses: tx.Witnesses(),
			}
		} else {
			view = TransactionView{
				ID:        bodies[i].Id(),
				Body:      bodies[i],
				Witnesses: witnesses[i],
			}
		}
		if err := visit(view); err != nil {
			return err
		}
	}
	return nil
}
