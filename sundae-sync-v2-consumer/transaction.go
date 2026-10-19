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
// original block transaction index. Call this helper inside those callbacks;
// use reverse visit order when undoing order-dependent effects.
func WalkTransaction(tx ledger.Transaction, visit func(TransactionView) error) error {
	if tx == nil {
		return fmt.Errorf("walk transaction: nil transaction")
	}
	bodies := common.SubTransactionBodiesFromTransaction(tx)
	witnesses := common.SubTransactionWitnessSetsFromTransaction(tx)
	if len(bodies) != len(witnesses) {
		return fmt.Errorf("transaction %s: %d sub-transaction bodies but %d witness sets",
			tx.Hash(), len(bodies), len(witnesses))
	}
	for i, body := range bodies {
		if err := visit(TransactionView{
			ID:        body.Id(),
			Body:      body,
			Witnesses: witnesses[i],
		}); err != nil {
			return err
		}
	}
	return visit(TransactionView{
		ID:        tx.Hash(),
		Body:      tx,
		Witnesses: tx.Witnesses(),
	})
}
