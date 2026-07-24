package cardano

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/fxamacker/cbor/v2"
	"golang.org/x/crypto/blake2b"
)

// ComputeOwnerHashesFromAddress computes the ownerHashes that an order from a
// given bech32 Cardano address may be stored under. V1 escrow datums store the
// owner as the payment key hash; V3/Stable/V4 order datums use the stake key
// hash. To find (or subscribe to) every order belonging to a wallet we have to
// consider both. The returned slice contains the payment-key-based hash first,
// then the stake-key-based hash if present (some script/enterprise addresses
// have no stake credential).
//
// The hash is blake2b-256 of the CBOR-serialised single-signature multisig
// wrapping the credential — the same encoding the order indexers write to the
// Order.OwnerHash field, so these values match the ByOwner GSI keys and the
// `orderUpdate:{ownerHash}` WebSocket topics.
func ComputeOwnerHashesFromAddress(address string) ([]string, error) {
	payment, staking, err := SplitAddress(address)
	if err != nil {
		return nil, fmt.Errorf("split address: %w", err)
	}
	if payment == nil {
		return nil, fmt.Errorf("address has no payment credential: %s", address)
	}

	hashes := make([]string, 0, 2)
	for _, key := range [][]byte{payment, staking} {
		if key == nil {
			continue
		}
		cborBytes, err := marshalSignatureMultisig(key)
		if err != nil {
			return nil, fmt.Errorf("marshal multisig: %w", err)
		}
		h := blake2b.Sum256(cborBytes)
		hashes = append(hashes, hex.EncodeToString(h[:]))
	}
	return hashes, nil
}

// marshalSignatureMultisig encodes a single-signature multisig as
// Constr(0, [keyHash]) — CBOR tag 121 wrapping an indefinite-length array with
// the key hash. This mirrors sundaedatum/multisig's MarshalCBOR for the
// signature case; it's reproduced here so this shared package doesn't take a
// dependency on sundaedatum.
func marshalSignatureMultisig(keyHash []byte) ([]byte, error) {
	em, err := cbor.EncOptions{}.EncMode()
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	enc := em.NewEncoder(&buf)
	if err := enc.StartIndefiniteArray(); err != nil {
		return nil, err
	}
	if err := enc.Encode(keyHash); err != nil {
		return nil, err
	}
	if err := enc.EndIndefinite(); err != nil {
		return nil, err
	}
	return em.Marshal(cbor.RawTag{Number: 121, Content: buf.Bytes()})
}
