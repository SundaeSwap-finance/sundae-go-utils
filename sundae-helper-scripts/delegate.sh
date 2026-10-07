#!/bin/bash

set -e

WALLET=${1}
WALLET_STAKE_VKEY="$WALLET/$WALLET-stake.vkey"
WALLET_ADDR=$(cat $WALLET/$WALLET.addr)

POOL="./stake-pool/stake-pool.vkey"

TXIN=$(cardano-cli query utxo --testnet-magic 2 --address ${WALLET_ADDR} | sort -nrk3 | awk 'BEGIN{OFS="#"} { if (NF <= 6 && $6 == "TxOutDatumNone") {print $1,$2} }' | head -n 1)
AMT_LOVELACE=$((AMT_ADA*1000000))

echo $TXIN

cardano-cli stake-address registration-certificate --stake-verification-key-file "$WALLET_STAKE_VKEY" --out-file "$WALLET/registration.cert"

cardano-cli stake-address delegation-certificate --stake-verification-key-file "$WALLET_STAKE_VKEY" --cold-verification-key-file "$POOL" --out-file "$WALLET/delegation.cert"

cardano-cli transaction build \
  --testnet-magic 2 \
  --tx-in "${TXIN}" \
  --change-address "${WALLET_ADDR}" \
  --witness-override 2 \
  --certificate-file "$WALLET/registration.cert" \
  --certificate-file "$WALLET/delegation.cert" \
  --out-file "tmp-delegate.json"

cardano-cli transaction sign \
  --tx-body-file "tmp-delegate.json" \
  --signing-key-file "${WALLET}/${WALLET}.skey" \
  --signing-key-file "${WALLET}/${WALLET}-stake.skey" \
  --out-file "tmp-delegate.signed.json"

cardano-cli transaction submit \
  --testnet-magic 2 \
  --tx-file "tmp-delegate.signed.json"

TX_ID=$(cardano-cli transaction txid --tx-body-file tmp-delegate.json)
echo "TxID: ${TX_ID}"

./wait-for-tx.sh "${TX_ID}"

rm tmp-delegate.*
