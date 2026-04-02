#!/bin/bash
set -e

TX=${1}
WALLET=${2:-scooper-0}

if [[ ! -f "./$TX" ]]; then
  echo "Specify a Tx file!"
  exit
fi

cardano-cli transaction sign --tx-body-file "${TX}" --signing-key-file "${WALLET}/${WALLET}.skey" --out-file "${TX}.signed"

TX_ID=$(cardano-cli transaction txid --tx-body-file "${TX}")

cardano-cli transaction submit --tx-file "${TX}.signed" --testnet-magic 2

echo "TxID: ${TX_ID}"

./wait-for-tx.sh "${TX_ID}"
