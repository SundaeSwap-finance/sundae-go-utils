#!/bin/bash
set -e

SRC=${1}
DEST=${2}
AMT_ADA=${3:-500}

if [[ $SRC == addr* ]]; then
	SRC_ADDR=$SRC
else
  SRC_ADDR=$(cat $SRC/$SRC.addr)
fi

if [[ $DEST == addr* ]]; then
  DEST_ADDR=$DEST
else
  DEST_ADDR=$(cat $DEST/$DEST.addr)
fi

AMT_LOVELACE=$((AMT_ADA*1000000))
AMT_SO_FAR=0
OPTS=()
IDX=1

UTXOS=$(cardano-cli query utxo --testnet-magic 2 --address ${SRC_ADDR} | sort -nrk3)
while [ $AMT_SO_FAR -le $AMT_LOVELACE ]; do
	AMT=$(echo "$UTXOS" | awk 'BEGIN{OFS="#"} { if (NF <= 6 && $6 == "TxOutDatumNone") {print $3} }' | sed -n "${IDX}p")
	TXIN=$(echo "$UTXOS" | awk 'BEGIN{OFS="#"} { if (NF <= 6 && $6 == "TxOutDatumNone") {print $1,$2} }' | sed -n "${IDX}p")

	if [ -z $TXIN ]; then
    echo "Not enough ADA"
    exit
  fi

	IDX=$((1+$IDX))
  AMT_SO_FAR=$((${AMT_SO_FAR} + ${AMT}))
  OPTS+=("--tx-in" "${TXIN}")
done

echo "$AMT_SO_FAR ADA found"

cardano-cli transaction build \
  --testnet-magic 2 \
  "${OPTS[@]/#/}" \
  --tx-out "${DEST_ADDR}+${AMT_LOVELACE}" \
  --change-address "${SRC_ADDR}" \
  --out-file tmp-move-funds.json

cardano-cli transaction sign \
  --tx-body-file tmp-move-funds.json \
  --signing-key-file "${SRC}/${SRC}.skey" \
  --out-file tmp-move-funds.signed.json

TX_ID=$(cardano-cli transaction txid --tx-body-file tmp-move-funds.json)
SIGNED_CBOR=`jq -r -n --argjson data "$(cat tmp-move-funds.json)" '$data.cborHex'`
echo "Signed CBOR hex data to transmit: $SIGNED_CBOR"

rm tmp-move-funds.*
