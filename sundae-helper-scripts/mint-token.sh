set -e

WALLET=${1:-scooper-0}
AMT=${2:-1000000000}
TOKEN=${3:-RBERRY}
METADATA_FILE=${5:-}

WALLET_ADDR=$(cat $WALLET/$WALLET.addr)
KEY_HASH=$(cardano-cli address key-hash --payment-verification-key-file "$WALLET/$WALLET.vkey")

DEST=${4:-$WALLET_ADDR}
if [[ $DEST == addr* ]]; then
	DEST_ADDR=$DEST
else
  DEST_ADDR=$(cat $DEST/$DEST.addr)
fi


cat << EOF > tmp-policy-script.json
{
  "type": "sig",
  "keyHash": "${KEY_HASH}"
}
EOF


TXIN=$(cardano-cli query utxo --testnet-magic 2 --address ${WALLET_ADDR} | sort -nrk3 |  awk 'BEGIN{OFS="#"} { if (NF <= 6 && $6 == "TxOutDatumNone") {print $1,$2} }' | head -n 1)

POLICY_ID=$(cardano-cli transaction policyid --script-file tmp-policy-script.json)
TOKEN_HEX=$(echo $TOKEN | tr -d '\n' | xxd -p)

# METADATA_CONTENTS=$(awk -v pattern="{policy_id}" -v replacement="${POLICY_ID}" "{gsub(pattern, replacement); print}" "${METADATA_FILE}")
# echo ${METADATA_CONTENTS} > tmp-metadata.json
# METADATA_CONTENTS=$(awk -v pattern="{Atrium}" -v replacement="${TOKEN_HEX}" "{gsub(pattern, replacement); print}" tmp-metadata.json)
# echo ${METADATA_CONTENTS} > tmp-metadata.json

echo "TxIn:     $TXIN"
echo "PolicyID: ${POLICY_ID}"
# echo "Metadata: ${METADATA_CONTENTS}"

echo "TokenHex: $TOKEN_HEX"

# echo ${METADATA_CONTENTS} > tmp-metadata.json

cardano-cli transaction build \
  --testnet-magic 2 \
  --tx-in "${TXIN}" \
  --tx-out "${DEST_ADDR}+1500000+${AMT} ${POLICY_ID}.${TOKEN_HEX}" \
  --mint "${AMT} ${POLICY_ID}.${TOKEN_HEX}" \
  --mint-script-file tmp-policy-script.json \
  --change-address "${WALLET_ADDR}" \
  --out-file tmp-mint-token.json
  # --metadata-json-file tmp-metadata.json \

cardano-cli transaction sign \
  --tx-body-file tmp-mint-token.json \
  --signing-key-file "${WALLET}/${WALLET}.skey" \
  --out-file tmp-mint-token.signed.json

cardano-cli transaction submit \
 --testnet-magic 2 \
 --tx-file tmp-mint-token.signed.json

TX_ID=$(cardano-cli transaction txid --tx-body-file tmp-mint-token.json)
echo "TxID: ${TX_ID}"

./wait-for-tx.sh "${TX_ID}"

rm tmp-*.json
