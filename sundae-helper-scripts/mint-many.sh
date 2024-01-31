set -e

WALLET=${1:-scooper-0}
AMT=${2:-1000000000}
TOKEN=${3:-RBERRY}

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

echo "TxIn:     $TXIN"
echo "PolicyID: ${POLICY_ID}"

TOKEN_HEX=$(echo $TOKEN | tr -d '\n' | xxd -p)
echo "TokenHex: $TOKEN_HEX"

cardano-cli transaction build \
  --testnet-magic 2 \
  --tx-in "${TXIN}" \
  --tx-out "${DEST_ADDR}+1500000+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d302d30+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d302d31" \
  --tx-out "${DEST_ADDR}+1500000+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d312d30" \
  --tx-out "${DEST_ADDR}+1500000+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d322d30" \
  --tx-out "${DEST_ADDR}+1500000+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d332d30" \
  --tx-out "${DEST_ADDR}+1500000+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d342d30" \
  --tx-out "${DEST_ADDR}+1500000+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d352d30" \
  --tx-out "${DEST_ADDR}+1500000+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d362d30" \
  --tx-out "${DEST_ADDR}+1500000+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d372d30" \
  --tx-out "${DEST_ADDR}+1500000+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d382d30" \
  --tx-out "${DEST_ADDR}+1500000+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d392d30" \
  --mint "${AMT} ${POLICY_ID}.${TOKEN_HEX}2d302d30+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d302d31+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d312d30+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d322d30+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d332d30+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d342d30+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d352d30+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d362d30+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d372d30+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d382d30+${AMT} ${POLICY_ID}.${TOKEN_HEX}2d392d30" \
  --mint-script-file tmp-policy-script.json \
  --change-address "${WALLET_ADDR}" \
  --out-file tmp-mint-token.json

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
