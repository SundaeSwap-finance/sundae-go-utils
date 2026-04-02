set -e

WALLET=${1:-scooper-0}
AMT=${2:-1000000000}
TOKEN=${3:-RBERRY}
TOKEN2="RBE"

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
TOKEN2_HEX=$(echo $TOKEN2 | tr -d '\n' | xxd -p)
echo "Token2Hex: $TOKEN2_HEX"

MINT="1 ${POLICY_ID}.52424531+1 ${POLICY_ID}.52424532+1 ${POLICY_ID}.52424533+1 ${POLICY_ID}.52424534+1 ${POLICY_ID}.52424532323232323232+1 ${POLICY_ID}.5242453232323232343434"

cardano-cli transaction build \
  --testnet-magic 2 \
  --tx-in "${TXIN}" \
  --tx-out "${DEST_ADDR}+1700000+1 ${POLICY_ID}.${TOKEN_HEX}+${MINT}" \
  --mint "1 ${POLICY_ID}.${TOKEN_HEX}+${MINT}" \
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
