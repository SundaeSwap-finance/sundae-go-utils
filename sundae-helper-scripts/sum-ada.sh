WALLET=${1:-scooper-0}

if [[ $WALLET == addr* ]]; then
	ADDR=$WALLET
else
  ADDR=$(cat ${WALLET}/${WALLET}.addr)
fi

AMT=$(cardano-cli query utxo --testnet-magic 2 --address "${ADDR}" | awk '{ if (NF <= 6 && $6 == "TxOutDatumNone") {print $3} }' | awk '{s+=$1} END {printf "%.0f", s}')

echo "Total ADA: $(expr $AMT / 1000000).$(expr $AMT % 1000000)"
