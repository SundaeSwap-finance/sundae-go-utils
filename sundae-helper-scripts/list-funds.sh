WALLET=${1:-scooper-0}

if [[ $WALLET == addr* ]]; then
	ADDR=$WALLET
else
  ADDR=$(cat ${WALLET}/${WALLET}.addr)
fi

cardano-cli query utxo --testnet-magic 2 --address "${ADDR}"
