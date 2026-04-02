set -e

while ! cardano-cli query utxo --whole-utxo --testnet-magic 2 | grep -q $1
do
	echo "Sleeping..."
  sleep 5s
done

echo "Tx found in UTXO"
