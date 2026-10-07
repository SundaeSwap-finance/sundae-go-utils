WALLET=${1:-scooper-0}
ADDR=$(cat $WALLET/$WALLET.addr)
echo $ADDR

curl -X POST \
  -s "https://faucet.preview.world.dev.cardano.org/send-money/${ADDR}?api_key=nohnuXahthoghaeNoht9Aow3ze4quohc"
echo
