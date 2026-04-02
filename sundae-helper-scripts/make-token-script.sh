#!/bin/bash

WALLET=${1:-scooper-0}

WALLET_ADDR=$(cat $WALLET/$WALLET.addr)
KEY_HASH=$(cardano-cli address key-hash --payment-verification-key-file "$WALLET/$WALLET.vkey")

cat << EOF > policy-script.json
{
  "type": "sig",
  "keyHash": "${KEY_HASH}"
}
EOF
