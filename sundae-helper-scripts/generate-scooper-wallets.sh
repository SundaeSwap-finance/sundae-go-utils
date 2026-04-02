#!/bin/bash

set -eu

rm -rf bak/ || true
mkdir -p bak/ || true
mv scooper* bak/ || true

echo "[" >> scoopers.json
echo "[" >> scoopers.raw.json

for i in {0..32}
do

  echo "Generating scooper-${i}"
  mkdir -p "scooper-${i}"
  cd "scooper-${i}"
  generate-addresss.sh "scooper-${i}"
  cd ../

  ADDR=$(cat "scooper-${i}/scooper-${i}.addr")

  if [ "$i" -gt "0" ]; then
    echo "," >> scoopers.json
  fi
  echo -n '  "'$ADDR'"' >> scoopers.json

  ADDR_BYTES=$(bech32 --format json $ADDR | jq '.[].data')
  PAYMENT_ADDR=${ADDR_BYTES:3:56}

  if [ "$i" -gt "0" ]; then
    echo "," >> scoopers.raw.json
  fi
  echo    '  {' >> scoopers.raw.json
  echo    '    "getPubKeyHash": "'$PAYMENT_ADDR'"' >> scoopers.raw.json
  echo -n '  }'  >> scoopers.raw.json

done

echo "" >> scoopers.json
echo "]" >> scoopers.json
echo "" >> scoopers.raw.json
echo "]" >> scoopers.raw.json

echo "Done."
