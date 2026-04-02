#! /bin/bash

for i in {1..10}
do
  echo "Minting with scooper ${i}"
  for j in {1..10}
  do
	  echo "NFT ${j}"
    ./mint-many.sh "scooper-${i}" 1 "NFT-${j}" addr_test1qrwddguk6mn5axczfc3m2ryx9wwkwf3y6yrrrvgzmdezu8tgpzrz0ht2a8faz0waqgsf42pz8rdajr7tf83p08nkdmqqv6w8gk
  done
done
