package cardano

import (
	"fmt"

	"github.com/savaki/bech32"
)

func SplitAddress(address string) (paymentCredential, stakingCredential []byte, err error) {
	_, bytes, err := bech32.Decode(address)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to decode address %v: %w", address, err)
	}
	paymentBytes := bytes[1:29]
	stakingBytes := []byte{}
	if len(bytes) == 57 {
		stakingBytes = bytes[29:]
	}
	return paymentBytes, stakingBytes, nil
}
