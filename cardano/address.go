package cardano

import (
	"fmt"
	"strings"

	"github.com/savaki/bech32"
)

var ErrByronAddress = fmt.Errorf("byron addresses have no payment / staking parts")
var ErrStakeAddress = fmt.Errorf("cannot split a staking address")

func SplitAddress(address string) (paymentCredential, stakingCredential []byte, err error) {
	if strings.HasPrefix(address, "stake") {
		_, bytes, err := bech32.Decode(address)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to decode address %v: %w", address, err)
		} else if len(bytes) != 29 {
			return nil, nil, fmt.Errorf("invalid address: decoded address %v is only %v bytes", address, len(bytes))
		}
		return nil, bytes[1:], nil
	}

	if !strings.HasPrefix(address, "addr") {
		return nil, nil, ErrByronAddress
	}
	_, bytes, err := bech32.Decode(address)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to decode address %v: %w", address, err)
	} else if len(bytes) != 29 && len(bytes) != 57 {
		// God help us if we ever have to deal with an staking pointer address lol
		return nil, nil, fmt.Errorf("invalid address: decoded address %v is only %v bytes", address, len(bytes))
	}
	paymentBytes := bytes[1:29]
	var stakingBytes []byte // default to nil if no staking key
	if len(bytes) == 57 {
		stakingBytes = bytes[29:]
	}
	return paymentBytes, stakingBytes, nil
}
