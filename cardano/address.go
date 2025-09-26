package cardano

import (
	"fmt"
	"strings"

	"github.com/savaki/bech32"
)

var ErrByronAddress = fmt.Errorf("byron addresses have no payment / staking parts")
var ErrStakeAddress = fmt.Errorf("cannot split a staking address")

func hasStakeAddress(bytes []byte) bool {
	if len(bytes) != 57 {
		return false
	}
	if len(bytes) < 1 {
		return false
	}
	return bytes[0]&0b01000000 == 0
}

func HasStakeAddress(address string) (bool, error) {
	_, bytes, err := bech32.Decode(address)
	if err != nil {
		return false, fmt.Errorf("unable to decode address %v: %w", address, err)
	}
	return hasStakeAddress(bytes), nil
}

func hasStakeAddressPointer(bytes []byte) bool {
	if len(bytes) <= 29 {
		return false
	}
	if len(bytes) < 1 {
		return false
	}
	return bytes[0]&0b01100000 == 0b01000000
}

func HasStakeAddressPointer(address string) (bool, error) {
	_, bytes, err := bech32.Decode(address)
	if err != nil {
		return false, fmt.Errorf("unable to decode address %v: %w", address, err)
	}
	return hasStakeAddressPointer(bytes), nil
}

func hasNoStakeAddress(bytes []byte) bool {
	if len(bytes) > 29 {
		return false
	}
	if len(bytes) < 1 {
		return false
	}
	return bytes[0]&0b01100000 == 0b01100000
}

func HasNoStakeAddress(address string) (bool, error) {
	_, bytes, err := bech32.Decode(address)
	if err != nil {
		return false, fmt.Errorf("unable to decode address %v: %w", address, err)
	}
	return hasNoStakeAddress(bytes), nil
}

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
	} else if len(bytes) < 29 {
		return nil, nil, fmt.Errorf("invalid address: decoded address %v is only %v bytes", address, len(bytes))
	}
	// Note: if it's 35 bytes, it's a pointer address; we just assume this has no staking address attached
	// They're disallowed in modern eras, and in historical replays they were never relevant
	paymentBytes := bytes[1:29]
	var stakingBytes []byte // default to nil if no staking key
	if hasStakeAddress(bytes) {
		stakingBytes = bytes[29:]
	}
	return paymentBytes, stakingBytes, nil
}
